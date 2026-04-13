use std::collections::HashMap;
use std::ffi::CString;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use quex_pq_sys as ffi;
use tokio::io::unix::AsyncFd;

use super::error::{Error, ExecuteResult, Result};
use super::options::ConnectOptions;
use super::rows::{Metadata, ResultSet};
use super::runtime::{
    CONNECTION_POISONED, CONNECTION_READY, ConnHandle, ConnectParams, ConnectionOpGuard,
    ParamScratch, SocketRef, execute_prepared_no_params, execute_result_from_handle,
    finish_request, prepare_named_statement, send_query, should_prepare_query, wait_for_socket,
};
use super::statement::{CachedStatement, Statement};

struct QueryCacheEntry {
    sql: CString,
    kind: QueryCacheKind,
}

enum QueryCacheKind {
    Simple {
        metadata: Option<Arc<Metadata>>,
    },
    Prepared {
        name: CString,
        metadata: Option<Arc<Metadata>>,
    },
}

pub(crate) struct CachedStmtEntry {
    pub(crate) name: CString,
    pub(crate) result_metadata: Option<Arc<Metadata>>,
    pub(crate) scratch: ParamScratch,
}

/// A single postgres connection backed by libpq.
///
/// This driver requires a tokio runtime because socket readiness is driven
/// through `tokio::io::unix::AsyncFd`. Query and statement methods take
/// `&mut self`, so one connection runs one operation at a time.
pub struct Connection {
    pub(crate) conn: ConnHandle,
    pub(crate) socket: AsyncFd<SocketRef>,
    pub(crate) state: Arc<AtomicU8>,
    query_cache: HashMap<Box<str>, QueryCacheEntry>,
    pub(crate) statement_cache: HashMap<Box<str>, CachedStmtEntry>,
    next_statement_id: u64,
}

impl Connection {
    /// Opens a new connection using libpq's nonblocking socket api.
    pub async fn connect(options: ConnectOptions) -> Result<Self> {
        // SAFETY: libpq connection calls use nul-terminated parameter arrays and a live connection.
        unsafe {
            let params = ConnectParams::new(&options)?;
            let conn =
                ffi::PQconnectStartParams(params.keywords.as_ptr(), params.values.as_ptr(), 0);
            let conn = ConnHandle(
                std::ptr::NonNull::new(conn)
                    .ok_or_else(|| Error::new("PQconnectStartParams returned null"))?,
            );

            if ffi::PQsetnonblocking(conn.as_ptr(), 1) != 0 {
                let error = Error::from_conn(conn.as_ptr(), "PQsetnonblocking failed");
                ffi::PQfinish(conn.as_ptr());
                return Err(error);
            }

            let socket_fd = ffi::PQsocket(conn.as_ptr());
            if socket_fd < 0 {
                let error = Error::from_conn(conn.as_ptr(), "libpq did not expose a valid socket");
                ffi::PQfinish(conn.as_ptr());
                return Err(error);
            }

            let connection = Self {
                conn,
                socket: AsyncFd::new(SocketRef(socket_fd))?,
                state: Arc::new(AtomicU8::new(CONNECTION_READY)),
                query_cache: HashMap::new(),
                statement_cache: HashMap::new(),
                next_statement_id: 1,
            };

            loop {
                match ffi::PQconnectPoll(connection.conn.as_ptr()) {
                    x if x == ffi::PostgresPollingStatusType_PGRES_POLLING_OK => break,
                    x if x == ffi::PostgresPollingStatusType_PGRES_POLLING_READING => {
                        connection.wait_readable().await?;
                    }
                    x if x == ffi::PostgresPollingStatusType_PGRES_POLLING_WRITING => {
                        connection.wait_writable().await?;
                    }
                    x if x == ffi::PostgresPollingStatusType_PGRES_POLLING_ACTIVE => continue,
                    _ => {
                        return Err(Error::from_conn(
                            connection.conn.as_ptr(),
                            "connection failed",
                        ));
                    }
                }
            }

            Ok(connection)
        }
    }

    /// Runs SQL and returns rows.
    ///
    /// The driver may prepare and cache repeated row-producing queries
    /// internally. Use [`Self::execute`] for statements that do not return rows.
    pub async fn query(&mut self, sql_text: &str) -> Result<ResultSet> {
        self.ensure_ready()?;
        let mut guard = ConnectionOpGuard::new(&self.state);
        if !self.query_cache.contains_key(sql_text) {
            let kind = if should_prepare_query(sql_text) {
                let statement_name = format!("quex_driver_query_{}", self.next_statement_id);
                self.next_statement_id += 1;
                QueryCacheKind::Prepared {
                    name: CString::new(statement_name).expect("statement name contains no nul"),
                    metadata: None,
                }
            } else {
                QueryCacheKind::Simple { metadata: None }
            };
            self.query_cache.insert(
                sql_text.into(),
                QueryCacheEntry {
                    sql: CString::new(sql_text).expect("query contains no nul byte"),
                    kind,
                },
            );
        }
        let entry = self
            .query_cache
            .get_mut(sql_text)
            .expect("query cache entry missing");
        let (result, metadata) = match &mut entry.kind {
            QueryCacheKind::Simple { metadata } => {
                send_query(self.conn, entry.sql.as_ptr())?;
                let result = finish_request(self.conn, &self.socket).await?;
                let metadata = match metadata {
                    Some(metadata) => Arc::clone(metadata),
                    None => {
                        let new_metadata = Arc::new(Metadata::from_result(result));
                        *metadata = Some(Arc::clone(&new_metadata));
                        new_metadata
                    }
                };
                (result, metadata)
            }
            QueryCacheKind::Prepared { name, metadata } => {
                if metadata.is_none() {
                    prepare_named_statement(self.conn, &self.socket, name, &entry.sql).await?;
                }
                let result =
                    execute_prepared_no_params(self.conn, &self.socket, &self.state, name).await?;
                let metadata = match metadata {
                    Some(metadata) => Arc::clone(metadata),
                    None => {
                        let new_metadata = Arc::new(Metadata::from_result(result));
                        *metadata = Some(Arc::clone(&new_metadata));
                        new_metadata
                    }
                };
                (result, metadata)
            }
        };
        guard.complete();
        Ok(ResultSet::new(result, metadata))
    }

    /// Prepares a statement.
    ///
    /// The unnamed prepared statement is tied to this connection.
    pub async fn prepare(&mut self, sql: &str) -> Result<Statement<'_>> {
        self.ensure_ready()?;
        let mut guard = ConnectionOpGuard::new(&self.state);
        let name = CString::new("")?;
        let sql = CString::new(sql)?;
        // SAFETY: connection and query strings are live for the duration of the send call.
        unsafe {
            if ffi::PQsendPrepare(
                self.conn.as_ptr(),
                name.as_ptr(),
                sql.as_ptr(),
                0,
                ptr::null(),
            ) == 0
            {
                return Err(Error::from_conn(self.conn.as_ptr(), "PQsendPrepare failed"));
            }
        }
        let result = finish_request(self.conn, &self.socket).await?;
        // SAFETY: result was returned by libpq and is still live.
        let status = unsafe { ffi::PQresultStatus(result.as_ptr()) };
        if status != ffi::ExecStatusType_PGRES_COMMAND_OK {
            // SAFETY: result is live and contains diagnostics for the failed prepare.
            let error = unsafe { Error::from_result(result.as_ptr(), "prepare failed") };
            // SAFETY: result was returned by libpq and is cleared exactly once here.
            unsafe { ffi::PQclear(result.as_ptr()) };
            return Err(error);
        }
        // SAFETY: result was returned by libpq and is cleared exactly once here.
        unsafe { ffi::PQclear(result.as_ptr()) };
        guard.complete();

        Ok(Statement {
            conn: self,
            name,
            result_metadata: None,
            scratch: ParamScratch::new(),
        })
    }

    /// Prepares a statement and keeps it cached on the connection.
    ///
    /// A cached statement is reused for the same SQL text until the connection
    /// is dropped.
    pub async fn prepare_cached(&mut self, sql: &str) -> Result<CachedStatement<'_>> {
        self.ensure_ready()?;
        let mut guard = ConnectionOpGuard::new(&self.state);
        if !self.statement_cache.contains_key(sql) {
            let statement_name = format!("quex_driver_stmt_{}", self.next_statement_id);
            self.next_statement_id += 1;
            let name = CString::new(statement_name).expect("statement name contains no nul");
            let query = CString::new(sql)?;
            prepare_named_statement(self.conn, &self.socket, &name, &query).await?;
            self.statement_cache.insert(
                sql.into(),
                CachedStmtEntry {
                    name,
                    result_metadata: None,
                    scratch: ParamScratch::new(),
                },
            );
        }
        guard.complete();

        Ok(CachedStatement {
            conn: self,
            key: sql.into(),
        })
    }

    /// Starts a transaction.
    ///
    /// Dropping the transaction without commit or rollback poisons the
    /// connection, because the async rollback cannot be completed from `Drop`.
    pub async fn begin(&mut self) -> Result<Transaction<'_>> {
        self.query("begin").await?;
        Ok(Transaction {
            conn: self,
            finished: false,
        })
    }

    /// Runs SQL when no rows are expected.
    pub async fn execute(&mut self, sql: &str) -> Result<ExecuteResult> {
        self.ensure_ready()?;
        let mut guard = ConnectionOpGuard::new(&self.state);
        let sql = CString::new(sql)?;
        // SAFETY: connection and query string are live for the duration of the send call.
        unsafe {
            if ffi::PQsendQuery(self.conn.as_ptr(), sql.as_ptr()) == 0 {
                return Err(Error::from_conn(self.conn.as_ptr(), "PQsendQuery failed"));
            }
        }
        let result = finish_request(self.conn, &self.socket).await?;
        let execute = execute_result_from_handle(result)?;
        // SAFETY: result was returned by libpq and is cleared exactly once here.
        unsafe { ffi::PQclear(result.as_ptr()) };
        guard.complete();
        Ok(execute)
    }

    /// Commits the current transaction.
    pub async fn commit(&mut self) -> Result<()> {
        self.query("commit").await.map(|_| ())
    }

    /// Rolls back the current transaction.
    pub async fn rollback(&mut self) -> Result<()> {
        self.query("rollback").await.map(|_| ())
    }

    #[inline]
    pub(crate) fn ensure_ready(&self) -> Result<()> {
        if self.state.load(Ordering::Acquire) == CONNECTION_POISONED {
            Err(Error::new(
                "postgres connection is no longer reusable after a cancelled or dropped operation",
            ))
        } else {
            Ok(())
        }
    }

    async fn wait_readable(&self) -> Result<()> {
        wait_for_socket(&self.socket, libc::POLLIN, true).await
    }

    async fn wait_writable(&self) -> Result<()> {
        wait_for_socket(&self.socket, libc::POLLOUT, false).await
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: the connection handle is owned by this Connection and finished once on drop.
        unsafe {
            ffi::PQfinish(self.conn.as_ptr());
        }
    }
}

/// A postgres transaction.
///
/// Dropping an unfinished transaction marks the connection as no longer
/// reusable.
pub struct Transaction<'a> {
    conn: &'a mut Connection,
    finished: bool,
}

impl Transaction<'_> {
    /// Returns the underlying connection.
    #[inline]
    pub fn connection(&mut self) -> &mut Connection {
        self.conn
    }

    /// Commits the transaction.
    pub async fn commit(mut self) -> Result<()> {
        self.finished = true;
        self.conn.commit().await
    }

    /// Rolls the transaction back.
    pub async fn rollback(mut self) -> Result<()> {
        self.finished = true;
        self.conn.rollback().await
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.conn
                .state
                .store(CONNECTION_POISONED, Ordering::Release);
        }
    }
}

// SAFETY: `Connection` operations require `&mut self` and the connection state machine poisons the
// handle after interrupted operations rather than allowing concurrent reuse.
unsafe impl Send for Connection {}
