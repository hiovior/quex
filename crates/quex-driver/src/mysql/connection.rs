use std::collections::HashMap;
use std::ffi::CString;
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::time::Duration;

use quex_mariadb_sys as ffi;
use tokio::io::unix::AsyncFd;

use super::error::{Error, ExecuteResult, Result};
use super::options::ConnectOptions;
use super::rows::{Metadata, ResultSet};
use super::runtime::{
    DriveOperation, MysqlHandle, MysqlOut, MysqlResOut, ParamScratch, SocketRef, StmtHandle,
    WAIT_EXCEPT, WAIT_READ, WAIT_TIMEOUT, WAIT_WRITE, continue_operation,
    ensure_mysql_thread_ready, start_operation, to_cstring_ptr,
};
use super::statement::{CachedStatement, Statement};

pub(crate) struct CachedStmtEntry {
    pub(crate) stmt: StmtHandle,
    pub(crate) param_count: usize,
    pub(crate) result_metadata: Option<Arc<Metadata>>,
    pub(crate) scratch: ParamScratch,
}

/// A single mysql or mariadb connection.
///
/// Operations use libmariadb's nonblocking api and require a tokio runtime.
/// Query and statement methods take `&mut self`, so one connection runs one
/// operation at a time.
pub struct Connection {
    pub(crate) mysql: MysqlHandle,
    socket: Option<AsyncFd<SocketRef>>,
    query_metadata_cache: HashMap<Box<str>, Arc<Metadata>>,
    pub(crate) statement_cache: HashMap<Box<str>, CachedStmtEntry>,
}

impl Connection {
    /// Opens a new connection.
    pub async fn connect(options: ConnectOptions) -> Result<Self> {
        ensure_mysql_thread_ready()?;
        // SAFETY: All libmariadb calls use handles and option pointers valid for each call.
        unsafe {
            let mysql = ffi::mysql_init(ptr::null_mut());
            let mysql = MysqlHandle(
                NonNull::new(mysql).ok_or_else(|| Error::new("mysql_init returned null"))?,
            );

            if ffi::mysql_optionsv(mysql.as_ptr(), ffi::mysql_option_MYSQL_OPT_NONBLOCK, 0usize)
                != 0
            {
                let error = Error::from_mysql(mysql.as_ptr(), "failed to enable nonblocking mode");
                ffi::mysql_close(mysql.as_ptr());
                return Err(error);
            }

            let host = to_cstring_ptr(options.host.as_deref())?;
            let user = to_cstring_ptr(options.user.as_deref())?;
            let password = to_cstring_ptr(options.password.as_deref())?;
            let database = to_cstring_ptr(options.database.as_deref())?;
            let unix_socket = to_cstring_ptr(options.unix_socket.as_deref())?;

            let mut conn = Self {
                mysql,
                socket: None,
                query_metadata_cache: HashMap::new(),
                statement_cache: HashMap::new(),
            };
            let mut out = MysqlOut {
                _ptr: mysql.as_ptr(),
            };

            conn.drive(
                DriveOperation::Connect {
                    mysql,
                    host: &host,
                    user: &user,
                    password: &password,
                    database: &database,
                    port: options.port,
                    unix_socket: &unix_socket,
                },
                &mut out,
            )
            .await?;

            Ok(conn)
        }
    }

    /// Runs SQL and returns rows.
    ///
    /// Use [`Self::execute`] for statements that do not return rows.
    pub async fn query(&mut self, sql_text: &str) -> Result<ResultSet> {
        ensure_mysql_thread_ready()?;
        // SAFETY: The MYSQL handle is live and the query/result pointers stay valid during calls.
        unsafe {
            let sql = CString::new(sql_text)?;
            let mysql = self.mysql;
            let mut query_ret = 0;

            self.drive(DriveOperation::Query { mysql, sql: &sql }, &mut query_ret)
                .await?;

            if query_ret != 0 {
                return Err(Error::from_mysql(mysql.as_ptr(), "query failed"));
            }

            if ffi::mysql_field_count(mysql.as_ptr()) == 0 {
                return Ok(ResultSet::empty());
            }

            let mut raw_result = MysqlResOut {
                ptr: ptr::null_mut(),
            };
            self.drive(DriveOperation::StoreResult { mysql }, &mut raw_result)
                .await?;

            let raw_result = NonNull::new(raw_result.ptr).ok_or_else(|| {
                Error::from_mysql(mysql.as_ptr(), "query did not produce a buffered result")
            })?;

            let metadata = match self.query_metadata_cache.get(sql_text) {
                Some(metadata) => Arc::clone(metadata),
                None => {
                    let metadata = Arc::new(Metadata::from_result(raw_result));
                    self.query_metadata_cache
                        .insert(sql_text.into(), Arc::clone(&metadata));
                    metadata
                }
            };
            Ok(ResultSet::text(raw_result, metadata))
        }
    }

    /// Runs SQL when no rows are expected.
    ///
    /// This returns an error if the statement produces a result set.
    pub async fn execute(&mut self, sql_text: &str) -> Result<ExecuteResult> {
        ensure_mysql_thread_ready()?;
        // SAFETY: The MYSQL handle is live and the SQL string is valid for the query call.
        unsafe {
            let sql = CString::new(sql_text)?;
            let mysql = self.mysql;
            let mut query_ret = 0;

            self.drive(DriveOperation::Query { mysql, sql: &sql }, &mut query_ret)
                .await?;

            if query_ret != 0 {
                return Err(Error::from_mysql(mysql.as_ptr(), "query failed"));
            }

            if ffi::mysql_field_count(mysql.as_ptr()) != 0 {
                return Err(Error::new("statement returned rows; use query instead"));
            }

            Ok(ExecuteResult {
                rows_affected: ffi::mysql_affected_rows(mysql.as_ptr()) as u64,
                last_insert_id: ffi::mysql_insert_id(mysql.as_ptr()) as u64,
            })
        }
    }

    /// Prepares a statement.
    ///
    /// The statement is closed when the returned [`Statement`] is dropped.
    pub async fn prepare(&mut self, sql: &str) -> Result<Statement<'_>> {
        ensure_mysql_thread_ready()?;
        // SAFETY: The MYSQL handle is live and the statement handle is closed on every error path.
        unsafe {
            let stmt = ffi::mysql_stmt_init(self.mysql.as_ptr());
            let stmt =
                StmtHandle(NonNull::new(stmt).ok_or_else(|| {
                    Error::from_mysql(self.mysql.as_ptr(), "mysql_stmt_init failed")
                })?);

            let sql = CString::new(sql)?;
            let mut prepare_ret = 0;
            self.drive(
                DriveOperation::StmtPrepare { stmt, sql: &sql },
                &mut prepare_ret,
            )
            .await?;

            if prepare_ret != 0 {
                let error = Error::from_stmt(stmt.as_ptr(), "mysql_stmt_prepare failed");
                ffi::mysql_stmt_close(stmt.as_ptr());
                return Err(error);
            }

            Ok(Statement {
                conn: self,
                stmt,
                result_metadata: None,
            })
        }
    }

    /// Prepares a statement and keeps it cached on the connection.
    ///
    /// A cached statement is reused for the same SQL text until the connection
    /// is dropped.
    pub async fn prepare_cached(&mut self, sql: &str) -> Result<CachedStatement<'_>> {
        if !self.statement_cache.contains_key(sql) {
            let stmt = self.prepare_stmt(sql).await?;
            // SAFETY: stmt is a live prepared statement.
            let param_count = unsafe { ffi::mysql_stmt_param_count(stmt.as_ptr()) as usize };
            self.statement_cache.insert(
                sql.into(),
                CachedStmtEntry {
                    stmt,
                    param_count,
                    result_metadata: None,
                    scratch: ParamScratch::new(param_count),
                },
            );
        }

        Ok(CachedStatement {
            conn: self,
            key: sql.into(),
        })
    }

    /// Starts a transaction.
    ///
    /// Dropping the transaction without commit or rollback attempts a
    /// best-effort rollback.
    pub async fn begin(&mut self) -> Result<Transaction<'_>> {
        self.query("START TRANSACTION").await?;
        Ok(Transaction {
            conn: self,
            finished: false,
        })
    }

    /// Commits the current transaction.
    pub async fn commit(&mut self) -> Result<()> {
        ensure_mysql_thread_ready()?;
        // SAFETY: The MYSQL handle is live for the nonblocking commit operation.
        unsafe {
            let mysql = self.mysql;
            let mut ret: ffi::my_bool = 0;
            self.drive(DriveOperation::Commit { mysql }, &mut ret)
                .await?;

            if ret != 0 {
                return Err(Error::from_mysql(mysql.as_ptr(), "commit failed"));
            }

            Ok(())
        }
    }

    /// Rolls back the current transaction.
    pub async fn rollback(&mut self) -> Result<()> {
        ensure_mysql_thread_ready()?;
        // SAFETY: The MYSQL handle is live for the nonblocking rollback operation.
        unsafe {
            let mysql = self.mysql;
            let mut ret: ffi::my_bool = 0;
            self.drive(DriveOperation::Rollback { mysql }, &mut ret)
                .await?;

            if ret != 0 {
                return Err(Error::from_mysql(mysql.as_ptr(), "rollback failed"));
            }

            Ok(())
        }
    }

    pub(crate) async fn drive<T>(&mut self, op: DriveOperation<'_>, out: &mut T) -> Result<()> {
        ensure_mysql_thread_ready()?;
        // SAFETY: op describes the matching libmariadb operation and out has its result type.
        let mut status = unsafe { start_operation(op, out) };
        while status != 0 {
            let ready = self.wait_for(status).await?;
            ensure_mysql_thread_ready()?;
            // SAFETY: This continues the same operation after the requested socket readiness.
            status = unsafe { continue_operation(op, out, ready) };
        }
        Ok(())
    }

    async fn wait_for(&mut self, status: i32) -> Result<i32> {
        self.refresh_socket()?;
        // SAFETY: The MYSQL handle is live while waiting for a nonblocking operation.
        let timeout_ms = unsafe { ffi::mysql_get_timeout_value_ms(self.mysql.as_ptr()) } as u64;
        let wants_read = (status & (WAIT_READ | WAIT_EXCEPT)) != 0;
        let wants_write = (status & WAIT_WRITE) != 0;
        let wants_timeout = (status & WAIT_TIMEOUT) != 0;
        let socket = self
            .socket
            .as_ref()
            .ok_or_else(|| Error::new("libmariadb did not expose a valid socket"))?;

        match (wants_read, wants_write, wants_timeout) {
            (true, true, true) => {
                tokio::select! {
                    ready = socket.readable() => {
                        let mut ready = ready?;
                        ready.clear_ready();
                        Ok(WAIT_READ)
                    }
                    ready = socket.writable() => {
                        let mut ready = ready?;
                        ready.clear_ready();
                        Ok(WAIT_WRITE)
                    }
                    _ = tokio::time::sleep(Duration::from_millis(timeout_ms)) => Ok(WAIT_TIMEOUT),
                }
            }
            (true, true, false) => {
                tokio::select! {
                    ready = socket.readable() => {
                        let mut ready = ready?;
                        ready.clear_ready();
                        Ok(WAIT_READ)
                    }
                    ready = socket.writable() => {
                        let mut ready = ready?;
                        ready.clear_ready();
                        Ok(WAIT_WRITE)
                    }
                }
            }
            (true, false, true) => {
                tokio::select! {
                    ready = socket.readable() => {
                        let mut ready = ready?;
                        ready.clear_ready();
                        Ok(WAIT_READ)
                    }
                    _ = tokio::time::sleep(Duration::from_millis(timeout_ms)) => Ok(WAIT_TIMEOUT),
                }
            }
            (false, true, true) => {
                tokio::select! {
                    ready = socket.writable() => {
                        let mut ready = ready?;
                        ready.clear_ready();
                        Ok(WAIT_WRITE)
                    }
                    _ = tokio::time::sleep(Duration::from_millis(timeout_ms)) => Ok(WAIT_TIMEOUT),
                }
            }
            (true, false, false) => {
                let mut ready = socket.readable().await?;
                ready.clear_ready();
                Ok(WAIT_READ)
            }
            (false, true, false) => {
                let mut ready = socket.writable().await?;
                ready.clear_ready();
                Ok(WAIT_WRITE)
            }
            (false, false, true) => {
                tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
                Ok(WAIT_TIMEOUT)
            }
            (false, false, false) => {
                Err(Error::new("libmariadb returned an unsupported wait status"))
            }
        }
    }

    fn refresh_socket(&mut self) -> Result<()> {
        // SAFETY: The MYSQL handle is live and libmariadb exposes its current socket fd.
        let fd = unsafe { ffi::mysql_get_socket(self.mysql.as_ptr()) };
        if fd < 0 {
            self.socket = None;
            return Err(Error::new("libmariadb did not expose a valid socket"));
        }

        let needs_refresh = self
            .socket
            .as_ref()
            .is_none_or(|socket| socket.get_ref().0 != fd);

        if needs_refresh {
            self.socket = Some(AsyncFd::new(SocketRef(fd))?);
        }

        Ok(())
    }

    async fn prepare_stmt(&mut self, sql: &str) -> Result<StmtHandle> {
        // SAFETY: The MYSQL handle is live and the statement handle is closed on error.
        unsafe {
            let stmt = ffi::mysql_stmt_init(self.mysql.as_ptr());
            let stmt =
                StmtHandle(NonNull::new(stmt).ok_or_else(|| {
                    Error::from_mysql(self.mysql.as_ptr(), "mysql_stmt_init failed")
                })?);

            let sql = CString::new(sql)?;
            let mut prepare_ret = 0;
            self.drive(
                DriveOperation::StmtPrepare { stmt, sql: &sql },
                &mut prepare_ret,
            )
            .await?;

            if prepare_ret != 0 {
                let error = Error::from_stmt(stmt.as_ptr(), "mysql_stmt_prepare failed");
                ffi::mysql_stmt_close(stmt.as_ptr());
                return Err(error);
            }

            Ok(stmt)
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: Cached statements and the connection handle are owned here and closed once.
        unsafe {
            for entry in self.statement_cache.values() {
                ffi::mysql_stmt_close(entry.stmt.as_ptr());
            }
            ffi::mysql_close(self.mysql.as_ptr());
        }
    }
}

/// A mysql or mariadb transaction.
///
/// Dropping an unfinished transaction attempts a best-effort rollback.
pub struct Transaction<'a> {
    conn: &'a mut Connection,
    finished: bool,
}

impl<'a> Transaction<'a> {
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
            // SAFETY: The connection is still live and this best-effort rollback does not outlive it.
            let _ = unsafe { ffi::mysql_rollback(self.conn.mysql.as_ptr()) };
        }
    }
}

// SAFETY: `Connection` enforces exclusive access with `&mut self` for all operations and owns the
// only live handle graph for its MariaDB resources.
unsafe impl Send for Connection {}
