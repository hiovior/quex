use std::sync::mpsc::{self, SyncSender};
use std::thread::{self, JoinHandle};

use tokio::sync::oneshot;

use super::error::{Error, Result};
use super::options::ConnectOptions;
use super::rows::ResultSet;
use super::runtime::{Command, worker_main};
use super::statement::{CachedStatement, ExecuteResult, Statement};

/// A single sqlite connection.
///
/// sqlite work is run on a dedicated worker thread so the async api does not
/// block the tokio runtime.
pub struct Connection {
    pub(crate) tx: SyncSender<Command>,
    join: Option<JoinHandle<()>>,
    next_statement_id: u64,
}

impl Connection {
    /// Opens a new sqlite connection.
    pub async fn connect(options: ConnectOptions) -> Result<Self> {
        let (tx, rx) = mpsc::sync_channel::<Command>(128);
        let (ready_tx, ready_rx) = oneshot::channel();

        let join = thread::Builder::new()
            .name("quex-driver-sqlite".into())
            .spawn(move || worker_main(options, rx, ready_tx))
            .map_err(|err| Error::new(err.to_string()))?;

        ready_rx
            .await
            .map_err(|_| Error::new("sqlite worker failed to initialize"))?
            .map_err(Error::from_worker)?;

        Ok(Self {
            tx,
            join: Some(join),
            next_statement_id: 1,
        })
    }

    /// Runs SQL and returns rows.
    pub async fn query(&mut self, sql: &str) -> Result<ResultSet> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send(Command::Query {
            sql: sql.into(),
            reply: reply_tx,
        })?;
        let data = reply_rx
            .await
            .map_err(|_| Error::new("sqlite worker dropped query reply"))?
            .map_err(Error::from_worker)?;
        Ok(ResultSet::new(data.columns, data.rows))
    }

    /// Runs SQL when no rows are expected.
    pub async fn execute(&mut self, sql: &str) -> Result<ExecuteResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send(Command::Execute {
            sql: sql.into(),
            reply: reply_tx,
        })?;
        reply_rx
            .await
            .map_err(|_| Error::new("sqlite worker dropped execute reply"))?
            .map_err(Error::from_worker)
    }

    /// Runs one or more SQL statements as a batch.
    ///
    /// This is useful for schema setup and transaction control.
    pub async fn execute_batch(&mut self, sql: &str) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send(Command::ExecuteBatch {
            sql: sql.into(),
            reply: reply_tx,
        })?;
        reply_rx
            .await
            .map_err(|_| Error::new("sqlite worker dropped execute_batch reply"))?
            .map_err(Error::from_worker)
    }

    /// Prepares a statement.
    ///
    /// The statement is finalized when the returned [`Statement`] is dropped.
    pub async fn prepare(&mut self, sql: &str) -> Result<Statement<'_>> {
        let statement_id = self.prepare_inner(sql, false).await?;
        Ok(Statement {
            conn: self,
            statement_id,
        })
    }

    /// Prepares a statement and keeps it cached on the connection.
    pub async fn prepare_cached(&mut self, sql: &str) -> Result<CachedStatement<'_>> {
        let statement_id = self.prepare_inner(sql, true).await?;
        Ok(CachedStatement {
            conn: self,
            statement_id,
        })
    }

    /// Starts an immediate transaction.
    ///
    /// Dropping the transaction without commit or rollback attempts a
    /// best-effort rollback.
    pub async fn begin(&mut self) -> Result<Transaction<'_>> {
        self.execute_batch("begin immediate").await?;
        Ok(Transaction {
            conn: self,
            finished: false,
        })
    }

    async fn prepare_inner(&mut self, sql: &str, cached: bool) -> Result<u64> {
        let statement_id = self.next_statement_id;
        self.next_statement_id += 1;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.send(Command::Prepare {
            sql: sql.into(),
            statement_id,
            cached,
            reply: reply_tx,
        })?;
        reply_rx
            .await
            .map_err(|_| Error::new("sqlite worker dropped prepare reply"))?
            .map_err(Error::from_worker)
    }

    pub(crate) fn send(&self, command: Command) -> Result<()> {
        self.tx
            .send(command)
            .map_err(|_| Error::new("sqlite worker channel closed"))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.tx.send(Command::Close);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// A sqlite transaction.
///
/// Dropping an unfinished transaction attempts a best-effort rollback.
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
        self.conn.execute_batch("commit").await
    }

    /// Rolls the transaction back.
    pub async fn rollback(mut self) -> Result<()> {
        self.finished = true;
        self.conn.execute_batch("rollback").await
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.conn.tx.send(Command::ExecuteBatch {
                sql: "rollback".into(),
                reply: oneshot::channel().0,
            });
        }
    }
}
