use tokio::sync::oneshot;

use super::connection::Connection;
use super::error::{Error, Result};
use super::rows::ResultSet;
use super::runtime::Command;
use super::value::Value;

/// Result metadata for a sqlite statement that does not return rows.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExecuteResult {
    /// Number of rows changed by the statement.
    pub rows_affected: u64,
    /// Last row id reported by sqlite.
    pub last_insert_rowid: i64,
}

/// A sqlite prepared statement.
///
/// The statement is finalized when this value is dropped.
pub struct Statement<'a> {
    pub(crate) conn: &'a mut Connection,
    pub(crate) statement_id: u64,
}

impl Statement<'_> {
    /// Runs the statement with owned values and returns rows.
    pub async fn execute(&mut self, params: &[Value]) -> Result<ResultSet> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.conn.send(Command::ExecutePrepared {
            statement_id: self.statement_id,
            params: params.to_vec(),
            reply: reply_tx,
        })?;
        let data = reply_rx
            .await
            .map_err(|_| Error::new("sqlite worker dropped statement reply"))?
            .map_err(Error::from_worker)?;
        Ok(ResultSet::new(data.columns, data.rows))
    }

    /// Runs the statement with owned values when no rows are expected.
    pub async fn exec(&mut self, params: &[Value]) -> Result<ExecuteResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.conn.send(Command::ExecutePreparedExec {
            statement_id: self.statement_id,
            params: params.to_vec(),
            reply: reply_tx,
        })?;
        reply_rx
            .await
            .map_err(|_| Error::new("sqlite worker dropped statement exec reply"))?
            .map_err(Error::from_worker)
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        let _ = self.conn.tx.send(Command::Finalize {
            statement_id: self.statement_id,
        });
    }
}

/// A sqlite prepared statement cached on its connection.
pub struct CachedStatement<'a> {
    pub(crate) conn: &'a mut Connection,
    pub(crate) statement_id: u64,
}

impl CachedStatement<'_> {
    /// Runs the cached statement with owned values and returns rows.
    pub async fn execute(&mut self, params: &[Value]) -> Result<ResultSet> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.conn.send(Command::ExecutePrepared {
            statement_id: self.statement_id,
            params: params.to_vec(),
            reply: reply_tx,
        })?;
        let data = reply_rx
            .await
            .map_err(|_| Error::new("sqlite worker dropped cached statement reply"))?
            .map_err(Error::from_worker)?;
        Ok(ResultSet::new(data.columns, data.rows))
    }

    /// Runs the cached statement with owned values when no rows are expected.
    pub async fn exec(&mut self, params: &[Value]) -> Result<ExecuteResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.conn.send(Command::ExecutePreparedExec {
            statement_id: self.statement_id,
            params: params.to_vec(),
            reply: reply_tx,
        })?;
        reply_rx
            .await
            .map_err(|_| Error::new("sqlite worker dropped cached statement exec reply"))?
            .map_err(Error::from_worker)
    }
}
