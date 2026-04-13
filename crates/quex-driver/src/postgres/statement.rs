use std::ffi::CString;
use std::sync::Arc;

use super::connection::Connection;
use super::error::{ExecuteResult, Result};
use super::rows::{Metadata, ResultSet};
use super::runtime::{ParamScratch, execute_prepared, execute_prepared_exec};
use super::value::{ParamRefSlice, ParamSource, Value, ValueRef, values_as_refs};

/// A postgres prepared statement.
///
/// This wraps a statement prepared on one connection.
pub struct Statement<'a> {
    pub(crate) conn: &'a mut Connection,
    pub(crate) name: CString,
    pub(crate) result_metadata: Option<Arc<Metadata>>,
    pub(crate) scratch: ParamScratch,
}

impl Statement<'_> {
    /// Runs the statement with owned values and returns rows.
    pub async fn execute(&mut self, params: &[Value]) -> Result<ResultSet> {
        let refs = values_as_refs(params);
        self.execute_ref(&refs).await
    }

    /// Runs the statement with borrowed values and returns rows.
    pub async fn execute_ref(&mut self, params: &[ValueRef<'_>]) -> Result<ResultSet> {
        self.execute_source(&ParamRefSlice(params)).await
    }

    /// Runs the statement with a custom parameter source and returns rows.
    pub async fn execute_source<P>(&mut self, params: &P) -> Result<ResultSet>
    where
        P: ParamSource + ?Sized,
    {
        self.conn.ensure_ready()?;
        execute_prepared(
            self.conn.conn,
            &self.conn.socket,
            &self.conn.state,
            &self.name,
            &mut self.scratch,
            params,
            &mut self.result_metadata,
        )
        .await
    }

    /// Runs the statement with owned values when no rows are expected.
    pub async fn exec(&mut self, params: &[Value]) -> Result<ExecuteResult> {
        let refs = values_as_refs(params);
        self.exec_ref(&refs).await
    }

    /// Runs the statement with borrowed values when no rows are expected.
    pub async fn exec_ref(&mut self, params: &[ValueRef<'_>]) -> Result<ExecuteResult> {
        self.exec_source(&ParamRefSlice(params)).await
    }

    /// Runs the statement with a custom parameter source when no rows are
    /// expected.
    pub async fn exec_source<P>(&mut self, params: &P) -> Result<ExecuteResult>
    where
        P: ParamSource + ?Sized,
    {
        self.conn.ensure_ready()?;
        execute_prepared_exec(
            self.conn.conn,
            &self.conn.socket,
            &self.conn.state,
            &self.name,
            &mut self.scratch,
            params,
        )
        .await
    }
}

/// A postgres prepared statement cached on its connection.
pub struct CachedStatement<'a> {
    pub(crate) conn: &'a mut Connection,
    pub(crate) key: Box<str>,
}

impl CachedStatement<'_> {
    /// Runs the cached statement with owned values and returns rows.
    pub async fn execute(&mut self, params: &[Value]) -> Result<ResultSet> {
        let refs = values_as_refs(params);
        self.execute_ref(&refs).await
    }

    /// Runs the cached statement with borrowed values and returns rows.
    pub async fn execute_ref(&mut self, params: &[ValueRef<'_>]) -> Result<ResultSet> {
        self.execute_source(&ParamRefSlice(params)).await
    }

    /// Runs the cached statement with a custom parameter source and returns rows.
    pub async fn execute_source<P>(&mut self, params: &P) -> Result<ResultSet>
    where
        P: ParamSource + ?Sized,
    {
        let conn = &mut *self.conn;
        conn.ensure_ready()?;
        let entry = conn
            .statement_cache
            .get_mut(&*self.key)
            .expect("cached statement missing");
        execute_prepared(
            conn.conn,
            &conn.socket,
            &conn.state,
            &entry.name,
            &mut entry.scratch,
            params,
            &mut entry.result_metadata,
        )
        .await
    }

    /// Runs the cached statement with owned values when no rows are expected.
    pub async fn exec(&mut self, params: &[Value]) -> Result<ExecuteResult> {
        let refs = values_as_refs(params);
        self.exec_ref(&refs).await
    }

    /// Runs the cached statement with borrowed values when no rows are expected.
    pub async fn exec_ref(&mut self, params: &[ValueRef<'_>]) -> Result<ExecuteResult> {
        self.exec_source(&ParamRefSlice(params)).await
    }

    /// Runs the cached statement with a custom parameter source when no rows are
    /// expected.
    pub async fn exec_source<P>(&mut self, params: &P) -> Result<ExecuteResult>
    where
        P: ParamSource + ?Sized,
    {
        let conn = &mut *self.conn;
        conn.ensure_ready()?;
        let entry = conn
            .statement_cache
            .get_mut(&*self.key)
            .expect("cached statement missing");
        execute_prepared_exec(
            conn.conn,
            &conn.socket,
            &conn.state,
            &entry.name,
            &mut entry.scratch,
            params,
        )
        .await
    }
}
