use std::ptr::NonNull;
use std::sync::Arc;

use quex_mariadb_sys as ffi;

use super::connection::Connection;
use super::error::{Error, ExecuteResult, Result};
use super::rows::{Metadata, ResultSet};
use super::runtime::{DriveOperation, ParamBindings, StmtHandle, enable_stmt_max_length};
use super::value::{ParamRefSlice, ParamSource, Value, ValueRef, values_as_refs};

/// A mysql or mariadb prepared statement.
///
/// This statement owns a native statement handle until it is dropped.
pub struct Statement<'a> {
    pub(crate) conn: &'a mut Connection,
    pub(crate) stmt: StmtHandle,
    pub(crate) result_metadata: Option<Arc<Metadata>>,
}

impl<'a> Statement<'a> {
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
        // SAFETY: The statement handle is live and parameter buffers live through execution.
        unsafe {
            let stmt = self.stmt;
            let expected = ffi::mysql_stmt_param_count(stmt.as_ptr()) as usize;
            if expected != params.len() {
                return Err(Error::new(format!(
                    "statement expects {} parameters but got {}",
                    expected,
                    params.len()
                )));
            }

            let mut bindings = ParamBindings::new(params);
            if expected != 0
                && ffi::mysql_stmt_bind_param(stmt.as_ptr(), bindings.binds.as_mut_ptr()) != 0
            {
                return Err(Error::from_stmt(
                    stmt.as_ptr(),
                    "mysql_stmt_bind_param failed",
                ));
            }

            let mut execute_ret = 0;
            self.conn
                .drive(DriveOperation::StmtExecute { stmt }, &mut execute_ret)
                .await?;

            if execute_ret != 0 {
                return Err(Error::from_stmt(stmt.as_ptr(), "mysql_stmt_execute failed"));
            }

            if ffi::mysql_stmt_field_count(stmt.as_ptr()) == 0 {
                return Ok(ResultSet::empty());
            }

            enable_stmt_max_length(stmt.as_ptr())?;
            let mut store_ret = 0;
            self.conn
                .drive(DriveOperation::StmtStoreResult { stmt }, &mut store_ret)
                .await?;

            if store_ret != 0 {
                return Err(Error::from_stmt(
                    stmt.as_ptr(),
                    "mysql_stmt_store_result failed",
                ));
            }

            let metadata = match &self.result_metadata {
                Some(metadata) => Arc::clone(metadata),
                None => {
                    let meta = NonNull::new(ffi::mysql_stmt_result_metadata(stmt.as_ptr()))
                        .ok_or_else(|| {
                            Error::from_stmt(
                                stmt.as_ptr(),
                                "statement returned rows but no result metadata",
                            )
                        })?;
                    let metadata = Arc::new(Metadata::from_result(meta));
                    ffi::mysql_free_result(meta.as_ptr());
                    self.result_metadata = Some(Arc::clone(&metadata));
                    metadata
                }
            };

            ResultSet::statement(self.stmt, metadata)
        }
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
        // SAFETY: The statement handle is live and parameter buffers live through execution.
        unsafe {
            let stmt = self.stmt;
            let expected = ffi::mysql_stmt_param_count(stmt.as_ptr()) as usize;
            if expected != params.len() {
                return Err(Error::new(format!(
                    "statement expects {} parameters but got {}",
                    expected,
                    params.len()
                )));
            }

            let mut bindings = ParamBindings::new(params);
            if expected != 0
                && ffi::mysql_stmt_bind_param(stmt.as_ptr(), bindings.binds.as_mut_ptr()) != 0
            {
                return Err(Error::from_stmt(
                    stmt.as_ptr(),
                    "mysql_stmt_bind_param failed",
                ));
            }

            let mut execute_ret = 0;
            self.conn
                .drive(DriveOperation::StmtExecute { stmt }, &mut execute_ret)
                .await?;

            if execute_ret != 0 {
                return Err(Error::from_stmt(stmt.as_ptr(), "mysql_stmt_execute failed"));
            }

            if ffi::mysql_stmt_field_count(stmt.as_ptr()) != 0 {
                return Err(Error::new("statement returned rows; use execute instead"));
            }

            Ok(ExecuteResult {
                rows_affected: ffi::mysql_stmt_affected_rows(stmt.as_ptr()) as u64,
                last_insert_id: ffi::mysql_stmt_insert_id(stmt.as_ptr()) as u64,
            })
        }
    }
}

/// A mysql or mariadb prepared statement cached on its connection.
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
        let key = self.key.clone();
        let (stmt, param_count) = {
            let entry = self
                .conn
                .statement_cache
                .get(key.as_ref())
                .ok_or_else(|| Error::new("cached statement missing"))?;
            (entry.stmt, entry.param_count)
        };

        if param_count != params.len() {
            return Err(Error::new(format!(
                "statement expects {} parameters but got {}",
                param_count,
                params.len()
            )));
        }

        {
            let entry = self
                .conn
                .statement_cache
                .get_mut(key.as_ref())
                .ok_or_else(|| Error::new("cached statement missing"))?;
            // SAFETY: stmt is a live cached statement owned by the connection.
            unsafe {
                if ffi::mysql_stmt_reset(stmt.as_ptr()) != 0 {
                    return Err(Error::from_stmt(stmt.as_ptr(), "mysql_stmt_reset failed"));
                }
            }
            entry.scratch.bind_source(params)?;
            // SAFETY: Bound buffers in scratch live until statement execution completes.
            unsafe {
                if param_count != 0
                    && ffi::mysql_stmt_bind_param(stmt.as_ptr(), entry.scratch.binds.as_mut_ptr())
                        != 0
                {
                    return Err(Error::from_stmt(
                        stmt.as_ptr(),
                        "mysql_stmt_bind_param failed",
                    ));
                }
            }
        }

        let mut execute_ret = 0;
        self.conn
            .drive(DriveOperation::StmtExecute { stmt }, &mut execute_ret)
            .await?;

        if execute_ret != 0 {
            // SAFETY: stmt is live and contains diagnostics for the failed execute call.
            return Err(unsafe { Error::from_stmt(stmt.as_ptr(), "mysql_stmt_execute failed") });
        }

        // SAFETY: stmt is live after execution and field count is query metadata.
        if unsafe { ffi::mysql_stmt_field_count(stmt.as_ptr()) } == 0 {
            return Ok(ResultSet::empty());
        }

        enable_stmt_max_length(stmt.as_ptr())?;
        let mut store_ret = 0;
        self.conn
            .drive(DriveOperation::StmtStoreResult { stmt }, &mut store_ret)
            .await?;

        if store_ret != 0 {
            // SAFETY: stmt is live and contains diagnostics for the failed store-result call.
            return Err(unsafe {
                Error::from_stmt(stmt.as_ptr(), "mysql_stmt_store_result failed")
            });
        }

        let metadata = match self
            .conn
            .statement_cache
            .get(key.as_ref())
            .and_then(|entry| entry.result_metadata.clone())
        {
            Some(metadata) => metadata,
            None => {
                // SAFETY: stmt is live and has result metadata after a row-producing execution.
                let meta = NonNull::new(unsafe { ffi::mysql_stmt_result_metadata(stmt.as_ptr()) })
                    .ok_or_else(|| unsafe {
                        // SAFETY: stmt is live and contains diagnostics for missing metadata.
                        Error::from_stmt(
                            stmt.as_ptr(),
                            "statement returned rows but no result metadata",
                        )
                    })?;
                let metadata = Arc::new(Metadata::from_result(meta));
                // SAFETY: meta was returned by mysql_stmt_result_metadata and is freed once here.
                unsafe {
                    ffi::mysql_free_result(meta.as_ptr());
                }
                self.conn
                    .statement_cache
                    .get_mut(key.as_ref())
                    .expect("cached statement missing")
                    .result_metadata = Some(Arc::clone(&metadata));
                metadata
            }
        };

        ResultSet::statement(stmt, metadata)
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
        let key = self.key.clone();
        let (stmt, param_count) = {
            let entry = self
                .conn
                .statement_cache
                .get(key.as_ref())
                .ok_or_else(|| Error::new("cached statement missing"))?;
            (entry.stmt, entry.param_count)
        };

        if param_count != params.len() {
            return Err(Error::new(format!(
                "statement expects {} parameters but got {}",
                param_count,
                params.len()
            )));
        }

        {
            let entry = self
                .conn
                .statement_cache
                .get_mut(key.as_ref())
                .ok_or_else(|| Error::new("cached statement missing"))?;
            // SAFETY: stmt is a live cached statement owned by the connection.
            unsafe {
                if ffi::mysql_stmt_reset(stmt.as_ptr()) != 0 {
                    return Err(Error::from_stmt(stmt.as_ptr(), "mysql_stmt_reset failed"));
                }
            }
            entry.scratch.bind_source(params)?;
            // SAFETY: Bound buffers in scratch live until statement execution completes.
            unsafe {
                if param_count != 0
                    && ffi::mysql_stmt_bind_param(stmt.as_ptr(), entry.scratch.binds.as_mut_ptr())
                        != 0
                {
                    return Err(Error::from_stmt(
                        stmt.as_ptr(),
                        "mysql_stmt_bind_param failed",
                    ));
                }
            }
        }

        let mut execute_ret = 0;
        self.conn
            .drive(DriveOperation::StmtExecute { stmt }, &mut execute_ret)
            .await?;

        if execute_ret != 0 {
            // SAFETY: stmt is live and contains diagnostics for the failed execute call.
            return Err(unsafe { Error::from_stmt(stmt.as_ptr(), "mysql_stmt_execute failed") });
        }

        // SAFETY: stmt is live after execution and field count is query metadata.
        if unsafe { ffi::mysql_stmt_field_count(stmt.as_ptr()) } != 0 {
            return Err(Error::new("statement returned rows; use execute instead"));
        }

        // SAFETY: stmt is live after a successful non-row execution.
        let rows_affected = unsafe { ffi::mysql_stmt_affected_rows(stmt.as_ptr()) as u64 };
        // SAFETY: stmt is live after a successful non-row execution.
        let last_insert_id = unsafe { ffi::mysql_stmt_insert_id(stmt.as_ptr()) as u64 };
        Ok(ExecuteResult {
            rows_affected,
            last_insert_id,
        })
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        // SAFETY: This statement is uniquely owned and closed exactly once here.
        unsafe {
            ffi::mysql_stmt_close(self.stmt.as_ptr());
        }
    }
}
