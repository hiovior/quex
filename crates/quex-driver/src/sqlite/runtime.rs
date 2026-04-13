use std::collections::HashMap;
use std::ffi::{CStr, CString, NulError};
use std::path::PathBuf;
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::mpsc::Receiver;

use quex_sqlite3_sys as ffi;
use tokio::sync::oneshot;

use super::error::Error;
use super::options::ConnectOptions;
use super::rows::Column;
use super::statement::ExecuteResult;
use super::value::Value;

const SQLITE_TRANSIENT_SENTINEL: isize = -1;

pub(crate) enum Command {
    Query {
        sql: String,
        reply: oneshot::Sender<std::result::Result<QueryData, WorkerError>>,
    },
    Execute {
        sql: String,
        reply: oneshot::Sender<std::result::Result<ExecuteResult, WorkerError>>,
    },
    ExecuteBatch {
        sql: String,
        reply: oneshot::Sender<std::result::Result<(), WorkerError>>,
    },
    Prepare {
        sql: String,
        statement_id: u64,
        cached: bool,
        reply: oneshot::Sender<std::result::Result<u64, WorkerError>>,
    },
    ExecutePrepared {
        statement_id: u64,
        params: Vec<Value>,
        reply: oneshot::Sender<std::result::Result<QueryData, WorkerError>>,
    },
    ExecutePreparedExec {
        statement_id: u64,
        params: Vec<Value>,
        reply: oneshot::Sender<std::result::Result<ExecuteResult, WorkerError>>,
    },
    Finalize {
        statement_id: u64,
    },
    Close,
}

#[derive(Clone)]
pub(crate) struct QueryData {
    pub(crate) columns: Vec<Column>,
    pub(crate) rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone)]
pub(crate) struct WorkerError {
    pub(crate) code: Option<i32>,
    pub(crate) message: String,
}

struct WorkerStatement {
    stmt: StatementHandle,
    cached: bool,
}

struct WorkerState {
    db: DbHandle,
    statements: HashMap<u64, WorkerStatement>,
    cached_by_sql: HashMap<Box<str>, u64>,
}

pub(crate) fn worker_main(
    options: ConnectOptions,
    rx: Receiver<Command>,
    ready_tx: oneshot::Sender<std::result::Result<(), WorkerError>>,
) {
    let mut state = match WorkerState::open(options) {
        Ok(state) => {
            let _ = ready_tx.send(Ok(()));
            state
        }
        Err(err) => {
            let _ = ready_tx.send(Err(err));
            return;
        }
    };

    while let Ok(command) = rx.recv() {
        match command {
            Command::Query { sql, reply } => {
                let _ = reply.send(state.query(&sql));
            }
            Command::Execute { sql, reply } => {
                let _ = reply.send(state.execute(&sql));
            }
            Command::ExecuteBatch { sql, reply } => {
                let _ = reply.send(state.execute_batch(&sql));
            }
            Command::Prepare {
                sql,
                statement_id,
                cached,
                reply,
            } => {
                let _ = reply.send(state.prepare(&sql, statement_id, cached));
            }
            Command::ExecutePrepared {
                statement_id,
                params,
                reply,
            } => {
                let _ = reply.send(state.execute_prepared(statement_id, &params));
            }
            Command::ExecutePreparedExec {
                statement_id,
                params,
                reply,
            } => {
                let _ = reply.send(state.execute_prepared_exec(statement_id, &params));
            }
            Command::Finalize { statement_id } => state.finalize(statement_id),
            Command::Close => break,
        }
    }
}

impl WorkerState {
    fn open(options: ConnectOptions) -> std::result::Result<Self, WorkerError> {
        // SAFETY: SQLite receives nul-terminated paths and initialized output pointers.
        unsafe {
            let path = if options.in_memory {
                CString::new(":memory:").expect("valid memory path")
            } else {
                CString::new(
                    options
                        .path
                        .unwrap_or_else(|| PathBuf::from("sqlite.db"))
                        .to_string_lossy()
                        .into_owned(),
                )
                .map_err(worker_from_nul)?
            };

            let mut db = ptr::null_mut();
            let mut flags = ffi::SQLITE_OPEN_NOMUTEX;
            flags |= if options.read_only {
                ffi::SQLITE_OPEN_READONLY
            } else {
                ffi::SQLITE_OPEN_READWRITE
            };
            if options.create_if_missing && !options.read_only {
                flags |= ffi::SQLITE_OPEN_CREATE;
            }

            let code = ffi::sqlite3_open_v2(path.as_ptr(), &mut db, flags as i32, ptr::null());
            if code != ffi::SQLITE_OK as i32 {
                return Err(worker_from_db(db, code, "sqlite3_open_v2 failed"));
            }

            let db = DbHandle(NonNull::new(db).ok_or_else(|| WorkerError {
                code: None,
                message: "sqlite3_open_v2 returned null".into(),
            })?);

            if let Some(timeout) = options.busy_timeout {
                let code = ffi::sqlite3_busy_timeout(
                    db.as_ptr(),
                    timeout.as_millis().min(i32::MAX as u128) as i32,
                );
                if code != ffi::SQLITE_OK as i32 {
                    return Err(worker_from_db(
                        db.as_ptr(),
                        code,
                        "sqlite3_busy_timeout failed",
                    ));
                }
            }

            Ok(Self {
                db,
                statements: HashMap::new(),
                cached_by_sql: HashMap::new(),
            })
        }
    }

    fn query(&mut self, sql: &str) -> std::result::Result<QueryData, WorkerError> {
        // SAFETY: the temporary statement is owned by this worker and finalized before return.
        unsafe {
            let stmt = self.prepare_temp(sql, false)?;
            let result = self.collect_rows(stmt);
            ffi::sqlite3_finalize(stmt.as_ptr());
            result
        }
    }

    fn execute(&mut self, sql: &str) -> std::result::Result<ExecuteResult, WorkerError> {
        // SAFETY: the temporary statement is owned by this worker and finalized before return.
        unsafe {
            let stmt = self.prepare_temp(sql, false)?;
            let result = self.step_to_completion(stmt);
            ffi::sqlite3_finalize(stmt.as_ptr());
            result
        }
    }

    fn execute_batch(&mut self, sql: &str) -> std::result::Result<(), WorkerError> {
        // SAFETY: db and sql are live for the duration of sqlite3_exec.
        unsafe {
            let c_sql = CString::new(sql).map_err(worker_from_nul)?;
            let code = ffi::sqlite3_exec(
                self.db.as_ptr(),
                c_sql.as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            if code != ffi::SQLITE_OK as i32 {
                return Err(worker_from_db(
                    self.db.as_ptr(),
                    code,
                    "sqlite3_exec failed",
                ));
            }
            Ok(())
        }
    }

    fn prepare(
        &mut self,
        sql: &str,
        statement_id: u64,
        cached: bool,
    ) -> std::result::Result<u64, WorkerError> {
        if cached {
            if let Some(existing) = self.cached_by_sql.get(sql).copied() {
                return Ok(existing);
            }
        }

        // SAFETY: the prepared statement is owned by this worker state.
        let stmt = unsafe { self.prepare_temp(sql, cached)? };
        self.statements
            .insert(statement_id, WorkerStatement { stmt, cached });
        if cached {
            self.cached_by_sql.insert(sql.into(), statement_id);
        }
        Ok(statement_id)
    }

    fn execute_prepared(
        &mut self,
        statement_id: u64,
        params: &[Value],
    ) -> std::result::Result<QueryData, WorkerError> {
        let stmt = self
            .statements
            .get(&statement_id)
            .ok_or_else(|| WorkerError {
                code: None,
                message: "sqlite statement missing".into(),
            })?
            .stmt;

        // SAFETY: the statement belongs to this worker and is not used concurrently.
        unsafe {
            let code = ffi::sqlite3_reset(stmt.as_ptr());
            if code != ffi::SQLITE_OK as i32 {
                return Err(worker_from_db(
                    self.db.as_ptr(),
                    code,
                    "sqlite3_reset failed",
                ));
            }
            let code = ffi::sqlite3_clear_bindings(stmt.as_ptr());
            if code != ffi::SQLITE_OK as i32 {
                return Err(worker_from_db(
                    self.db.as_ptr(),
                    code,
                    "sqlite3_clear_bindings failed",
                ));
            }
            bind_params(stmt, params)?;
            self.collect_rows(stmt)
        }
    }

    fn execute_prepared_exec(
        &mut self,
        statement_id: u64,
        params: &[Value],
    ) -> std::result::Result<ExecuteResult, WorkerError> {
        let stmt = self
            .statements
            .get(&statement_id)
            .ok_or_else(|| WorkerError {
                code: None,
                message: "sqlite statement missing".into(),
            })?
            .stmt;

        // SAFETY: the statement belongs to this worker and is not used concurrently.
        unsafe {
            let code = ffi::sqlite3_reset(stmt.as_ptr());
            if code != ffi::SQLITE_OK as i32 {
                return Err(worker_from_db(
                    self.db.as_ptr(),
                    code,
                    "sqlite3_reset failed",
                ));
            }
            let code = ffi::sqlite3_clear_bindings(stmt.as_ptr());
            if code != ffi::SQLITE_OK as i32 {
                return Err(worker_from_db(
                    self.db.as_ptr(),
                    code,
                    "sqlite3_clear_bindings failed",
                ));
            }
            bind_params(stmt, params)?;
            self.step_to_completion(stmt)
        }
    }

    fn finalize(&mut self, statement_id: u64) {
        if let Some(statement) = self.statements.remove(&statement_id) {
            if statement.cached {
                return;
            }
            // SAFETY: non-cached statement is owned here and finalized exactly once.
            unsafe {
                ffi::sqlite3_finalize(statement.stmt.as_ptr());
            }
        }
    }

    unsafe fn prepare_temp(
        &self,
        sql: &str,
        persistent: bool,
    ) -> std::result::Result<StatementHandle, WorkerError> {
        let c_sql = CString::new(sql).map_err(worker_from_nul)?;
        let mut stmt = ptr::null_mut();
        let mut tail = ptr::null();
        let flags = if persistent {
            ffi::SQLITE_PREPARE_PERSISTENT
        } else {
            0
        };
        // SAFETY: db and c_sql are live and stmt and tail point to initialized output storage.
        let code = unsafe {
            ffi::sqlite3_prepare_v3(
                self.db.as_ptr(),
                c_sql.as_ptr(),
                -1,
                flags,
                &mut stmt,
                &mut tail,
            )
        };
        if code != ffi::SQLITE_OK as i32 {
            return Err(worker_from_db(
                self.db.as_ptr(),
                code,
                "sqlite3_prepare_v3 failed",
            ));
        }
        let stmt = StatementHandle(NonNull::new(stmt).ok_or_else(|| WorkerError {
            code: None,
            message: "sqlite3_prepare_v3 returned null".into(),
        })?);
        // SAFETY: tail is returned by sqlite3_prepare_v3 for this SQL string.
        if unsafe { !tail_is_empty(tail) } {
            // SAFETY: stmt was prepared above and is finalized exactly once on this error path.
            unsafe { ffi::sqlite3_finalize(stmt.as_ptr()) };
            return Err(WorkerError {
                code: None,
                message: "multiple SQL statements are not allowed here".into(),
            });
        }
        Ok(stmt)
    }

    unsafe fn collect_rows(
        &self,
        stmt: StatementHandle,
    ) -> std::result::Result<QueryData, WorkerError> {
        // SAFETY: stmt is a live prepared statement owned by the worker.
        let columns = unsafe { statement_columns(stmt) };
        let mut rows = Vec::new();

        loop {
            // SAFETY: stmt is live and stepped only on the worker thread.
            let code = unsafe { ffi::sqlite3_step(stmt.as_ptr()) };
            if code == ffi::SQLITE_ROW as i32 {
                // SAFETY: stmt currently points at a valid row.
                rows.push(unsafe { read_row(stmt) });
                continue;
            }
            if code == ffi::SQLITE_DONE as i32 {
                // SAFETY: stmt is live and can be reset after completion.
                let reset = unsafe { ffi::sqlite3_reset(stmt.as_ptr()) };
                if reset != ffi::SQLITE_OK as i32 {
                    return Err(worker_from_db(
                        self.db.as_ptr(),
                        reset,
                        "sqlite3_reset failed",
                    ));
                }
                return Ok(QueryData { columns, rows });
            }
            return Err(worker_from_db(
                self.db.as_ptr(),
                code,
                "sqlite3_step failed",
            ));
        }
    }

    unsafe fn step_to_completion(
        &self,
        stmt: StatementHandle,
    ) -> std::result::Result<ExecuteResult, WorkerError> {
        loop {
            // SAFETY: stmt is live and stepped only on the worker thread.
            let code = unsafe { ffi::sqlite3_step(stmt.as_ptr()) };
            if code == ffi::SQLITE_ROW as i32 {
                continue;
            }
            if code == ffi::SQLITE_DONE as i32 {
                let result = ExecuteResult {
                    // SAFETY: db is live and owned by this worker.
                    rows_affected: unsafe { ffi::sqlite3_changes64(self.db.as_ptr()) as u64 },
                    // SAFETY: db is live and owned by this worker.
                    last_insert_rowid: unsafe { ffi::sqlite3_last_insert_rowid(self.db.as_ptr()) },
                };
                // SAFETY: stmt is live and can be reset after completion.
                let reset = unsafe { ffi::sqlite3_reset(stmt.as_ptr()) };
                if reset != ffi::SQLITE_OK as i32 {
                    return Err(worker_from_db(
                        self.db.as_ptr(),
                        reset,
                        "sqlite3_reset failed",
                    ));
                }
                return Ok(result);
            }
            return Err(worker_from_db(
                self.db.as_ptr(),
                code,
                "sqlite3_step failed",
            ));
        }
    }
}

impl Drop for WorkerState {
    fn drop(&mut self) {
        for statement in self.statements.values() {
            // SAFETY: each statement is owned by this worker and finalized during drop.
            unsafe {
                ffi::sqlite3_finalize(statement.stmt.as_ptr());
            }
        }
        // SAFETY: db is owned by this worker and closed once after statements are finalized.
        unsafe {
            ffi::sqlite3_close(self.db.as_ptr());
        }
    }
}

#[derive(Clone, Copy)]
struct DbHandle(NonNull<ffi::sqlite3>);

impl DbHandle {
    #[inline]
    fn as_ptr(self) -> *mut ffi::sqlite3 {
        self.0.as_ptr()
    }
}

#[derive(Clone, Copy)]
struct StatementHandle(NonNull<ffi::sqlite3_stmt>);

impl StatementHandle {
    #[inline]
    fn as_ptr(self) -> *mut ffi::sqlite3_stmt {
        self.0.as_ptr()
    }
}

// SAFETY: `sqlite3*` is sent to the dedicated worker thread once and then only used there until
// shutdown; it is never touched concurrently from the async-facing side.
unsafe impl Send for DbHandle {}
// SAFETY: `sqlite3_stmt*` values are created, stepped, reset, cached, and finalized only on that
// same worker thread together with their owning database handle.
unsafe impl Send for StatementHandle {}

fn worker_from_nul(error: NulError) -> WorkerError {
    WorkerError {
        code: None,
        message: error.to_string(),
    }
}

fn worker_from_db(db: *mut ffi::sqlite3, code: i32, fallback: impl Into<String>) -> WorkerError {
    // SAFETY: db is either null or a live SQLite handle from the failing operation.
    unsafe { Error::from_db(db, code, fallback) }.into()
}

impl From<Error> for WorkerError {
    fn from(value: Error) -> Self {
        Self {
            code: value.code,
            message: value.message,
        }
    }
}

unsafe fn bind_params(
    stmt: StatementHandle,
    params: &[Value],
) -> std::result::Result<(), WorkerError> {
    for (index, value) in params.iter().enumerate() {
        let index = (index + 1) as i32;
        let code = match value {
            // SAFETY: stmt is live and the parameter index is within the supplied parameter list.
            Value::Null => unsafe { ffi::sqlite3_bind_null(stmt.as_ptr(), index) },
            // SAFETY: stmt is live and SQLite copies scalar values immediately.
            Value::I64(value) => unsafe { ffi::sqlite3_bind_int64(stmt.as_ptr(), index, *value) },
            // SAFETY: stmt is live and SQLite copies scalar values immediately.
            Value::F64(value) => unsafe { ffi::sqlite3_bind_double(stmt.as_ptr(), index, *value) },
            // SAFETY: SQLite copies the string because SQLITE_TRANSIENT is used.
            Value::String(value) => unsafe {
                ffi::sqlite3_bind_text(
                    stmt.as_ptr(),
                    index,
                    value.as_ptr().cast(),
                    value.len() as i32,
                    sqlite_transient(),
                )
            },
            // SAFETY: SQLite copies the bytes because SQLITE_TRANSIENT is used.
            Value::Bytes(value) => unsafe {
                ffi::sqlite3_bind_blob(
                    stmt.as_ptr(),
                    index,
                    value.as_ptr().cast(),
                    value.len() as i32,
                    sqlite_transient(),
                )
            },
        };

        if code != ffi::SQLITE_OK as i32 {
            return Err(WorkerError {
                code: Some(code),
                message: "sqlite bind failed".into(),
            });
        }
    }
    Ok(())
}

unsafe fn statement_columns(stmt: StatementHandle) -> Vec<Column> {
    // SAFETY: stmt is live and owned by the worker.
    let count = unsafe { ffi::sqlite3_column_count(stmt.as_ptr()) as usize };
    let mut columns = Vec::with_capacity(count);
    for index in 0..count {
        // SAFETY: SQLite returns a valid column name pointer for a live statement.
        let name =
            c_sqlite_string(unsafe { ffi::sqlite3_column_name(stmt.as_ptr(), index as i32) })
                .unwrap_or_default();
        let declared_type = {
            // SAFETY: stmt is live and index is within the column count.
            let ptr = unsafe { ffi::sqlite3_column_decltype(stmt.as_ptr(), index as i32) };
            if ptr.is_null() {
                None
            } else {
                Some(
                    // SAFETY: non-null declared type pointers are nul-terminated by SQLite.
                    unsafe { CStr::from_ptr(ptr) }
                        .to_string_lossy()
                        .into_owned(),
                )
            }
        };
        columns.push(Column {
            name,
            declared_type,
            nullable: true,
        });
    }
    columns
}

unsafe fn read_row(stmt: StatementHandle) -> Vec<Value> {
    // SAFETY: stmt is positioned on a row and is live.
    let count = unsafe { ffi::sqlite3_column_count(stmt.as_ptr()) as usize };
    let mut row = Vec::with_capacity(count);
    for index in 0..count {
        let index = index as i32;
        row.push(
            // SAFETY: stmt is on a row and index is within the column count.
            match unsafe { ffi::sqlite3_column_type(stmt.as_ptr(), index) } {
                x if x == ffi::SQLITE_NULL as i32 => Value::Null,
                x if x == ffi::SQLITE_INTEGER as i32 => {
                    // SAFETY: stmt is on a row and index is within the column count.
                    Value::I64(unsafe { ffi::sqlite3_column_int64(stmt.as_ptr(), index) })
                }
                x if x == ffi::SQLITE_FLOAT as i32 => {
                    // SAFETY: stmt is on a row and index is within the column count.
                    Value::F64(unsafe { ffi::sqlite3_column_double(stmt.as_ptr(), index) })
                }
                x if x == ffi::SQLITE_TEXT as i32 => {
                    // SAFETY: SQLite returns text bytes valid until the next statement step.
                    let ptr = unsafe { ffi::sqlite3_column_text(stmt.as_ptr(), index) };
                    // SAFETY: stmt is on a row and index is within the column count.
                    let len = unsafe { ffi::sqlite3_column_bytes(stmt.as_ptr(), index) as usize };
                    // SAFETY: ptr and len describe the current SQLite text cell.
                    let bytes = unsafe { sqlite_bytes(ptr.cast::<u8>(), len) };
                    Value::String(String::from_utf8_lossy(bytes).into_owned())
                }
                _ => {
                    // SAFETY: SQLite returns blob bytes valid until the next statement step.
                    let ptr = unsafe { ffi::sqlite3_column_blob(stmt.as_ptr(), index) };
                    // SAFETY: stmt is on a row and index is within the column count.
                    let len = unsafe { ffi::sqlite3_column_bytes(stmt.as_ptr(), index) as usize };
                    // SAFETY: ptr and len describe the current SQLite blob cell.
                    let bytes = unsafe { sqlite_bytes(ptr.cast::<u8>(), len) };
                    Value::Bytes(bytes.to_vec())
                }
            },
        );
    }
    row
}

unsafe fn sqlite_bytes<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if len == 0 {
        &[]
    } else {
        debug_assert!(!ptr.is_null());
        // SAFETY: callers guarantee `ptr` points to `len` live bytes for the current row cell.
        unsafe { slice::from_raw_parts(ptr, len) }
    }
}

unsafe fn tail_is_empty(tail: *const i8) -> bool {
    if tail.is_null() {
        return true;
    }
    // SAFETY: tail points inside SQLite's prepared SQL buffer and is nul-terminated.
    let bytes = unsafe { CStr::from_ptr(tail) }.to_bytes();
    bytes.iter().all(|byte| byte.is_ascii_whitespace())
}

#[inline]
pub(super) fn c_sqlite_string(ptr: *const i8) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        // SAFETY: non-null SQLite string pointers are NUL-terminated for the duration of the call.
        Some(
            unsafe { CStr::from_ptr(ptr) }
                .to_string_lossy()
                .into_owned(),
        )
    }
}

#[inline]
fn sqlite_transient() -> Option<unsafe extern "C" fn(*mut std::ffi::c_void)> {
    // SQLite documents the sentinel destructor value -1 as SQLITE_TRANSIENT. The binding exposes
    // the destructor as an optional callback, so the FFI boundary requires representing that
    // non-null sentinel bit-pattern as the callback type.
    // SAFETY: the bit-pattern is the documented SQLite sentinel and is never invoked as a Rust
    // function pointer; SQLite only compares it to decide whether to copy the bound data.
    unsafe {
        Some(std::mem::transmute::<
            isize,
            unsafe extern "C" fn(*mut std::ffi::c_void),
        >(SQLITE_TRANSIENT_SENTINEL))
    }
}

#[cfg(test)]
mod tests {
    use super::super::connection::Connection;
    use super::*;

    #[test]
    fn c_sqlite_string_handles_null() {
        assert_eq!(c_sqlite_string(std::ptr::null()), None);
    }

    #[test]
    fn sqlite_transient_uses_documented_sentinel() {
        let transient = sqlite_transient();
        let bits = unsafe {
            std::mem::transmute::<Option<unsafe extern "C" fn(*mut std::ffi::c_void)>, isize>(
                transient,
            )
        };
        assert_eq!(bits, SQLITE_TRANSIENT_SENTINEL);
    }

    #[test]
    fn sqlite_bytes_allows_zero_length_null_pointer() {
        let bytes = unsafe { sqlite_bytes(std::ptr::null(), 0) };
        assert!(bytes.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_in_memory_round_trip() {
        let mut conn = Connection::connect(ConnectOptions::new().in_memory())
            .await
            .expect("connect");
        conn.execute_batch(
            "create table users(id integer primary key, name text not null, score integer not null);",
        )
        .await
        .expect("create");

        {
            let mut stmt = conn
                .prepare_cached("insert into users(name, score) values(?, ?)")
                .await
                .expect("prepare");
            stmt.execute(&[Value::String("Ada".into()), Value::I64(37)])
                .await
                .expect("insert");
        }

        let mut rows = conn
            .query("select id, name, score from users order by id")
            .await
            .expect("query");
        let row = rows.next().await.expect("next").expect("row");
        assert_eq!(row.get_i64(0).expect("id"), 1);
        assert_eq!(row.get_str("name").expect("name"), "Ada");
        assert_eq!(row.get_i64(2).expect("score"), 37);
        assert!(rows.next().await.expect("next none").is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_transaction_rolls_back() {
        let mut conn = Connection::connect(ConnectOptions::new().in_memory())
            .await
            .expect("connect");
        conn.execute_batch("create table items(id integer primary key, name text not null);")
            .await
            .expect("create");

        {
            let mut tx = conn.begin().await.expect("begin");
            tx.connection()
                .execute("insert into items(name) values('one')")
                .await
                .expect("insert");
            tx.rollback().await.expect("rollback");
        }

        let mut rows = conn
            .query("select count(*) from items")
            .await
            .expect("count");
        let row = rows.next().await.expect("next").expect("row");
        assert_eq!(row.get_i64(0).expect("count"), 0);
    }
}
