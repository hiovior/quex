use std::cell::Cell as ThreadCell;
use std::ffi::{CStr, CString};
use std::fmt::Display;
use std::marker::PhantomData;
use std::mem;
use std::os::fd::{AsRawFd, RawFd};
use std::ptr::{self, NonNull};
use std::slice;
use std::str;

use quex_mariadb_sys as ffi;

use super::error::{Error, Result};
use super::rows::{Cell, Column, StatementCell};
use super::value::{
    DateTimeTzValue, DateTimeValue, DateValue, ParamSource, TimeValue, Value, ValueRef,
};

pub(crate) const MYSQL_TYPE_FLOAT: u32 = 4;
pub(crate) const MYSQL_TYPE_DOUBLE: u32 = 5;
pub(crate) const MYSQL_TYPE_TIMESTAMP: u32 = 7;
pub(crate) const MYSQL_TYPE_LONGLONG: u32 = 8;
pub(crate) const MYSQL_TYPE_DATE: u32 = 10;
pub(crate) const MYSQL_TYPE_TIME: u32 = 11;
pub(crate) const MYSQL_TYPE_DATETIME: u32 = 12;
pub(crate) const MYSQL_TYPE_TIMESTAMP2: u32 = 17;
pub(crate) const MYSQL_TYPE_DATETIME2: u32 = 18;
pub(crate) const MYSQL_TYPE_TIME2: u32 = 19;
pub(crate) const NOT_NULL_FLAG: u32 = 1;
pub(crate) const WAIT_READ: i32 = ffi::MYSQL_WAIT_READ as i32;
pub(crate) const WAIT_WRITE: i32 = ffi::MYSQL_WAIT_WRITE as i32;
pub(crate) const WAIT_EXCEPT: i32 = ffi::MYSQL_WAIT_EXCEPT as i32;
pub(crate) const WAIT_TIMEOUT: i32 = ffi::MYSQL_WAIT_TIMEOUT as i32;

thread_local! {
    static MYSQL_THREAD_GUARD: MysqlThreadGuard = MysqlThreadGuard::new();
}
struct MysqlThreadGuard {
    initialized: ThreadCell<bool>,
}

impl MysqlThreadGuard {
    fn new() -> Self {
        // SAFETY: This is the required per-thread libmariadb initializer.
        let initialized = unsafe { ffi::mysql_thread_init() == 0 };
        Self {
            initialized: ThreadCell::new(initialized),
        }
    }
}

impl Drop for MysqlThreadGuard {
    fn drop(&mut self) {
        if self.initialized.get() {
            // SAFETY: This thread successfully initialized libmariadb and ends it once.
            unsafe {
                ffi::mysql_thread_end();
            }
        }
    }
}

#[inline]
pub(crate) fn ensure_mysql_thread_ready() -> Result<()> {
    let initialized = MYSQL_THREAD_GUARD
        .try_with(|guard| guard.initialized.get())
        .map_err(|_| Error::new("failed to access MariaDB thread-local state"))?;

    if initialized {
        Ok(())
    } else {
        Err(Error::new("mysql_thread_init failed"))
    }
}

pub(crate) unsafe fn start_operation<T>(op: DriveOperation<'_>, out: &mut T) -> i32 {
    match op {
        DriveOperation::Connect {
            mysql,
            host,
            user,
            password,
            database,
            port,
            unix_socket,
        } => {
            // SAFETY: The MYSQL handle and C strings are valid for the call, and out has the right type.
            unsafe {
                ffi::mysql_real_connect_start(
                    (out as *mut T).cast::<*mut ffi::MYSQL>(),
                    mysql.as_ptr(),
                    opt_ptr(host),
                    opt_ptr(user),
                    opt_ptr(password),
                    opt_ptr(database),
                    port,
                    opt_ptr(unix_socket),
                    0,
                )
            }
        }
        DriveOperation::Query { mysql, sql } => {
            // SAFETY: The MYSQL handle and SQL string are valid, and out stores the return code.
            unsafe {
                ffi::mysql_real_query_start(
                    (out as *mut T).cast::<i32>(),
                    mysql.as_ptr(),
                    sql.as_ptr(),
                    sql.as_bytes().len() as u64,
                )
            }
        }
        DriveOperation::StoreResult { mysql } => {
            // SAFETY: The MYSQL handle is valid after a query, and out stores the result pointer.
            unsafe {
                ffi::mysql_store_result_start(
                    (out as *mut T).cast::<*mut ffi::MYSQL_RES>(),
                    mysql.as_ptr(),
                )
            }
        }
        DriveOperation::StmtPrepare { stmt, sql } => {
            // SAFETY: The statement and SQL string are valid, and out stores the return code.
            unsafe {
                ffi::mysql_stmt_prepare_start(
                    (out as *mut T).cast::<i32>(),
                    stmt.as_ptr(),
                    sql.as_ptr(),
                    sql.as_bytes().len() as u64,
                )
            }
        }
        DriveOperation::StmtExecute { stmt } => {
            // SAFETY: The statement is valid, and out stores the return code.
            unsafe { ffi::mysql_stmt_execute_start((out as *mut T).cast::<i32>(), stmt.as_ptr()) }
        }
        DriveOperation::StmtStoreResult { stmt } => {
            // SAFETY: The statement is valid after execution, and out stores the return code.
            unsafe {
                ffi::mysql_stmt_store_result_start((out as *mut T).cast::<i32>(), stmt.as_ptr())
            }
        }
        DriveOperation::Commit { mysql } => {
            // SAFETY: The MYSQL handle is valid, and out stores the return code.
            unsafe {
                ffi::mysql_commit_start((out as *mut T).cast::<ffi::my_bool>(), mysql.as_ptr())
            }
        }
        DriveOperation::Rollback { mysql } => {
            // SAFETY: The MYSQL handle is valid, and out stores the return code.
            unsafe {
                ffi::mysql_rollback_start((out as *mut T).cast::<ffi::my_bool>(), mysql.as_ptr())
            }
        }
    }
}

pub(crate) unsafe fn continue_operation<T>(op: DriveOperation<'_>, out: &mut T, ready: i32) -> i32 {
    match op {
        DriveOperation::Connect { mysql, .. } => {
            // SAFETY: This continues the same connect operation with valid handles and output storage.
            unsafe {
                ffi::mysql_real_connect_cont(
                    (out as *mut T).cast::<*mut ffi::MYSQL>(),
                    mysql.as_ptr(),
                    ready,
                )
            }
        }
        DriveOperation::Query { mysql, .. } => {
            // SAFETY: This continues the same query operation with valid handle and output storage.
            unsafe {
                ffi::mysql_real_query_cont((out as *mut T).cast::<i32>(), mysql.as_ptr(), ready)
            }
        }
        DriveOperation::StoreResult { mysql } => {
            // SAFETY: This continues the same store-result operation with valid output storage.
            unsafe {
                ffi::mysql_store_result_cont(
                    (out as *mut T).cast::<*mut ffi::MYSQL_RES>(),
                    mysql.as_ptr(),
                    ready,
                )
            }
        }
        DriveOperation::StmtPrepare { stmt, .. } => {
            // SAFETY: This continues the same statement prepare operation.
            unsafe {
                ffi::mysql_stmt_prepare_cont((out as *mut T).cast::<i32>(), stmt.as_ptr(), ready)
            }
        }
        DriveOperation::StmtExecute { stmt } => {
            // SAFETY: This continues the same statement execute operation.
            unsafe {
                ffi::mysql_stmt_execute_cont((out as *mut T).cast::<i32>(), stmt.as_ptr(), ready)
            }
        }
        DriveOperation::StmtStoreResult { stmt } => {
            // SAFETY: This continues the same statement store-result operation.
            unsafe {
                ffi::mysql_stmt_store_result_cont(
                    (out as *mut T).cast::<i32>(),
                    stmt.as_ptr(),
                    ready,
                )
            }
        }
        DriveOperation::Commit { mysql } => {
            // SAFETY: This continues the same commit operation.
            unsafe {
                ffi::mysql_commit_cont(
                    (out as *mut T).cast::<ffi::my_bool>(),
                    mysql.as_ptr(),
                    ready,
                )
            }
        }
        DriveOperation::Rollback { mysql } => {
            // SAFETY: This continues the same rollback operation.
            unsafe {
                ffi::mysql_rollback_cont(
                    (out as *mut T).cast::<ffi::my_bool>(),
                    mysql.as_ptr(),
                    ready,
                )
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum DriveOperation<'a> {
    Connect {
        mysql: MysqlHandle,
        host: &'a Option<CString>,
        user: &'a Option<CString>,
        password: &'a Option<CString>,
        database: &'a Option<CString>,
        port: u32,
        unix_socket: &'a Option<CString>,
    },
    Query {
        mysql: MysqlHandle,
        sql: &'a CString,
    },
    StoreResult {
        mysql: MysqlHandle,
    },
    StmtPrepare {
        stmt: StmtHandle,
        sql: &'a CString,
    },
    StmtExecute {
        stmt: StmtHandle,
    },
    StmtStoreResult {
        stmt: StmtHandle,
    },
    Commit {
        mysql: MysqlHandle,
    },
    Rollback {
        mysql: MysqlHandle,
    },
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct MysqlOut {
    pub(crate) _ptr: *mut ffi::MYSQL,
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct MysqlResOut {
    pub(crate) ptr: *mut ffi::MYSQL_RES,
}
#[derive(Clone, Copy)]
pub(crate) struct SocketRef(pub(crate) RawFd);

impl AsRawFd for SocketRef {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

pub(crate) struct ParamBindings<'a> {
    pub(crate) binds: Vec<ffi::MYSQL_BIND>,
    _scalars: Vec<ScalarSlot>,
    _times: Vec<ffi::MYSQL_TIME>,
    _owned: Vec<Vec<u8>>,
    _lengths: Vec<u64>,
    _nulls: Vec<ffi::my_bool>,
    _marker: PhantomData<&'a [Value]>,
}

pub(crate) struct ParamScratch {
    pub(crate) binds: Vec<ffi::MYSQL_BIND>,
    scalars: Vec<ScalarSlot>,
    times: Vec<ffi::MYSQL_TIME>,
    owned: Vec<Vec<u8>>,
    lengths: Vec<u64>,
    nulls: Vec<ffi::my_bool>,
}

#[derive(Clone, Copy)]
pub(crate) struct MysqlHandle(pub(crate) NonNull<ffi::MYSQL>);

impl MysqlHandle {
    #[inline]
    pub(crate) fn as_ptr(self) -> *mut ffi::MYSQL {
        self.0.as_ptr()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct StmtHandle(pub(crate) NonNull<ffi::MYSQL_STMT>);

impl StmtHandle {
    #[inline]
    pub(crate) fn as_ptr(self) -> *mut ffi::MYSQL_STMT {
        self.0.as_ptr()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ResultHandle(pub(crate) NonNull<ffi::MYSQL_RES>);

impl ResultHandle {
    #[inline]
    pub(crate) fn as_ptr(self) -> *mut ffi::MYSQL_RES {
        self.0.as_ptr()
    }
}

#[derive(Clone, Copy, Default)]
struct ScalarSlot {
    i64_value: i64,
    u64_value: u64,
    f64_value: f64,
}

// SAFETY: `MYSQL*` handles are only ever accessed through the owning `Connection`/statement state,
// and all public driver operations require `&mut self`, so moving the opaque pointer between threads
// does not create aliasing or concurrent access on its own.
unsafe impl Send for MysqlHandle {}
// SAFETY: `MYSQL_STMT*` handles stay owned by one statement state object at a time and are only
// used through unique borrows of that owner.
unsafe impl Send for StmtHandle {}
// SAFETY: `MYSQL_RES*` handles move with their owning `ResultSet` and are never accessed after drop.
unsafe impl Send for ResultHandle {}
// SAFETY: These wrappers contain only operation-local output storage passed to libmariadb during a
// single nonblocking driver call.
unsafe impl Send for MysqlOut {}
// SAFETY: These wrappers contain only operation-local output storage passed to libmariadb during a
// single nonblocking driver call.
unsafe impl Send for MysqlResOut {}
// SAFETY: `DriveOperation` only packages opaque handles and borrowed C strings that remain valid
// for the duration of `Connection::drive`; it is never persisted beyond that await-driven loop.
unsafe impl Send for DriveOperation<'_> {}
// SAFETY: `ParamScratch` owns all buffers referenced by its bind array and is only used through
// unique borrows while binding a cached statement execution.
unsafe impl Send for ParamScratch {}
// SAFETY: `ParamBindings` borrows caller-owned parameters plus its own backing storage and is used
// synchronously for exactly one statement execution.
unsafe impl Send for ParamBindings<'_> {}
impl<'a> ParamBindings<'a> {
    pub(crate) fn new<P>(params: &'a P) -> Self
    where
        P: ParamSource + ?Sized,
    {
        let len = params.len();
        let mut binds = (0..len).map(|_| mysql_bind_init()).collect::<Vec<_>>();
        let mut scalars = vec![ScalarSlot::default(); len];
        let mut times = vec![mysql_time_init(); len];
        let mut owned = Vec::with_capacity(len);
        let mut lengths = vec![0; len];
        let mut nulls = vec![0 as ffi::my_bool; len];

        for index in 0..len {
            let value = params.value_at(index);
            let bind = &mut binds[index];
            bind.is_null = &mut nulls[index];

            match value {
                ValueRef::Null => {
                    nulls[index] = 1;
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_NULL;
                }
                ValueRef::I64(v) => {
                    scalars[index].i64_value = *v;
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_LONGLONG;
                    bind.buffer = (&mut scalars[index].i64_value as *mut i64).cast();
                }
                ValueRef::U64(v) => {
                    scalars[index].u64_value = *v;
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_LONGLONG;
                    bind.is_unsigned = 1;
                    bind.buffer = (&mut scalars[index].u64_value as *mut u64).cast();
                }
                ValueRef::F64(v) => {
                    scalars[index].f64_value = *v;
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_DOUBLE;
                    bind.buffer = (&mut scalars[index].f64_value as *mut f64).cast();
                }
                ValueRef::Date(value) => {
                    times[index] = mysql_date(value);
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_DATE;
                    bind.buffer = (&mut times[index] as *mut ffi::MYSQL_TIME).cast();
                    bind.buffer_length = mem::size_of::<ffi::MYSQL_TIME>() as u64;
                }
                ValueRef::Time(value) => {
                    times[index] = mysql_time(value);
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_TIME;
                    bind.buffer = (&mut times[index] as *mut ffi::MYSQL_TIME).cast();
                    bind.buffer_length = mem::size_of::<ffi::MYSQL_TIME>() as u64;
                }
                ValueRef::DateTime(value) => {
                    times[index] = mysql_datetime(
                        value,
                        ffi::enum_mysql_timestamp_type_MYSQL_TIMESTAMP_DATETIME,
                    );
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_DATETIME;
                    bind.buffer = (&mut times[index] as *mut ffi::MYSQL_TIME).cast();
                    bind.buffer_length = mem::size_of::<ffi::MYSQL_TIME>() as u64;
                }
                ValueRef::DateTimeTz(value) => {
                    let encoded = format_datetime_tz_text(value);
                    lengths[index] = encoded.len() as u64;
                    owned.push(encoded);
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_STRING;
                    bind.buffer = owned.last().unwrap().as_ptr() as *mut _;
                    bind.buffer_length = lengths[index];
                    bind.length = &mut lengths[index];
                }
                ValueRef::Uuid(bytes) => {
                    let encoded = format_uuid_text(*bytes);
                    lengths[index] = encoded.len() as u64;
                    owned.push(encoded);
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_STRING;
                    bind.buffer = owned.last().unwrap().as_ptr() as *mut _;
                    bind.buffer_length = lengths[index];
                    bind.length = &mut lengths[index];
                }
                ValueRef::Bytes(bytes) => {
                    lengths[index] = bytes.len() as u64;
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_BLOB;
                    bind.buffer = bytes.as_ptr() as *mut _;
                    bind.buffer_length = lengths[index];
                    bind.length = &mut lengths[index];
                }
                ValueRef::String(text) => {
                    lengths[index] = text.len() as u64;
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_STRING;
                    bind.buffer = text.as_ptr() as *mut _;
                    bind.buffer_length = lengths[index];
                    bind.length = &mut lengths[index];
                }
            }
        }

        Self {
            binds,
            _scalars: scalars,
            _times: times,
            _owned: owned,
            _lengths: lengths,
            _nulls: nulls,
            _marker: PhantomData,
        }
    }
}

impl ParamScratch {
    pub(crate) fn new(param_count: usize) -> Self {
        let binds = (0..param_count).map(|_| mysql_bind_init()).collect();
        Self {
            binds,
            scalars: vec![ScalarSlot::default(); param_count],
            times: vec![mysql_time_init(); param_count],
            owned: Vec::with_capacity(param_count),
            lengths: vec![0; param_count],
            nulls: vec![0 as ffi::my_bool; param_count],
        }
    }

    pub(crate) fn bind_source<P>(&mut self, params: &P) -> Result<()>
    where
        P: ParamSource + ?Sized,
    {
        if self.binds.len() != params.len() {
            return Err(Error::new(format!(
                "statement expects {} parameters but got {}",
                self.binds.len(),
                params.len()
            )));
        }

        self.owned.clear();
        self.owned.reserve(params.len());

        for index in 0..params.len() {
            let value = params.value_at(index);
            let bind = &mut self.binds[index];
            *bind = mysql_bind_init();
            self.nulls[index] = 0;
            self.lengths[index] = 0;
            bind.is_null = &mut self.nulls[index];

            match value {
                ValueRef::Null => {
                    self.nulls[index] = 1;
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_NULL;
                }
                ValueRef::I64(v) => {
                    self.scalars[index].i64_value = *v;
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_LONGLONG;
                    bind.buffer = (&mut self.scalars[index].i64_value as *mut i64).cast();
                }
                ValueRef::U64(v) => {
                    self.scalars[index].u64_value = *v;
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_LONGLONG;
                    bind.is_unsigned = 1;
                    bind.buffer = (&mut self.scalars[index].u64_value as *mut u64).cast();
                }
                ValueRef::F64(v) => {
                    self.scalars[index].f64_value = *v;
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_DOUBLE;
                    bind.buffer = (&mut self.scalars[index].f64_value as *mut f64).cast();
                }
                ValueRef::Date(value) => {
                    self.times[index] = mysql_date(value);
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_DATE;
                    bind.buffer = (&mut self.times[index] as *mut ffi::MYSQL_TIME).cast();
                    bind.buffer_length = mem::size_of::<ffi::MYSQL_TIME>() as u64;
                }
                ValueRef::Time(value) => {
                    self.times[index] = mysql_time(value);
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_TIME;
                    bind.buffer = (&mut self.times[index] as *mut ffi::MYSQL_TIME).cast();
                    bind.buffer_length = mem::size_of::<ffi::MYSQL_TIME>() as u64;
                }
                ValueRef::DateTime(value) => {
                    self.times[index] = mysql_datetime(
                        value,
                        ffi::enum_mysql_timestamp_type_MYSQL_TIMESTAMP_DATETIME,
                    );
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_DATETIME;
                    bind.buffer = (&mut self.times[index] as *mut ffi::MYSQL_TIME).cast();
                    bind.buffer_length = mem::size_of::<ffi::MYSQL_TIME>() as u64;
                }
                ValueRef::DateTimeTz(value) => {
                    let encoded = format_datetime_tz_text(value);
                    self.lengths[index] = encoded.len() as u64;
                    self.owned.push(encoded);
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_STRING;
                    bind.buffer = self.owned.last().unwrap().as_ptr() as *mut _;
                    bind.buffer_length = self.lengths[index];
                    bind.length = &mut self.lengths[index];
                }
                ValueRef::Uuid(bytes) => {
                    let encoded = format_uuid_text(*bytes);
                    self.lengths[index] = encoded.len() as u64;
                    self.owned.push(encoded);
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_STRING;
                    bind.buffer = self.owned.last().unwrap().as_ptr() as *mut _;
                    bind.buffer_length = self.lengths[index];
                    bind.length = &mut self.lengths[index];
                }
                ValueRef::Bytes(bytes) => {
                    self.lengths[index] = bytes.len() as u64;
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_BLOB;
                    bind.buffer = bytes.as_ptr() as *mut _;
                    bind.buffer_length = self.lengths[index];
                    bind.length = &mut self.lengths[index];
                }
                ValueRef::String(text) => {
                    self.lengths[index] = text.len() as u64;
                    bind.buffer_type = ffi::enum_field_types_MYSQL_TYPE_STRING;
                    bind.buffer = text.as_ptr() as *mut _;
                    bind.buffer_length = self.lengths[index];
                    bind.length = &mut self.lengths[index];
                }
            }
        }

        Ok(())
    }
}

fn format_uuid_text(bytes: [u8; 16]) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut out = Vec::with_capacity(36);
    for (index, byte) in bytes.into_iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            out.push(b'-');
        }
        out.push(HEX[(byte >> 4) as usize]);
        out.push(HEX[(byte & 0x0f) as usize]);
    }
    out
}

fn format_date_text(value: DateValue) -> Vec<u8> {
    format!("{:04}-{:02}-{:02}", value.year, value.month, value.day).into_bytes()
}

fn format_time_text(value: TimeValue) -> Vec<u8> {
    if value.microsecond == 0 {
        format!("{:02}:{:02}:{:02}", value.hour, value.minute, value.second).into_bytes()
    } else {
        let mut micros = format!("{:06}", value.microsecond);
        while micros.ends_with('0') {
            micros.pop();
        }
        format!(
            "{:02}:{:02}:{:02}.{}",
            value.hour, value.minute, value.second, micros
        )
        .into_bytes()
    }
}

fn format_datetime_tz_text(value: DateTimeTzValue) -> Vec<u8> {
    let date = format_date_text(value.datetime.date);
    let time = format_time_text(value.datetime.time);
    let offset = format_offset(value.offset_seconds);
    let mut out = Vec::with_capacity(date.len() + 1 + time.len() + offset.len());
    out.extend_from_slice(&date);
    out.push(b'T');
    out.extend_from_slice(&time);
    out.extend_from_slice(offset.as_bytes());
    out
}

fn format_offset(offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let offset_seconds = offset_seconds.abs();
    let hours = offset_seconds / 3600;
    let minutes = (offset_seconds % 3600) / 60;
    format!("{sign}{hours:02}:{minutes:02}")
}

fn mysql_time_init() -> ffi::MYSQL_TIME {
    ffi::MYSQL_TIME {
        year: 0,
        month: 0,
        day: 0,
        hour: 0,
        minute: 0,
        second: 0,
        second_part: 0,
        neg: 0,
        time_type: ffi::enum_mysql_timestamp_type_MYSQL_TIMESTAMP_NONE,
    }
}

fn mysql_date(value: DateValue) -> ffi::MYSQL_TIME {
    ffi::MYSQL_TIME {
        year: value.year as u32,
        month: value.month as u32,
        day: value.day as u32,
        hour: 0,
        minute: 0,
        second: 0,
        second_part: 0,
        neg: 0,
        time_type: ffi::enum_mysql_timestamp_type_MYSQL_TIMESTAMP_DATE,
    }
}

fn mysql_time(value: TimeValue) -> ffi::MYSQL_TIME {
    ffi::MYSQL_TIME {
        year: 0,
        month: 0,
        day: 0,
        hour: value.hour as u32,
        minute: value.minute as u32,
        second: value.second as u32,
        second_part: value.microsecond as u64,
        neg: 0,
        time_type: ffi::enum_mysql_timestamp_type_MYSQL_TIMESTAMP_TIME,
    }
}

fn mysql_datetime(
    value: DateTimeValue,
    time_type: ffi::enum_mysql_timestamp_type,
) -> ffi::MYSQL_TIME {
    ffi::MYSQL_TIME {
        year: value.date.year as u32,
        month: value.date.month as u32,
        day: value.date.day as u32,
        hour: value.time.hour as u32,
        minute: value.time.minute as u32,
        second: value.time.second as u32,
        second_part: value.time.microsecond as u64,
        neg: 0,
        time_type,
    }
}

#[inline]
pub(crate) fn mysql_bind_init() -> ffi::MYSQL_BIND {
    // SAFETY: In the generated bindgen layout, `MYSQL_BIND` consists only of raw pointers, plain
    // integer aliases, and an all-pointer union. An all-zero bit pattern is therefore a valid Rust
    // value for the type, and libmariadb's C API expects callers to start from a zeroed bind and
    // then fill the fields relevant to the chosen `buffer_type`.
    unsafe { mem::MaybeUninit::<ffi::MYSQL_BIND>::zeroed().assume_init() }
}

#[inline]
pub(crate) fn decode_value(column: &Column, bytes: &[u8]) -> Result<Value> {
    match column.column_type {
        MYSQL_TYPE_LONGLONG => {
            if bytes.first() == Some(&b'-') {
                Ok(Value::I64(parse_i64_ascii(bytes)?))
            } else {
                Ok(Value::U64(parse_u64_ascii(bytes)?))
            }
        }
        MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => {
            let text = str::from_utf8(bytes).map_err(|err| Error::new(err.to_string()))?;
            Ok(Value::F64(parse_text::<f64>(text, "f64")?))
        }
        MYSQL_TYPE_DATE => parse_mysql_date_text(bytes).map(Value::Date),
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => parse_mysql_time_text(bytes).map(Value::Time),
        MYSQL_TYPE_DATETIME
        | MYSQL_TYPE_DATETIME2
        | MYSQL_TYPE_TIMESTAMP
        | MYSQL_TYPE_TIMESTAMP2 => parse_mysql_datetime_text(bytes).map(Value::DateTime),
        _ => match str::from_utf8(bytes) {
            Ok(text) => Ok(Value::String(text.to_owned())),
            Err(_) => Ok(Value::Bytes(bytes.to_vec())),
        },
    }
}

#[inline]
pub(crate) fn decode_statement_value(column: &Column, cell: &StatementCell) -> Result<Value> {
    if cell.is_null != 0 {
        return Ok(Value::Null);
    }

    match column.column_type {
        1 | 2 | 3 | 8 | 9 => {
            if column_is_unsigned(column.flags) != 0 {
                statement_u64(cell, column).map(Value::U64)
            } else {
                statement_i64(cell, column).map(Value::I64)
            }
        }
        MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => statement_f64(cell, column).map(Value::F64),
        MYSQL_TYPE_DATE => parse_statement_mysql_time(cell)
            .map(mysql_time_to_date)
            .map(Value::Date),
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => parse_statement_mysql_time(cell)
            .map(mysql_time_to_time)
            .map(Value::Time),
        MYSQL_TYPE_DATETIME
        | MYSQL_TYPE_DATETIME2
        | MYSQL_TYPE_TIMESTAMP
        | MYSQL_TYPE_TIMESTAMP2 => parse_statement_mysql_time(cell)
            .map(mysql_time_to_datetime)
            .map(Value::DateTime),
        _ => match str::from_utf8(statement_bytes(cell)) {
            Ok(text) => Ok(Value::String(text.to_owned())),
            Err(_) => Ok(Value::Bytes(statement_bytes(cell).to_vec())),
        },
    }
}

#[inline]
pub(crate) fn parse_number<T>(bytes: &[u8], name: &'static str) -> Result<T>
where
    T: str::FromStr,
    T::Err: Display,
{
    let text = str::from_utf8(bytes).map_err(|err| Error::new(err.to_string()))?;
    parse_text(text, name)
}

#[inline]
pub(crate) fn parse_i64_ascii(bytes: &[u8]) -> Result<i64> {
    let bytes = non_null_bytes(bytes)?;
    let (negative, digits) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };
    let value = parse_u64_digits(digits, "i64")?;
    if negative {
        if value == (i64::MAX as u64) + 1 {
            Ok(i64::MIN)
        } else {
            let signed = i64::try_from(value)
                .map_err(|_| Error::new("failed to parse i64: out of range"))?;
            Ok(-signed)
        }
    } else {
        i64::try_from(value).map_err(|_| Error::new("failed to parse i64: out of range"))
    }
}

#[inline]
pub(crate) fn parse_u64_ascii(bytes: &[u8]) -> Result<u64> {
    parse_u64_digits(non_null_bytes(bytes)?, "u64")
}

#[inline]
fn parse_u64_digits(bytes: &[u8], name: &'static str) -> Result<u64> {
    let mut value: u64 = 0;
    for &byte in bytes {
        if !byte.is_ascii_digit() {
            return Err(Error::new(format!(
                "failed to parse {}: invalid digit",
                name
            )));
        }
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add((byte - b'0') as u64))
            .ok_or_else(|| Error::new(format!("failed to parse {}: out of range", name)))?;
    }
    Ok(value)
}

#[inline]
fn non_null_bytes(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.is_empty() {
        Err(Error::new("failed to parse numeric value: empty input"))
    } else {
        Ok(bytes)
    }
}

#[inline]
pub(crate) fn cell_bytes(cell: &Cell) -> Result<&[u8]> {
    if cell.is_null {
        Err(Error::new("column is null"))
    } else {
        // SAFETY: Cell points into the live MYSQL_RES for the current row.
        Ok(unsafe { mysql_cell_bytes(cell.ptr, cell.len) })
    }
}

#[inline]
pub(crate) unsafe fn mysql_cell_bytes<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if len == 0 {
        &[]
    } else {
        debug_assert!(!ptr.is_null());
        // SAFETY: callers guarantee `ptr` points into a live MYSQL result buffer for `len` bytes.
        unsafe { slice::from_raw_parts(ptr, len) }
    }
}

#[inline]
pub(crate) fn statement_i64(cell: &StatementCell, column: &Column) -> Result<i64> {
    if cell.is_null != 0 {
        return Err(Error::new("column is null"));
    }
    match column.column_type {
        1 => Ok(i8::from_ne_bytes([statement_bytes(cell)[0]]) as i64),
        2 => {
            let mut array = [0; 2];
            array.copy_from_slice(&statement_bytes(cell)[..2]);
            Ok(i16::from_ne_bytes(array) as i64)
        }
        3 | 9 => {
            let mut array = [0; 4];
            array.copy_from_slice(&statement_bytes(cell)[..4]);
            Ok(i32::from_ne_bytes(array) as i64)
        }
        MYSQL_TYPE_LONGLONG => {
            let bytes = statement_bytes(cell);
            if bytes.len() == 8 {
                let mut array = [0; 8];
                array.copy_from_slice(bytes);
                Ok(i64::from_ne_bytes(array))
            } else {
                parse_i64_ascii(bytes)
            }
        }
        _ => parse_i64_ascii(statement_bytes(cell)),
    }
}

#[inline]
pub(crate) fn statement_u64(cell: &StatementCell, column: &Column) -> Result<u64> {
    if cell.is_null != 0 {
        return Err(Error::new("column is null"));
    }
    match column.column_type {
        1 => Ok(statement_bytes(cell)[0] as u64),
        2 => {
            let mut array = [0; 2];
            array.copy_from_slice(&statement_bytes(cell)[..2]);
            Ok(u16::from_ne_bytes(array) as u64)
        }
        3 | 9 => {
            let mut array = [0; 4];
            array.copy_from_slice(&statement_bytes(cell)[..4]);
            Ok(u32::from_ne_bytes(array) as u64)
        }
        MYSQL_TYPE_LONGLONG => {
            let bytes = statement_bytes(cell);
            if bytes.len() == 8 {
                let mut array = [0; 8];
                array.copy_from_slice(bytes);
                Ok(u64::from_ne_bytes(array))
            } else {
                parse_u64_ascii(bytes)
            }
        }
        _ => parse_u64_ascii(statement_bytes(cell)),
    }
}

#[inline]
pub(crate) fn statement_f64(cell: &StatementCell, column: &Column) -> Result<f64> {
    if cell.is_null != 0 {
        return Err(Error::new("column is null"));
    }
    let bytes = statement_bytes(cell);
    match column.column_type {
        MYSQL_TYPE_FLOAT => {
            if bytes.len() == 4 {
                let mut array = [0; 4];
                array.copy_from_slice(bytes);
                Ok(f32::from_ne_bytes(array) as f64)
            } else {
                parse_number::<f64>(bytes, "f64")
            }
        }
        MYSQL_TYPE_DOUBLE => {
            if bytes.len() == 8 {
                let mut array = [0; 8];
                array.copy_from_slice(bytes);
                Ok(f64::from_ne_bytes(array))
            } else {
                parse_number::<f64>(bytes, "f64")
            }
        }
        _ => parse_number::<f64>(bytes, "f64"),
    }
}

#[inline]
pub(crate) fn statement_bytes(cell: &StatementCell) -> &[u8] {
    &cell.buffer[..cell.length as usize]
}

pub(crate) fn parse_statement_mysql_time(cell: &StatementCell) -> Result<ffi::MYSQL_TIME> {
    if cell.is_null != 0 {
        return Err(Error::new("column is null"));
    }
    let expected = mem::size_of::<ffi::MYSQL_TIME>();
    let bytes = statement_bytes(cell);
    if bytes.len() != expected {
        return Err(Error::new("invalid MYSQL_TIME buffer length"));
    }
    // SAFETY: `bytes` points to `expected` initialized bytes produced by libmariadb for a
    // `MYSQL_TIME` result buffer. `read_unaligned` is used because the backing `Vec<u8>` does not
    // guarantee `MYSQL_TIME` alignment.
    Ok(unsafe { bytes.as_ptr().cast::<ffi::MYSQL_TIME>().read_unaligned() })
}

#[inline]
fn parse_text<T>(text: &str, name: &'static str) -> Result<T>
where
    T: str::FromStr,
    T::Err: Display,
{
    text.parse::<T>()
        .map_err(|err| Error::new(format!("failed to parse {}: {}", name, err)))
}

pub(crate) fn parse_mysql_date_text(bytes: &[u8]) -> Result<DateValue> {
    let text = str::from_utf8(bytes).map_err(|err| Error::new(err.to_string()))?;
    let mut parts = text.split('-');
    let year = parts
        .next()
        .ok_or_else(|| Error::new("invalid mysql date"))?
        .parse()
        .map_err(|err| Error::new(format!("failed to parse date year: {err}")))?;
    let month = parts
        .next()
        .ok_or_else(|| Error::new("invalid mysql date"))?
        .parse()
        .map_err(|err| Error::new(format!("failed to parse date month: {err}")))?;
    let day = parts
        .next()
        .ok_or_else(|| Error::new("invalid mysql date"))?
        .parse()
        .map_err(|err| Error::new(format!("failed to parse date day: {err}")))?;
    Ok(DateValue { year, month, day })
}

pub(crate) fn parse_mysql_time_text(bytes: &[u8]) -> Result<TimeValue> {
    let text = str::from_utf8(bytes).map_err(|err| Error::new(err.to_string()))?;
    let text = text.strip_prefix('-').unwrap_or(text);
    let (time, microsecond) = match text.split_once('.') {
        Some((time, fraction)) => {
            if fraction.is_empty()
                || fraction.len() > 6
                || !fraction.bytes().all(|b| b.is_ascii_digit())
            {
                return Err(Error::new("invalid mysql time"));
            }
            let mut micros = fraction
                .parse::<u32>()
                .map_err(|err| Error::new(format!("failed to parse time fraction: {err}")))?;
            for _ in fraction.len()..6 {
                micros *= 10;
            }
            (time, micros)
        }
        None => (text, 0),
    };
    let mut parts = time.split(':');
    let hour = parts
        .next()
        .ok_or_else(|| Error::new("invalid mysql time"))?
        .parse()
        .map_err(|err| Error::new(format!("failed to parse time hour: {err}")))?;
    let minute = parts
        .next()
        .ok_or_else(|| Error::new("invalid mysql time"))?
        .parse()
        .map_err(|err| Error::new(format!("failed to parse time minute: {err}")))?;
    let second = parts
        .next()
        .ok_or_else(|| Error::new("invalid mysql time"))?
        .parse()
        .map_err(|err| Error::new(format!("failed to parse time second: {err}")))?;
    Ok(TimeValue {
        hour,
        minute,
        second,
        microsecond,
    })
}

pub(crate) fn parse_mysql_datetime_text(bytes: &[u8]) -> Result<DateTimeValue> {
    let text = str::from_utf8(bytes).map_err(|err| Error::new(err.to_string()))?;
    let (date, time) = text
        .split_once(' ')
        .or_else(|| text.split_once('T'))
        .ok_or_else(|| Error::new("invalid mysql datetime"))?;
    Ok(DateTimeValue {
        date: parse_mysql_date_text(date.as_bytes())?,
        time: parse_mysql_time_text(time.as_bytes())?,
    })
}

pub(crate) fn mysql_time_to_date(value: ffi::MYSQL_TIME) -> DateValue {
    DateValue {
        year: value.year as i32,
        month: value.month as u8,
        day: value.day as u8,
    }
}

pub(crate) fn mysql_time_to_time(value: ffi::MYSQL_TIME) -> TimeValue {
    TimeValue {
        hour: value.hour as u8,
        minute: value.minute as u8,
        second: value.second as u8,
        microsecond: value.second_part as u32,
    }
}

pub(crate) fn mysql_time_to_datetime(value: ffi::MYSQL_TIME) -> DateTimeValue {
    DateTimeValue {
        date: mysql_time_to_date(value),
        time: mysql_time_to_time(value),
    }
}

#[inline]
fn statement_buffer_len(declared_length: u64) -> usize {
    declared_length.clamp(1, 8 * 1024) as usize
}

#[inline]
pub(crate) fn statement_buffer_len_for_column(column: &Column) -> usize {
    match column.column_type {
        1 => 1,
        2 => 2,
        3 | 4 | 9 => 4,
        5 | 8 => 8,
        MYSQL_TYPE_DATE
        | MYSQL_TYPE_TIME
        | MYSQL_TYPE_DATETIME
        | MYSQL_TYPE_TIMESTAMP
        | MYSQL_TYPE_TIME2
        | MYSQL_TYPE_DATETIME2
        | MYSQL_TYPE_TIMESTAMP2 => mem::size_of::<ffi::MYSQL_TIME>(),
        _ => statement_buffer_len(column.declared_length),
    }
}

#[inline]
pub(crate) fn bind_buffer_type(column_type: u32) -> u32 {
    match column_type {
        1 => ffi::enum_field_types_MYSQL_TYPE_TINY,
        2 => ffi::enum_field_types_MYSQL_TYPE_SHORT,
        3 => ffi::enum_field_types_MYSQL_TYPE_LONG,
        4 => ffi::enum_field_types_MYSQL_TYPE_FLOAT,
        5 => ffi::enum_field_types_MYSQL_TYPE_DOUBLE,
        8 => ffi::enum_field_types_MYSQL_TYPE_LONGLONG,
        9 => ffi::enum_field_types_MYSQL_TYPE_LONG,
        _ => column_type,
    }
}

#[inline]
pub(crate) fn column_is_unsigned(flags: u32) -> ffi::my_bool {
    const UNSIGNED_FLAG: u32 = 32;
    ((flags & UNSIGNED_FLAG) != 0) as ffi::my_bool
}

#[inline]
pub(crate) fn enable_stmt_max_length(stmt: *mut ffi::MYSQL_STMT) -> Result<()> {
    let update: ffi::my_bool = 1;
    // SAFETY: stmt is live and the attribute value pointer is valid for this call.
    let rc = unsafe {
        ffi::mysql_stmt_attr_set(
            stmt,
            ffi::enum_stmt_attr_type_STMT_ATTR_UPDATE_MAX_LENGTH,
            (&update as *const ffi::my_bool).cast(),
        )
    };
    if rc != 0 {
        // SAFETY: stmt is live and contains diagnostics for the failed attr-set call.
        Err(unsafe {
            Error::from_stmt(
                stmt,
                "mysql_stmt_attr_set(STMT_ATTR_UPDATE_MAX_LENGTH) failed",
            )
        })
    } else {
        Ok(())
    }
}

#[inline]
pub(crate) fn to_cstring_ptr(value: Option<&str>) -> Result<Option<CString>> {
    value.map(CString::new).transpose().map_err(Into::into)
}

#[inline]
fn opt_ptr(value: &Option<CString>) -> *const i8 {
    value.as_ref().map_or(ptr::null(), |value| value.as_ptr())
}

#[inline]
pub(super) fn c_error_string(ptr: *const i8, fallback: String) -> String {
    if ptr.is_null() {
        return fallback;
    }

    // SAFETY: ptr is non-null and points to a NUL-terminated C error string.
    let text = unsafe { CStr::from_ptr(ptr) }.to_string_lossy();
    if text.is_empty() {
        fallback
    } else {
        text.into_owned()
    }
}

#[inline]
pub(super) fn c_opt_string(ptr: *const i8) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        // SAFETY: non-null caller-provided C string pointers are expected to be NUL-terminated.
        Some(
            unsafe { CStr::from_ptr(ptr) }
                .to_string_lossy()
                .into_owned(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::super::connection::Connection;
    use super::super::options::ConnectOptions;
    use super::super::rows::{Metadata, ResultSet, Row, RowRef, RowRefInner};
    use super::*;

    fn assert_send<T: Send>() {}

    #[test]
    fn c_opt_string_handles_null() {
        assert_eq!(c_opt_string(std::ptr::null()), None);
    }

    #[test]
    fn mysql_cell_bytes_allows_zero_length_null_pointer() {
        let bytes = unsafe { mysql_cell_bytes(std::ptr::null(), 0) };
        assert!(bytes.is_empty());
    }

    #[test]
    fn mysql_bind_init_starts_zeroed() {
        let bind = mysql_bind_init();
        assert!(bind.length.is_null());
        assert!(bind.is_null.is_null());
        assert!(bind.buffer.is_null());
        assert!(bind.error.is_null());
        assert!(bind.store_param_func.is_none());
        assert!(bind.fetch_result.is_none());
        assert!(bind.skip_result.is_none());
        assert_eq!(bind.buffer_length, 0);
        assert_eq!(bind.offset, 0);
        assert_eq!(bind.length_value, 0);
        assert_eq!(bind.flags, 0);
        assert_eq!(bind.pack_length, 0);
        assert_eq!(bind.buffer_type, 0);
        assert_eq!(bind.error_value, 0);
        assert_eq!(bind.is_unsigned, 0);
        assert_eq!(bind.long_data_used, 0);
        assert_eq!(bind.is_null_value, 0);
        assert!(bind.extension.is_null());
    }

    #[test]
    fn connect_options_builder_round_trips() {
        let opts = ConnectOptions::new()
            .host("127.0.0.1")
            .port(3307)
            .user("app")
            .password("secret")
            .database("demo")
            .unix_socket("/tmp/mysql.sock");

        assert_eq!(opts.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(opts.port, 3307);
        assert_eq!(opts.user.as_deref(), Some("app"));
        assert_eq!(opts.password.as_deref(), Some("secret"));
        assert_eq!(opts.database.as_deref(), Some("demo"));
        assert_eq!(opts.unix_socket.as_deref(), Some("/tmp/mysql.sock"));
    }

    #[test]
    fn row_name_lookup_works() {
        let metadata = Metadata {
            columns: vec![
                Column {
                    name: "id".into(),
                    column_type: MYSQL_TYPE_LONGLONG,
                    flags: 0,
                    declared_length: 20,
                    nullable: false,
                },
                Column {
                    name: "name".into(),
                    column_type: 253,
                    flags: 0,
                    declared_length: 255,
                    nullable: false,
                },
            ],
            name_order: vec![0, 1].into_boxed_slice(),
        };
        let row = [
            Cell {
                ptr: b"42".as_ptr(),
                len: 2,
                is_null: false,
            },
            Cell {
                ptr: b"Ada".as_ptr(),
                len: 3,
                is_null: false,
            },
        ];
        let row = RowRef {
            metadata: &metadata,
            row: RowRefInner::Text(&row[..]),
        };

        assert_eq!(row.get_i64("id").unwrap(), 42);
        assert_eq!(row.get_str("name").unwrap(), "Ada");
    }

    #[test]
    fn native_types_are_send() {
        assert_send::<Connection>();
        assert_send::<ResultSet>();
        assert_send::<Row>();
    }
}
