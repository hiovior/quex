use std::ffi::{CStr, CString};
use std::fmt::Display;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::ptr::{self, NonNull};
use std::str;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use quex_pq_sys as ffi;
use tokio::io::unix::AsyncFd;

use super::error::{Error, ExecuteResult, Result};
use super::options::ConnectOptions;
use super::rows::{Column, Metadata, ResultSet};
use super::value::{
    DateTimeTzValue, DateTimeValue, DateValue, ParamSource, TimeValue, Value, ValueRef,
};

const OID_BOOL: u32 = 16;
const OID_BYTEA: u32 = 17;
const OID_INT8: u32 = 20;
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_TEXT: u32 = 25;
const OID_DATE: u32 = 1082;
const OID_TIME: u32 = 1083;
const OID_TIMESTAMP: u32 = 1114;
const OID_TIMESTAMPTZ: u32 = 1184;
const OID_FLOAT4: u32 = 700;
const OID_FLOAT8: u32 = 701;
const OID_VARCHAR: u32 = 1043;
const OID_BPCHAR: u32 = 1042;
const OID_NAME: u32 = 19;
const OID_UUID: u32 = 2950;

pub(crate) const CONNECTION_READY: u8 = 0;
pub(crate) const CONNECTION_POISONED: u8 = 1;

pub(crate) struct ConnectParams {
    _storage: Vec<CString>,
    pub(crate) keywords: Vec<*const i8>,
    pub(crate) values: Vec<*const i8>,
}

impl ConnectParams {
    pub(crate) fn new(options: &ConnectOptions) -> Result<Self> {
        let mut storage = Vec::with_capacity(10);
        let mut keywords = Vec::with_capacity(6);
        let mut values = Vec::with_capacity(6);

        let port = options.port.to_string();
        push_conn_param(&mut storage, &mut keywords, &mut values, "port", &port)?;
        if let Some(host) = options.host.as_deref() {
            push_conn_param(&mut storage, &mut keywords, &mut values, "host", host)?;
        }
        if let Some(user) = options.user.as_deref() {
            push_conn_param(&mut storage, &mut keywords, &mut values, "user", user)?;
        }
        if let Some(password) = options.password.as_deref() {
            push_conn_param(
                &mut storage,
                &mut keywords,
                &mut values,
                "password",
                password,
            )?;
        }
        if let Some(database) = options.database.as_deref() {
            push_conn_param(&mut storage, &mut keywords, &mut values, "dbname", database)?;
        }

        keywords.push(ptr::null());
        values.push(ptr::null());

        Ok(Self {
            _storage: storage,
            keywords,
            values,
        })
    }
}

fn push_conn_param(
    storage: &mut Vec<CString>,
    keywords: &mut Vec<*const i8>,
    values: &mut Vec<*const i8>,
    key: &str,
    value: &str,
) -> Result<()> {
    let key = CString::new(key)?;
    let value = CString::new(value)?;
    let key_ptr = key.as_ptr();
    let value_ptr = value.as_ptr();
    storage.push(key);
    storage.push(value);
    keywords.push(key_ptr);
    values.push(value_ptr);
    Ok(())
}

pub(crate) struct ParamScratch {
    values: Vec<*const i8>,
    lengths: Vec<i32>,
    formats: Vec<i32>,
    owned: Vec<Vec<u8>>,
}

impl ParamScratch {
    pub(crate) fn new() -> Self {
        Self {
            values: Vec::new(),
            lengths: Vec::new(),
            formats: Vec::new(),
            owned: Vec::new(),
        }
    }

    fn bind_source<P>(&mut self, params: &P) -> ParamBindView<'_>
    where
        P: ParamSource + ?Sized,
    {
        self.values.clear();
        self.lengths.clear();
        self.formats.clear();
        self.owned.clear();

        self.values.reserve(params.len());
        self.lengths.reserve(params.len());
        self.owned.reserve(params.len());
        self.formats.reserve(params.len());

        for index in 0..params.len() {
            let value = params.value_at(index);
            match value {
                ValueRef::Null => {
                    self.values.push(ptr::null());
                    self.lengths.push(0);
                    self.formats.push(0);
                }
                ValueRef::String(text) => {
                    self.values.push(text.as_ptr().cast());
                    self.lengths.push(text.len() as i32);
                    self.formats.push(0);
                }
                ValueRef::Bytes(bytes) => {
                    let mut encoded = encode_bytea_hex(bytes);
                    self.lengths.push(encoded.len() as i32);
                    encoded.push(0);
                    self.owned.push(encoded);
                    let ptr = self.owned.last().unwrap().as_ptr().cast();
                    self.values.push(ptr);
                    self.formats.push(0);
                }
                ValueRef::Date(value) => {
                    let encoded = encode_date_binary(value);
                    self.lengths.push(encoded.len() as i32);
                    self.owned.push(encoded);
                    self.values.push(self.owned.last().unwrap().as_ptr().cast());
                    self.formats.push(1);
                }
                ValueRef::Time(value) => {
                    let encoded = encode_time_binary(value);
                    self.lengths.push(encoded.len() as i32);
                    self.owned.push(encoded);
                    self.values.push(self.owned.last().unwrap().as_ptr().cast());
                    self.formats.push(1);
                }
                ValueRef::DateTime(value) => {
                    let encoded = encode_datetime_binary(value);
                    self.lengths.push(encoded.len() as i32);
                    self.owned.push(encoded);
                    self.values.push(self.owned.last().unwrap().as_ptr().cast());
                    self.formats.push(1);
                }
                ValueRef::DateTimeTz(value) => {
                    let encoded = encode_datetime_tz_binary(value);
                    self.lengths.push(encoded.len() as i32);
                    self.owned.push(encoded);
                    self.values.push(self.owned.last().unwrap().as_ptr().cast());
                    self.formats.push(1);
                }
                ValueRef::Uuid(bytes) => {
                    self.lengths.push(16);
                    self.owned.push(bytes.to_vec());
                    self.values.push(self.owned.last().unwrap().as_ptr().cast());
                    self.formats.push(1);
                }
                ValueRef::I64(v) => {
                    let mut encoded = v.to_string().into_bytes();
                    self.lengths.push(encoded.len() as i32);
                    encoded.push(0);
                    self.owned.push(encoded);
                    self.values.push(self.owned.last().unwrap().as_ptr().cast());
                    self.formats.push(0);
                }
                ValueRef::U64(v) => {
                    let mut encoded = v.to_string().into_bytes();
                    self.lengths.push(encoded.len() as i32);
                    encoded.push(0);
                    self.owned.push(encoded);
                    self.values.push(self.owned.last().unwrap().as_ptr().cast());
                    self.formats.push(0);
                }
                ValueRef::F64(v) => {
                    let mut encoded = v.to_string().into_bytes();
                    self.lengths.push(encoded.len() as i32);
                    encoded.push(0);
                    self.owned.push(encoded);
                    self.values.push(self.owned.last().unwrap().as_ptr().cast());
                    self.formats.push(0);
                }
            }
        }

        ParamBindView {
            values: &self.values,
            lengths: &self.lengths,
            formats: &self.formats,
        }
    }
}

struct ParamBindView<'a> {
    values: &'a [*const i8],
    lengths: &'a [i32],
    formats: &'a [i32],
}

fn encode_date_binary(value: DateValue) -> Vec<u8> {
    let days = days_since_pg_epoch(value);
    { days }.to_be_bytes().to_vec()
}

fn encode_time_binary(value: TimeValue) -> Vec<u8> {
    time_micros(value).to_be_bytes().to_vec()
}

fn encode_datetime_binary(value: DateTimeValue) -> Vec<u8> {
    datetime_micros(value).to_be_bytes().to_vec()
}

fn encode_datetime_tz_binary(value: DateTimeTzValue) -> Vec<u8> {
    let micros = datetime_micros(value.datetime) - i64::from(value.offset_seconds) * 1_000_000;
    micros.to_be_bytes().to_vec()
}

fn time_micros(value: TimeValue) -> i64 {
    (((i64::from(value.hour) * 60 + i64::from(value.minute)) * 60) + i64::from(value.second))
        * 1_000_000
        + i64::from(value.microsecond)
}

fn datetime_micros(value: DateTimeValue) -> i64 {
    i64::from(days_since_pg_epoch(value.date)) * 86_400_000_000 + time_micros(value.time)
}

fn days_since_pg_epoch(value: DateValue) -> i32 {
    days_from_civil(value.year, value.month, value.day) - days_from_civil(2000, 1, 1)
}

fn days_from_civil(year: i32, month: u8, day: u8) -> i32 {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i32::from(month);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i32::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[derive(Clone, Copy)]
pub(crate) struct ConnHandle(pub(crate) NonNull<ffi::PGconn>);

impl ConnHandle {
    #[inline]
    pub(crate) fn as_ptr(self) -> *mut ffi::PGconn {
        self.0.as_ptr()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ResultHandle(pub(crate) NonNull<ffi::PGresult>);

impl ResultHandle {
    #[inline]
    pub(crate) fn as_ptr(self) -> *mut ffi::PGresult {
        self.0.as_ptr()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SocketRef(pub(crate) RawFd);

impl AsRawFd for SocketRef {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

// SAFETY: `PGconn*` is only used through the owning `Connection`, whose public operations require
// unique access and whose operation guard prevents overlapping use.
unsafe impl Send for ConnHandle {}
// SAFETY: `PGresult*` moves with its owning `ResultSet` and libpq treats result buffers as owned
// by that handle until `PQclear`.
unsafe impl Send for ResultHandle {}
// SAFETY: `ParamScratch` owns the storage referenced by its pointer arrays, and the whole value
// moves together.
unsafe impl Send for ParamScratch {}

pub(crate) struct ConnectionOpGuard {
    state: Arc<AtomicU8>,
    completed: bool,
}

impl ConnectionOpGuard {
    pub(crate) fn new(state: &Arc<AtomicU8>) -> Self {
        Self {
            state: Arc::clone(state),
            completed: false,
        }
    }

    pub(crate) fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for ConnectionOpGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.state.store(CONNECTION_POISONED, Ordering::Release);
        }
    }
}

pub(crate) fn send_query(conn: ConnHandle, sql: *const i8) -> Result<()> {
    // SAFETY: conn is live and sql points to a nul-terminated query string.
    unsafe {
        if ffi::PQsendQueryParams(
            conn.as_ptr(),
            sql,
            0,
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
            1,
        ) == 0
        {
            return Err(Error::from_conn(conn.as_ptr(), "PQsendQueryParams failed"));
        }
    }

    Ok(())
}

pub(crate) async fn prepare_named_statement(
    conn: ConnHandle,
    socket: &AsyncFd<SocketRef>,
    name: &CString,
    sql: &CString,
) -> Result<()> {
    // SAFETY: conn, name, and sql are live for the duration of the send call.
    unsafe {
        if ffi::PQsendPrepare(conn.as_ptr(), name.as_ptr(), sql.as_ptr(), 0, ptr::null()) == 0 {
            return Err(Error::from_conn(conn.as_ptr(), "PQsendPrepare failed"));
        }
    }
    let result = finish_request(conn, socket).await?;
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
    Ok(())
}

pub(crate) async fn execute_prepared_no_params(
    conn: ConnHandle,
    socket: &AsyncFd<SocketRef>,
    state: &Arc<AtomicU8>,
    name: &CString,
) -> Result<ResultHandle> {
    let mut guard = ConnectionOpGuard::new(state);
    // SAFETY: conn and statement name are live for the duration of the send call.
    unsafe {
        if ffi::PQsendQueryPrepared(
            conn.as_ptr(),
            name.as_ptr(),
            0,
            ptr::null(),
            ptr::null(),
            ptr::null(),
            1,
        ) == 0
        {
            return Err(Error::from_conn(
                conn.as_ptr(),
                "PQsendQueryPrepared failed",
            ));
        }
    }
    let result = finish_request(conn, socket).await?;
    guard.complete();
    Ok(result)
}

pub(crate) async fn execute_prepared<P>(
    conn: ConnHandle,
    socket: &AsyncFd<SocketRef>,
    state: &Arc<AtomicU8>,
    name: &CString,
    params: &mut ParamScratch,
    values: &P,
    result_metadata: &mut Option<Arc<Metadata>>,
) -> Result<ResultSet>
where
    P: ParamSource + ?Sized,
{
    let mut guard = ConnectionOpGuard::new(state);
    let bound = params.bind_source(values);
    // SAFETY: bound parameter arrays point into params and values for the duration of the send call.
    unsafe {
        if ffi::PQsendQueryPrepared(
            conn.as_ptr(),
            name.as_ptr(),
            bound.values.len() as i32,
            bound.values.as_ptr(),
            bound.lengths.as_ptr(),
            bound.formats.as_ptr(),
            1,
        ) == 0
        {
            return Err(Error::from_conn(
                conn.as_ptr(),
                "PQsendQueryPrepared failed",
            ));
        }
    }

    let result = finish_request(conn, socket).await?;
    let metadata = match result_metadata {
        Some(metadata) => Arc::clone(metadata),
        None => {
            let metadata = Arc::new(Metadata::from_result(result));
            *result_metadata = Some(Arc::clone(&metadata));
            metadata
        }
    };
    guard.complete();
    Ok(ResultSet::new(result, metadata))
}

pub(crate) async fn execute_prepared_exec<P>(
    conn: ConnHandle,
    socket: &AsyncFd<SocketRef>,
    state: &Arc<AtomicU8>,
    name: &CString,
    params: &mut ParamScratch,
    values: &P,
) -> Result<ExecuteResult>
where
    P: ParamSource + ?Sized,
{
    let mut guard = ConnectionOpGuard::new(state);
    let bound = params.bind_source(values);
    // SAFETY: bound parameter arrays point into params and values for the duration of the send call.
    unsafe {
        if ffi::PQsendQueryPrepared(
            conn.as_ptr(),
            name.as_ptr(),
            bound.values.len() as i32,
            bound.values.as_ptr(),
            bound.lengths.as_ptr(),
            bound.formats.as_ptr(),
            1,
        ) == 0
        {
            return Err(Error::from_conn(
                conn.as_ptr(),
                "PQsendQueryPrepared failed",
            ));
        }
    }

    let result = finish_request(conn, socket).await?;
    let execute = execute_result_from_handle(result)?;
    // SAFETY: result was returned by libpq and is cleared exactly once here.
    unsafe { ffi::PQclear(result.as_ptr()) };
    guard.complete();
    Ok(execute)
}

pub(crate) fn execute_result_from_handle(result: ResultHandle) -> Result<ExecuteResult> {
    // SAFETY: result is a live PG result handle.
    let fields = unsafe { ffi::PQnfields(result.as_ptr()) };
    if fields != 0 {
        return Err(Error::new("statement returned rows; use query instead"));
    }

    // SAFETY: result is live and the returned command tuple string is owned by the result.
    let rows_affected = unsafe {
        let ptr = ffi::PQcmdTuples(result.as_ptr());
        if ptr.is_null() || *ptr == 0 {
            0
        } else {
            CStr::from_ptr(ptr)
                .to_string_lossy()
                .parse::<u64>()
                .unwrap_or(0)
        }
    };

    Ok(ExecuteResult {
        rows_affected,
        last_insert_id: None,
    })
}

pub(crate) fn should_prepare_query(sql: &str) -> bool {
    let sql = sql.trim_start();
    sql.get(..6)
        .map(|prefix| prefix.eq_ignore_ascii_case("select"))
        .unwrap_or(false)
        || sql
            .get(..4)
            .map(|prefix| prefix.eq_ignore_ascii_case("with"))
            .unwrap_or(false)
        || sql
            .get(..6)
            .map(|prefix| prefix.eq_ignore_ascii_case("values"))
            .unwrap_or(false)
}

pub(crate) async fn finish_request(
    conn: ConnHandle,
    socket: &AsyncFd<SocketRef>,
) -> Result<ResultHandle> {
    loop {
        // SAFETY: conn is live and owned by the in-flight operation.
        match unsafe { ffi::PQflush(conn.as_ptr()) } {
            0 => break,
            1 => wait_for_socket(socket, libc::POLLOUT, false).await?,
            _ => {
                // SAFETY: conn is live and contains diagnostics for the failed flush.
                return Err(unsafe { Error::from_conn(conn.as_ptr(), "PQflush failed") });
            }
        }
    }

    loop {
        // SAFETY: conn is live and polled only by this operation.
        unsafe {
            if ffi::PQconsumeInput(conn.as_ptr()) == 0 {
                return Err(Error::from_conn(conn.as_ptr(), "PQconsumeInput failed"));
            }
            if ffi::PQisBusy(conn.as_ptr()) == 0 {
                break;
            }
        }
        wait_for_socket(socket, libc::POLLIN, true).await?;
    }

    let mut final_result: Option<ResultHandle> = None;
    loop {
        // SAFETY: conn is live and results are drained by this operation.
        let result = unsafe { ffi::PQgetResult(conn.as_ptr()) };
        let Some(result) = NonNull::new(result) else {
            break;
        };
        let handle = ResultHandle(result);
        // SAFETY: handle is a live PG result returned by libpq.
        let status = unsafe { ffi::PQresultStatus(handle.as_ptr()) };
        if status == ffi::ExecStatusType_PGRES_FATAL_ERROR
            || status == ffi::ExecStatusType_PGRES_BAD_RESPONSE
            || status == ffi::ExecStatusType_PGRES_NONFATAL_ERROR
        {
            // SAFETY: handle is live and contains diagnostics for the failed operation.
            let error = unsafe { Error::from_result(handle.as_ptr(), "postgres operation failed") };
            // SAFETY: handle was returned by libpq and is cleared exactly once here.
            unsafe { ffi::PQclear(handle.as_ptr()) };
            if let Some(prev) = final_result.take() {
                // SAFETY: prev was returned by libpq and is cleared exactly once here.
                unsafe { ffi::PQclear(prev.as_ptr()) };
            }
            loop {
                // SAFETY: conn is live and remaining results are being drained.
                let extra = unsafe { ffi::PQgetResult(conn.as_ptr()) };
                let Some(extra) = NonNull::new(extra) else {
                    break;
                };
                // SAFETY: extra was returned by libpq and is cleared exactly once here.
                unsafe { ffi::PQclear(extra.as_ptr()) };
            }
            return Err(error);
        }
        if let Some(prev) = final_result.replace(handle) {
            // SAFETY: prev was returned by libpq and is cleared exactly once here.
            unsafe { ffi::PQclear(prev.as_ptr()) };
        }
    }

    final_result.ok_or_else(|| Error::new("postgres operation returned no result"))
}

pub(crate) async fn wait_for_socket(
    socket: &AsyncFd<SocketRef>,
    events: i16,
    read: bool,
) -> Result<()> {
    loop {
        let mut ready = if read {
            socket.readable().await?
        } else {
            socket.writable().await?
        };

        if socket_has_event(socket.get_ref().as_raw_fd(), events)? {
            return Ok(());
        }

        ready.clear_ready();
    }
}

fn socket_has_event(fd: RawFd, events: i16) -> io::Result<bool> {
    let mut poll_fd = libc::pollfd {
        fd,
        events,
        revents: 0,
    };

    loop {
        // SAFETY: poll_fd points to initialized storage for one pollfd entry.
        let rc = unsafe { libc::poll(&mut poll_fd, 1, 0) };
        if rc >= 0 {
            return Ok(rc > 0 && (poll_fd.revents & events) != 0);
        }

        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

fn encode_bytea_hex(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + bytes.len() * 2);
    out.extend_from_slice(br"\x");
    for &byte in bytes {
        out.push(hex_digit(byte >> 4));
        out.push(hex_digit(byte & 0x0f));
    }
    out
}

#[inline]
fn hex_digit(value: u8) -> u8 {
    match value {
        0..=9 => b'0' + value,
        _ => b'a' + (value - 10),
    }
}

#[inline]
pub(crate) fn decode_value(column: &Column, bytes: &[u8]) -> Result<Value> {
    if column.format == 1 {
        match column.type_oid {
            OID_BOOL => Ok(Value::I64(
                (bytes.first().copied().unwrap_or(0) != 0) as i64,
            )),
            OID_INT2 | OID_INT4 | OID_INT8 => {
                Ok(Value::I64(parse_binary_i64(bytes, column.type_oid)?))
            }
            OID_FLOAT4 | OID_FLOAT8 => Ok(Value::F64(parse_binary_f64(bytes, column.type_oid)?)),
            OID_DATE => Ok(Value::Date(parse_binary_date(bytes)?)),
            OID_TIME => Ok(Value::Time(parse_binary_time(bytes)?)),
            OID_TIMESTAMP => Ok(Value::DateTime(parse_binary_datetime(bytes)?)),
            OID_TIMESTAMPTZ => Ok(Value::DateTimeTz(parse_binary_datetimetz(bytes)?)),
            OID_UUID => Ok(Value::Uuid(parse_binary_uuid(bytes)?)),
            OID_BYTEA => Ok(Value::Bytes(bytes.to_vec())),
            OID_TEXT | OID_VARCHAR | OID_BPCHAR | OID_NAME => match str::from_utf8(bytes) {
                Ok(text) => Ok(Value::String(text.to_owned())),
                Err(_) => Ok(Value::Bytes(bytes.to_vec())),
            },
            _ => Ok(Value::Bytes(bytes.to_vec())),
        }
    } else {
        match column.type_oid {
            OID_INT2 | OID_INT4 | OID_INT8 => Ok(Value::I64(parse_i64_ascii(bytes)?)),
            OID_FLOAT4 | OID_FLOAT8 => Ok(Value::F64(parse_number::<f64>(bytes, "f64")?)),
            OID_DATE => parse_text_date(bytes).map(Value::Date),
            OID_TIME => parse_text_time(bytes).map(Value::Time),
            OID_TIMESTAMP => parse_text_datetime(bytes).map(Value::DateTime),
            OID_TIMESTAMPTZ => parse_text_datetimetz(bytes).map(Value::DateTimeTz),
            OID_UUID => parse_text_uuid(bytes).map(Value::Uuid),
            _ => match str::from_utf8(bytes) {
                Ok(text) => Ok(Value::String(text.to_owned())),
                Err(_) => Ok(Value::Bytes(bytes.to_vec())),
            },
        }
    }
}

#[inline]
pub(crate) fn parse_binary_i64(bytes: &[u8], oid: u32) -> Result<i64> {
    match oid {
        OID_INT2 => {
            let array: [u8; 2] = bytes
                .try_into()
                .map_err(|_| Error::new("invalid int2 length"))?;
            Ok(i16::from_be_bytes(array) as i64)
        }
        OID_INT4 => {
            let array: [u8; 4] = bytes
                .try_into()
                .map_err(|_| Error::new("invalid int4 length"))?;
            Ok(i32::from_be_bytes(array) as i64)
        }
        OID_INT8 => {
            let array: [u8; 8] = bytes
                .try_into()
                .map_err(|_| Error::new("invalid int8 length"))?;
            Ok(i64::from_be_bytes(array))
        }
        _ => parse_i64_ascii(bytes),
    }
}

#[inline]
pub(crate) fn parse_binary_u64(bytes: &[u8], oid: u32) -> Result<u64> {
    match oid {
        OID_INT2 => {
            let array: [u8; 2] = bytes
                .try_into()
                .map_err(|_| Error::new("invalid int2 length"))?;
            Ok(u16::from_be_bytes(array) as u64)
        }
        OID_INT4 => {
            let array: [u8; 4] = bytes
                .try_into()
                .map_err(|_| Error::new("invalid int4 length"))?;
            Ok(u32::from_be_bytes(array) as u64)
        }
        OID_INT8 => {
            let array: [u8; 8] = bytes
                .try_into()
                .map_err(|_| Error::new("invalid int8 length"))?;
            Ok(u64::from_be_bytes(array))
        }
        _ => parse_u64_ascii(bytes),
    }
}

#[inline]
pub(crate) fn parse_binary_f64(bytes: &[u8], oid: u32) -> Result<f64> {
    match oid {
        OID_FLOAT4 => {
            let array: [u8; 4] = bytes
                .try_into()
                .map_err(|_| Error::new("invalid float4 length"))?;
            Ok(f32::from_bits(u32::from_be_bytes(array)) as f64)
        }
        OID_FLOAT8 => {
            let array: [u8; 8] = bytes
                .try_into()
                .map_err(|_| Error::new("invalid float8 length"))?;
            Ok(f64::from_bits(u64::from_be_bytes(array)))
        }
        _ => parse_number::<f64>(bytes, "f64"),
    }
}

#[inline]
pub(crate) fn parse_binary_uuid(bytes: &[u8]) -> Result<[u8; 16]> {
    bytes
        .try_into()
        .map_err(|_| Error::new("invalid uuid length"))
}

#[inline]
pub(crate) fn parse_binary_date(bytes: &[u8]) -> Result<DateValue> {
    let days = i32::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| Error::new("invalid date length"))?,
    );
    let (year, month, day) = civil_from_days(days + days_from_civil(2000, 1, 1));
    Ok(DateValue { year, month, day })
}

#[inline]
pub(crate) fn parse_binary_time(bytes: &[u8]) -> Result<TimeValue> {
    let micros = i64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| Error::new("invalid time length"))?,
    );
    time_from_micros(micros)
}

#[inline]
pub(crate) fn parse_binary_datetime(bytes: &[u8]) -> Result<DateTimeValue> {
    let micros = i64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| Error::new("invalid timestamp length"))?,
    );
    datetime_from_micros(micros)
}

#[inline]
pub(crate) fn parse_binary_datetimetz(bytes: &[u8]) -> Result<DateTimeTzValue> {
    Ok(DateTimeTzValue {
        datetime: parse_binary_datetime(bytes)?,
        offset_seconds: 0,
    })
}

pub(crate) fn parse_text_date(bytes: &[u8]) -> Result<DateValue> {
    parse_date_text(str::from_utf8(bytes).map_err(|_| Error::new("invalid utf-8 date"))?)
        .ok_or_else(|| Error::new("invalid date"))
}

pub(crate) fn parse_text_time(bytes: &[u8]) -> Result<TimeValue> {
    parse_time_text(str::from_utf8(bytes).map_err(|_| Error::new("invalid utf-8 time"))?)
        .ok_or_else(|| Error::new("invalid time"))
}

pub(crate) fn parse_text_datetime(bytes: &[u8]) -> Result<DateTimeValue> {
    parse_datetime_text(str::from_utf8(bytes).map_err(|_| Error::new("invalid utf-8 datetime"))?)
        .ok_or_else(|| Error::new("invalid datetime"))
}

pub(crate) fn parse_text_datetimetz(bytes: &[u8]) -> Result<DateTimeTzValue> {
    parse_datetimetz_text(
        str::from_utf8(bytes).map_err(|_| Error::new("invalid utf-8 datetime with offset"))?,
    )
    .ok_or_else(|| Error::new("invalid datetime with offset"))
}

pub(crate) fn parse_text_uuid(bytes: &[u8]) -> Result<[u8; 16]> {
    parse_uuid_text(str::from_utf8(bytes).map_err(|_| Error::new("invalid utf-8 uuid"))?)
        .ok_or_else(|| Error::new("invalid uuid"))
}

fn parse_date_text(text: &str) -> Option<DateValue> {
    let mut parts = text.split('-');
    Some(DateValue {
        year: parts.next()?.parse().ok()?,
        month: parts.next()?.parse().ok()?,
        day: parts.next()?.parse().ok()?,
    })
}

fn parse_time_text(text: &str) -> Option<TimeValue> {
    let (time, microsecond) = match text.split_once('.') {
        Some((time, fraction)) => {
            if fraction.is_empty()
                || fraction.len() > 6
                || !fraction.bytes().all(|b| b.is_ascii_digit())
            {
                return None;
            }
            let mut micros = fraction.parse::<u32>().ok()?;
            for _ in fraction.len()..6 {
                micros *= 10;
            }
            (time, micros)
        }
        None => (text, 0),
    };
    let mut parts = time.split(':');
    Some(TimeValue {
        hour: parts.next()?.parse().ok()?,
        minute: parts.next()?.parse().ok()?,
        second: parts.next()?.parse().ok()?,
        microsecond,
    })
}

fn parse_datetime_text(text: &str) -> Option<DateTimeValue> {
    let (date, time) = text.split_once(' ').or_else(|| text.split_once('T'))?;
    Some(DateTimeValue {
        date: parse_date_text(date)?,
        time: parse_time_text(time)?,
    })
}

fn parse_datetimetz_text(text: &str) -> Option<DateTimeTzValue> {
    let (datetime, offset_seconds) = parse_offset_datetime_parts(text)?;
    Some(DateTimeTzValue {
        datetime: parse_datetime_text(datetime)?,
        offset_seconds,
    })
}

fn parse_offset_datetime_parts(text: &str) -> Option<(&str, i32)> {
    if let Some(datetime) = text.strip_suffix('Z') {
        return Some((datetime, 0));
    }

    let split = text
        .char_indices()
        .rev()
        .find(|(_, ch)| matches!(ch, '+' | '-'))?
        .0;
    let (datetime, offset) = text.split_at(split);
    let sign = if offset.starts_with('-') { -1 } else { 1 };
    let offset = &offset[1..];
    let (hours, minutes) = offset.split_once(':')?;
    let total = hours.parse::<i32>().ok()? * 3600 + minutes.parse::<i32>().ok()? * 60;
    Some((datetime, sign * total))
}

fn parse_uuid_text(text: &str) -> Option<[u8; 16]> {
    let mut out = [0u8; 16];
    let mut chars = text.bytes().filter(|b| *b != b'-');
    for slot in &mut out {
        let high = hex_value(chars.next()?)?;
        let low = hex_value(chars.next()?)?;
        *slot = (high << 4) | low;
    }
    if chars.next().is_some() {
        return None;
    }
    Some(out)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn time_from_micros(micros: i64) -> Result<TimeValue> {
    if micros < 0 {
        return Err(Error::new("negative postgres time is unsupported"));
    }
    let total_seconds = micros / 1_000_000;
    let microsecond = (micros % 1_000_000) as u32;
    let hour = (total_seconds / 3600) as u8;
    let minute = ((total_seconds % 3600) / 60) as u8;
    let second = (total_seconds % 60) as u8;
    Ok(TimeValue {
        hour,
        minute,
        second,
        microsecond,
    })
}

fn datetime_from_micros(micros: i64) -> Result<DateTimeValue> {
    let days = micros.div_euclid(86_400_000_000);
    let day_micros = micros.rem_euclid(86_400_000_000);
    let (year, month, day) = civil_from_days(days as i32 + days_from_civil(2000, 1, 1));
    Ok(DateTimeValue {
        date: DateValue { year, month, day },
        time: time_from_micros(day_micros)?,
    })
}

fn civil_from_days(days: i32) -> (i32, u8, u8) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + i32::from(month <= 2);
    (year, month as u8, day as u8)
}

#[inline]
pub(crate) fn parse_number<T>(bytes: &[u8], name: &'static str) -> Result<T>
where
    T: str::FromStr,
    T::Err: Display,
{
    let text = str::from_utf8(bytes).map_err(|err| Error::new(err.to_string()))?;
    text.parse::<T>()
        .map_err(|err| Error::new(format!("failed to parse {}: {}", name, err)))
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
pub(super) fn c_error_string(ptr: *const i8, fallback: String) -> String {
    if ptr.is_null() {
        return fallback;
    }
    // SAFETY: ptr is expected to be a valid nul-terminated string from libpq.
    let text = unsafe { CStr::from_ptr(ptr) }.to_string_lossy();
    if text.is_empty() {
        fallback
    } else {
        text.into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::super::connection::Connection;
    use super::super::rows::{ColumnIndex, Row, pq_bytes};
    use super::*;

    fn assert_send<T: Send>() {}

    #[test]
    fn pq_bytes_allows_zero_length_null_pointer() {
        let bytes = unsafe { pq_bytes(std::ptr::null(), 0) };
        assert!(bytes.is_empty());
    }

    #[test]
    fn connect_options_builder_round_trips() {
        let opts = ConnectOptions::new()
            .host("127.0.0.1")
            .port(5433)
            .user("postgres")
            .password("postgres")
            .database("postgres");

        assert_eq!(opts.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(opts.port, 5433);
        assert_eq!(opts.user.as_deref(), Some("postgres"));
        assert_eq!(opts.password.as_deref(), Some("postgres"));
        assert_eq!(opts.database.as_deref(), Some("postgres"));
    }

    #[test]
    fn row_name_lookup_works() {
        let metadata = Metadata {
            columns: vec![
                Column {
                    name: "id".into(),
                    type_oid: OID_INT8,
                    format: 0,
                    nullable: false,
                },
                Column {
                    name: "name".into(),
                    type_oid: OID_TEXT,
                    format: 0,
                    nullable: false,
                },
            ],
            name_order: vec![0, 1].into_boxed_slice(),
        };

        assert_eq!(
            "id".index(&metadata.columns, &metadata.name_order).unwrap(),
            0
        );
        assert_eq!(
            "name"
                .index(&metadata.columns, &metadata.name_order)
                .unwrap(),
            1
        );
    }

    #[test]
    fn bytea_encoding_uses_hex_prefix() {
        assert_eq!(encode_bytea_hex(&[0xAB, 0xCD]), br"\xabcd");
    }

    #[test]
    fn date_time_binary_encodings_match_postgres_epoch_rules() {
        assert_eq!(
            encode_date_binary(DateValue {
                year: 2000,
                month: 1,
                day: 1,
            }),
            0i32.to_be_bytes(),
        );

        assert_eq!(
            encode_time_binary(TimeValue {
                hour: 1,
                minute: 2,
                second: 3,
                microsecond: 456_789,
            }),
            3_723_456_789i64.to_be_bytes(),
        );

        assert_eq!(
            encode_datetime_binary(DateTimeValue {
                date: DateValue {
                    year: 2000,
                    month: 1,
                    day: 2,
                },
                time: TimeValue {
                    hour: 0,
                    minute: 0,
                    second: 0,
                    microsecond: 0,
                },
            }),
            86_400_000_000i64.to_be_bytes(),
        );

        assert_eq!(
            encode_datetime_tz_binary(DateTimeTzValue {
                datetime: DateTimeValue {
                    date: DateValue {
                        year: 2000,
                        month: 1,
                        day: 1,
                    },
                    time: TimeValue {
                        hour: 1,
                        minute: 0,
                        second: 0,
                        microsecond: 0,
                    },
                },
                offset_seconds: 3600,
            }),
            0i64.to_be_bytes(),
        );
    }

    #[test]
    fn uuid_params_bind_as_binary() {
        struct OneUuid([u8; 16]);

        impl ParamSource for OneUuid {
            fn len(&self) -> usize {
                1
            }

            fn value_at(&self, index: usize) -> ValueRef<'_> {
                assert_eq!(index, 0);
                ValueRef::Uuid(&self.0)
            }
        }

        let uuid = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];
        let params = OneUuid(uuid);
        let mut scratch = ParamScratch::new();
        let (lengths, formats, value_ptr) = {
            let bound = scratch.bind_source(&params);
            (
                bound.lengths.to_vec(),
                bound.formats.to_vec(),
                bound.values[0],
            )
        };

        assert_eq!(lengths, [16]);
        assert_eq!(formats, [1]);
        assert_eq!(scratch.owned.len(), 1);
        assert_eq!(scratch.owned[0].as_slice(), &uuid);
        assert_eq!(value_ptr, scratch.owned[0].as_ptr().cast());
    }

    #[test]
    fn native_types_are_send() {
        assert_send::<Connection>();
        assert_send::<ResultSet>();
        assert_send::<Row>();
    }

    #[test]
    fn cancelled_operation_poisons_connection_state() {
        let state = Arc::new(AtomicU8::new(CONNECTION_READY));
        {
            let _guard = ConnectionOpGuard::new(&state);
        }

        assert_eq!(state.load(Ordering::Acquire), CONNECTION_POISONED);
    }

    #[test]
    fn completed_operation_keeps_connection_state_ready() {
        let state = Arc::new(AtomicU8::new(CONNECTION_READY));
        {
            let mut guard = ConnectionOpGuard::new(&state);
            guard.complete();
        }

        assert_eq!(state.load(Ordering::Acquire), CONNECTION_READY);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires a local PostgreSQL instance"]
    async fn postgres_round_trip() {
        let mut conn = Connection::connect(
            ConnectOptions::new()
                .host("127.0.0.1")
                .port(5432)
                .user("postgres")
                .password("postgres")
                .database("postgres"),
        )
        .await
        .expect("connect");

        let mut rows = conn
            .query("select 42::bigint as id, 'Ada'::text as name")
            .await
            .expect("query");
        let row = rows.next().await.expect("next").expect("row");
        assert_eq!(row.get_i64("id").expect("id"), 42);
        assert_eq!(row.get_str("name").expect("name"), "Ada");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires a local PostgreSQL instance"]
    async fn postgres_large_scan() {
        let mut conn = Connection::connect(
            ConnectOptions::new()
                .host("127.0.0.1")
                .port(5432)
                .user("postgres")
                .password("postgres")
                .database("postgres"),
        )
        .await
        .expect("connect");

        let mut rows = conn
            .query("select g::bigint as id, (g * 7)::bigint as score from generate_series(1, 2000) g order by g")
            .await
            .expect("query");

        let mut seen = 0;
        while let Some(row) = rows.next().await.expect("next") {
            let _ = row.get_i64("id").expect("id");
            let _ = row.get_i64("score").expect("score");
            seen += 1;
        }

        assert_eq!(seen, 2000);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires a local PostgreSQL instance"]
    async fn postgres_connection_moves_across_tokio_worker_threads() {
        let conn = Connection::connect(
            ConnectOptions::new()
                .host("127.0.0.1")
                .port(5432)
                .user("postgres")
                .password("postgres")
                .database("postgres"),
        )
        .await
        .expect("connect");

        let conn = tokio::spawn(async move {
            let mut conn = conn;
            let mut rows = conn
                .query("select 7::bigint as id, 'tokio-a'::text as name")
                .await
                .expect("query");
            let row = rows.next().await.expect("next").expect("row");
            assert_eq!(row.get_i64("id").expect("id"), 7);
            assert_eq!(row.get_str("name").expect("name"), "tokio-a");
            drop(rows);
            conn
        })
        .await
        .expect("join query task");

        tokio::spawn(async move {
            let mut conn = conn;
            let mut stmt = conn
                .prepare_cached("select $1::bigint as id, $2::text as name")
                .await
                .expect("prepare");
            let mut rows = stmt
                .execute(&[Value::I64(9), Value::String("tokio-b".into())])
                .await
                .expect("execute");
            let row = rows.next().await.expect("next").expect("row");
            assert_eq!(row.get_i64("id").expect("id"), 9);
            assert_eq!(row.get_str("name").expect("name"), "tokio-b");
        })
        .await
        .expect("join prepared task");
    }
}
