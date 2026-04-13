use std::ffi::NulError;
use std::fmt::{Display, Formatter};

use quex_pq_sys as ffi;

use super::runtime::c_error_string;

/// Convenient result type for the postgres driver.
pub type Result<T> = std::result::Result<T, Error>;

/// Result metadata for a statement that does not return rows.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExecuteResult {
    /// Number of rows changed by the statement.
    pub rows_affected: u64,
    /// Last generated id when the caller requested one.
    pub last_insert_id: Option<u64>,
}

/// Error returned by the postgres driver.
#[derive(Debug, Clone)]
pub struct Error {
    sql_state: Option<String>,
    message: String,
}

impl Error {
    #[inline]
    pub(super) fn new(message: impl Into<String>) -> Self {
        Self {
            sql_state: None,
            message: message.into(),
        }
    }

    pub(super) unsafe fn from_conn(conn: *mut ffi::PGconn, fallback: impl Into<String>) -> Self {
        if conn.is_null() {
            return Self::new(fallback);
        }

        let message = c_error_string(unsafe { ffi::PQerrorMessage(conn) }, fallback.into());
        Self {
            sql_state: None,
            message,
        }
    }

    pub(super) unsafe fn from_result(
        result: *mut ffi::PGresult,
        fallback: impl Into<String>,
    ) -> Self {
        if result.is_null() {
            return Self::new(fallback);
        }

        let sql_state = unsafe {
            let field = ffi::PQresultErrorField(result, ffi::PG_DIAG_SQLSTATE as i32);
            if field.is_null() {
                None
            } else {
                Some(
                    std::ffi::CStr::from_ptr(field)
                        .to_string_lossy()
                        .into_owned(),
                )
            }
        };
        let message = c_error_string(
            unsafe { ffi::PQresultErrorMessage(result) },
            fallback.into(),
        );
        Self { sql_state, message }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.sql_state {
            Some(state) => write!(f, "{} (sqlstate {})", self.message, state),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::new(value.to_string())
    }
}

impl From<NulError> for Error {
    fn from(value: NulError) -> Self {
        Self::new(value.to_string())
    }
}
