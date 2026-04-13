use std::ffi::NulError;
use std::fmt::{Display, Formatter};

use quex_mariadb_sys as ffi;

use super::runtime::{c_error_string, c_opt_string};

/// Convenient result type for the mysql and mariadb driver.
pub type Result<T> = std::result::Result<T, Error>;

/// Result metadata for a statement that does not return rows.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExecuteResult {
    /// Number of rows changed by the statement.
    pub rows_affected: u64,
    /// Last generated id reported by libmariadb.
    ///
    /// libmariadb reports `0` when there is no generated id.
    pub last_insert_id: u64,
}

/// Error returned by the mysql and mariadb driver.
#[derive(Debug, Clone)]
pub struct Error {
    code: Option<u32>,
    sql_state: Option<String>,
    message: String,
}

impl Error {
    #[inline]
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            code: None,
            sql_state: None,
            message: message.into(),
        }
    }

    pub(super) unsafe fn from_mysql(mysql: *mut ffi::MYSQL, fallback: impl Into<String>) -> Self {
        if mysql.is_null() {
            return Self::new(fallback);
        }

        // SAFETY: mysql is non-null and points to a live MYSQL handle.
        let code = unsafe { ffi::mysql_errno(mysql) };
        // SAFETY: mysql is valid and mysql_error returns a NUL-terminated string copied here.
        let message = c_error_string(unsafe { ffi::mysql_error(mysql) }, fallback.into());
        let sql_state = c_opt_string(unsafe { ffi::mysql_sqlstate(mysql) });

        Self {
            code: Some(code),
            sql_state,
            message,
        }
    }

    pub(super) unsafe fn from_stmt(
        stmt: *mut ffi::MYSQL_STMT,
        fallback: impl Into<String>,
    ) -> Self {
        if stmt.is_null() {
            return Self::new(fallback);
        }

        // SAFETY: stmt is non-null and points to a live MYSQL_STMT handle.
        let code = unsafe { ffi::mysql_stmt_errno(stmt) };
        // SAFETY: stmt is valid and mysql_stmt_error returns a NUL-terminated string copied here.
        let message = c_error_string(unsafe { ffi::mysql_stmt_error(stmt) }, fallback.into());
        let sql_state = c_opt_string(unsafe { ffi::mysql_stmt_sqlstate(stmt) });

        Self {
            code: Some(code),
            sql_state,
            message,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match (self.code, &self.sql_state) {
            (Some(code), Some(state)) => {
                write!(f, "{} (code {}, sqlstate {})", self.message, code, state)
            }
            _ => f.write_str(&self.message),
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
