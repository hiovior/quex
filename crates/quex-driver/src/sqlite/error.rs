use std::ffi::NulError;
use std::fmt::{Display, Formatter};

use quex_sqlite3_sys as ffi;

use super::runtime::{WorkerError, c_sqlite_string};

/// Convenient result type for the sqlite driver.
pub type Result<T> = std::result::Result<T, Error>;

/// Error returned by the sqlite driver.
#[derive(Debug, Clone)]
pub struct Error {
    pub(super) code: Option<i32>,
    pub(super) message: String,
}

impl Error {
    #[inline]
    pub(super) fn new(message: impl Into<String>) -> Self {
        Self {
            code: None,
            message: message.into(),
        }
    }

    pub(super) unsafe fn from_db(
        db: *mut ffi::sqlite3,
        code: i32,
        fallback: impl Into<String>,
    ) -> Self {
        if db.is_null() {
            return Self {
                code: Some(code),
                message: fallback.into(),
            };
        }

        Self {
            code: Some(code),
            message: c_sqlite_string(unsafe { ffi::sqlite3_errmsg(db) })
                .unwrap_or_else(|| fallback.into()),
        }
    }

    pub(super) fn from_worker(error: WorkerError) -> Self {
        Self {
            code: error.code,
            message: error.message,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.code {
            Some(code) => write!(f, "{} (sqlite code {})", self.message, code),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for Error {}

impl From<NulError> for Error {
    fn from(value: NulError) -> Self {
        Self::new(value.to_string())
    }
}
