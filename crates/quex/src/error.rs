use std::fmt::{Display, Formatter};

/// Errors returned by the facade.
///
/// Driver errors are kept intact so callers can still inspect or display the
/// original mariadb, mysql, postgres, or sqlite failure.
#[derive(Debug)]
pub enum Error {
    /// A database URL could not be parsed or used.
    InvalidUrl(String),
    /// The pool was closed and no longer hands out connections.
    PoolClosed,
    /// The pool is at capacity and cannot hand out another connection without waiting.
    PoolExhausted,
    /// Waiting for a pool connection hit the configured timeout.
    PoolTimedOut,
    /// The requested conversion or operation is not supported by the facade.
    Unsupported(String),
    #[cfg(feature = "mysql")]
    /// Error returned by the mysql or mariadb driver.
    Mysql(quex_driver::mysql::Error),
    #[cfg(feature = "postgres")]
    /// Error returned by the postgres driver.
    Postgres(quex_driver::postgres::Error),
    #[cfg(feature = "sqlite")]
    /// Error returned by the sqlite driver.
    Sqlite(quex_driver::sqlite::Error),
}

impl Error {
    pub(crate) fn invalid_url(message: impl Into<String>) -> Self {
        Self::InvalidUrl(message.into())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUrl(message) => write!(f, "invalid database URL: {message}"),
            Self::PoolClosed => f.write_str("pool is closed"),
            Self::PoolExhausted => f.write_str("pool is at capacity"),
            Self::PoolTimedOut => f.write_str("timed out waiting for a pool connection"),
            Self::Unsupported(message) => f.write_str(message),
            #[cfg(feature = "mysql")]
            Self::Mysql(err) => Display::fmt(err, f),
            #[cfg(feature = "postgres")]
            Self::Postgres(err) => Display::fmt(err, f),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(err) => Display::fmt(err, f),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(feature = "mysql")]
impl From<quex_driver::mysql::Error> for Error {
    fn from(value: quex_driver::mysql::Error) -> Self {
        Self::Mysql(value)
    }
}

#[cfg(feature = "postgres")]
impl From<quex_driver::postgres::Error> for Error {
    fn from(value: quex_driver::postgres::Error) -> Self {
        Self::Postgres(value)
    }
}

#[cfg(feature = "sqlite")]
impl From<quex_driver::sqlite::Error> for Error {
    fn from(value: quex_driver::sqlite::Error) -> Self {
        Self::Sqlite(value)
    }
}

/// Convenient result type used by `quex`.
pub type Result<T> = std::result::Result<T, Error>;
