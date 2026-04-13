use std::path::PathBuf;
use std::time::Duration;

use url::Url;

use crate::{Error, Result};

/// The database driver selected for a connection or pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Driver {
    /// mysql or mariadb through libmariadb.
    Mysql,
    /// postgres through libpq.
    Pgsql,
    /// sqlite through sqlite3.
    Sqlite,
}

impl Driver {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "mysql" => Ok(Self::Mysql),
            "pgsql" | "postgres" | "postgresql" => Ok(Self::Pgsql),
            "sqlite" => Ok(Self::Sqlite),
            _ => Err(Error::invalid_url(format!("unknown driver `{value}`"))),
        }
    }
}

/// Connection settings parsed from a URL or built manually.
///
/// Use this when the driver is selected dynamically. For direct construction,
/// [`MysqlConnectOptions`], [`PostgresConnectOptions`], and
/// [`SqliteConnectOptions`] keep call sites a little clearer.
///
/// ```
/// let options = quex::ConnectOptions::from_url("sqlite::memory:")?;
/// # Ok::<(), quex::Error>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct ConnectOptions {
    pub(crate) driver: Option<Driver>,
    pub(crate) host: Option<String>,
    pub(crate) port: Option<u16>,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) database: Option<String>,
    pub(crate) unix_socket: Option<String>,
    pub(crate) path: Option<PathBuf>,
    pub(crate) in_memory: bool,
    pub(crate) read_only: bool,
    pub(crate) create_if_missing: bool,
    pub(crate) busy_timeout: Option<Duration>,
}

impl ConnectOptions {
    /// Starts a new option builder for the selected driver.
    pub fn new(driver: Driver) -> Self {
        Self {
            driver: Some(driver),
            create_if_missing: true,
            ..Self::default()
        }
    }

    /// Sets the selected driver.
    pub fn driver(mut self, value: Driver) -> Self {
        self.driver = Some(value);
        self
    }

    /// Sets the network host.
    ///
    /// This is used by mysql, mariadb, and postgres.
    pub fn host(mut self, value: impl Into<String>) -> Self {
        self.host = Some(value.into());
        self
    }

    /// Sets the network port.
    pub fn port(mut self, value: u16) -> Self {
        self.port = Some(value);
        self
    }

    /// Sets the database username.
    pub fn username(mut self, value: impl Into<String>) -> Self {
        self.username = Some(value.into());
        self
    }

    /// Sets the database password.
    pub fn password(mut self, value: impl Into<String>) -> Self {
        self.password = Some(value.into());
        self
    }

    /// Sets the database name.
    pub fn database(mut self, value: impl Into<String>) -> Self {
        self.database = Some(value.into());
        self
    }

    /// Sets a unix socket path for mysql or mariadb.
    pub fn unix_socket(mut self, value: impl Into<String>) -> Self {
        self.unix_socket = Some(value.into());
        self
    }

    /// Sets the sqlite database path.
    pub fn path(mut self, value: impl Into<PathBuf>) -> Self {
        self.path = Some(value.into());
        self
    }

    /// Uses an in-memory sqlite database.
    ///
    /// This clears any path already set on the options.
    pub fn in_memory(mut self) -> Self {
        self.in_memory = true;
        self.path = None;
        self
    }

    /// Sets whether sqlite opens the database as read-only.
    pub fn read_only(mut self, value: bool) -> Self {
        self.read_only = value;
        self
    }

    /// Sets whether sqlite creates the database file when missing.
    pub fn create_if_missing(mut self, value: bool) -> Self {
        self.create_if_missing = value;
        self
    }

    /// Sets sqlite's busy timeout.
    pub fn busy_timeout(mut self, value: Duration) -> Self {
        self.busy_timeout = Some(value);
        self
    }

    /// Parses a database URL.
    ///
    /// Supported schemes are `mysql`, `postgres`, `postgresql`, `pgsql`, and
    /// `sqlite`. sqlite also accepts `sqlite::memory:`.
    pub fn from_url(input: &str) -> Result<Self> {
        if input == "sqlite::memory:" {
            return Ok(Self::new(Driver::Sqlite).in_memory());
        }

        if let Some(path) = input.strip_prefix("sqlite:") {
            return parse_sqlite_url(path);
        }

        let url = Url::parse(input).map_err(|err| Error::invalid_url(err.to_string()))?;
        let driver = Driver::parse(url.scheme())?;

        match driver {
            Driver::Mysql => {
                let mut options = Self::new(Driver::Mysql);
                if let Some(host) = url.host_str() {
                    options = options.host(host);
                }
                if let Some(port) = url.port() {
                    options = options.port(port);
                }
                if !url.username().is_empty() {
                    options = options.username(url.username());
                }
                if let Some(password) = url.password() {
                    options = options.password(password);
                }
                let database = url.path().trim_start_matches('/');
                if !database.is_empty() {
                    options = options.database(database);
                }
                Ok(options)
            }
            Driver::Pgsql => {
                let mut options = Self::new(Driver::Pgsql);
                if let Some(host) = url.host_str() {
                    options = options.host(host);
                }
                if let Some(port) = url.port() {
                    options = options.port(port);
                }
                if !url.username().is_empty() {
                    options = options.username(url.username());
                }
                if let Some(password) = url.password() {
                    options = options.password(password);
                }
                let database = url.path().trim_start_matches('/');
                if !database.is_empty() {
                    options = options.database(database);
                }
                Ok(options)
            }
            Driver::Sqlite => {
                let mut options = Self::new(Driver::Sqlite);
                let path = sqlite_path_from_url(&url)?;
                options = options.path(path);
                Ok(options)
            }
        }
    }
}

/// mysql or mariadb connection settings.
#[derive(Debug, Clone, Default)]
pub struct MysqlConnectOptions {
    pub(crate) host: Option<String>,
    pub(crate) port: Option<u32>,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) database: Option<String>,
    pub(crate) unix_socket: Option<String>,
}

impl MysqlConnectOptions {
    /// Starts with driver defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the network host.
    pub fn host(mut self, value: impl Into<String>) -> Self {
        self.host = Some(value.into());
        self
    }

    /// Sets the network port.
    pub fn port(mut self, value: u32) -> Self {
        self.port = Some(value);
        self
    }

    /// Sets the database username.
    pub fn username(mut self, value: impl Into<String>) -> Self {
        self.username = Some(value.into());
        self
    }

    /// Sets the database password.
    pub fn password(mut self, value: impl Into<String>) -> Self {
        self.password = Some(value.into());
        self
    }

    /// Sets the database name.
    pub fn database(mut self, value: impl Into<String>) -> Self {
        self.database = Some(value.into());
        self
    }

    /// Sets a unix socket path.
    pub fn unix_socket(mut self, value: impl Into<String>) -> Self {
        self.unix_socket = Some(value.into());
        self
    }
}

#[cfg(feature = "mysql")]
impl From<MysqlConnectOptions> for quex_driver::mysql::ConnectOptions {
    fn from(value: MysqlConnectOptions) -> Self {
        let mut options = quex_driver::mysql::ConnectOptions::new();
        if let Some(host) = value.host {
            options = options.host(host);
        }
        if let Some(port) = value.port {
            options = options.port(port);
        }
        if let Some(username) = value.username {
            options = options.user(username);
        }
        if let Some(password) = value.password {
            options = options.password(password);
        }
        if let Some(database) = value.database {
            options = options.database(database);
        }
        if let Some(unix_socket) = value.unix_socket {
            options = options.unix_socket(unix_socket);
        }
        options
    }
}

impl From<MysqlConnectOptions> for ConnectOptions {
    fn from(value: MysqlConnectOptions) -> Self {
        let mut options = Self::new(Driver::Mysql);
        if let Some(host) = value.host {
            options = options.host(host);
        }
        if let Some(port) = value.port {
            options = options.port(port as u16);
        }
        if let Some(username) = value.username {
            options = options.username(username);
        }
        if let Some(password) = value.password {
            options = options.password(password);
        }
        if let Some(database) = value.database {
            options = options.database(database);
        }
        if let Some(unix_socket) = value.unix_socket {
            options = options.unix_socket(unix_socket);
        }
        options
    }
}

/// postgres connection settings.
#[derive(Debug, Clone, Default)]
pub struct PostgresConnectOptions {
    pub(crate) host: Option<String>,
    pub(crate) port: Option<u16>,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) database: Option<String>,
}

impl PostgresConnectOptions {
    /// Starts with driver defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the network host.
    pub fn host(mut self, value: impl Into<String>) -> Self {
        self.host = Some(value.into());
        self
    }

    /// Sets the network port.
    pub fn port(mut self, value: u16) -> Self {
        self.port = Some(value);
        self
    }

    /// Sets the database username.
    pub fn username(mut self, value: impl Into<String>) -> Self {
        self.username = Some(value.into());
        self
    }

    /// Sets the database password.
    pub fn password(mut self, value: impl Into<String>) -> Self {
        self.password = Some(value.into());
        self
    }

    /// Sets the database name.
    pub fn database(mut self, value: impl Into<String>) -> Self {
        self.database = Some(value.into());
        self
    }
}

#[cfg(feature = "postgres")]
impl From<PostgresConnectOptions> for quex_driver::postgres::ConnectOptions {
    fn from(value: PostgresConnectOptions) -> Self {
        let mut options = quex_driver::postgres::ConnectOptions::new();
        if let Some(host) = value.host {
            options = options.host(host);
        }
        if let Some(port) = value.port {
            options = options.port(port);
        }
        if let Some(username) = value.username {
            options = options.user(username);
        }
        if let Some(password) = value.password {
            options = options.password(password);
        }
        if let Some(database) = value.database {
            options = options.database(database);
        }
        options
    }
}

impl From<PostgresConnectOptions> for ConnectOptions {
    fn from(value: PostgresConnectOptions) -> Self {
        let mut options = Self::new(Driver::Pgsql);
        if let Some(host) = value.host {
            options = options.host(host);
        }
        if let Some(port) = value.port {
            options = options.port(port);
        }
        if let Some(username) = value.username {
            options = options.username(username);
        }
        if let Some(password) = value.password {
            options = options.password(password);
        }
        if let Some(database) = value.database {
            options = options.database(database);
        }
        options
    }
}

/// sqlite connection settings.
#[derive(Debug, Clone, Default)]
pub struct SqliteConnectOptions {
    pub(crate) path: Option<PathBuf>,
    pub(crate) in_memory: bool,
    pub(crate) read_only: bool,
    pub(crate) create_if_missing: bool,
    pub(crate) busy_timeout: Option<Duration>,
}

impl SqliteConnectOptions {
    /// Starts with an on-disk sqlite database created when missing.
    pub fn new() -> Self {
        Self {
            create_if_missing: true,
            ..Self::default()
        }
    }

    /// Sets the database path.
    pub fn path(mut self, value: impl Into<PathBuf>) -> Self {
        self.path = Some(value.into());
        self
    }

    /// Uses an in-memory database.
    ///
    /// This clears any path already set on the options.
    pub fn in_memory(mut self) -> Self {
        self.in_memory = true;
        self.path = None;
        self
    }

    /// Sets whether sqlite opens the database as read-only.
    pub fn read_only(mut self, value: bool) -> Self {
        self.read_only = value;
        self
    }

    /// Sets whether sqlite creates the database file when missing.
    pub fn create_if_missing(mut self, value: bool) -> Self {
        self.create_if_missing = value;
        self
    }

    /// Sets sqlite's busy timeout.
    pub fn busy_timeout(mut self, value: Duration) -> Self {
        self.busy_timeout = Some(value);
        self
    }
}

#[cfg(feature = "sqlite")]
impl From<SqliteConnectOptions> for quex_driver::sqlite::ConnectOptions {
    fn from(value: SqliteConnectOptions) -> Self {
        let mut options = quex_driver::sqlite::ConnectOptions::new()
            .read_only(value.read_only)
            .create_if_missing(value.create_if_missing);
        if let Some(timeout) = value.busy_timeout {
            options = options.busy_timeout(timeout);
        }
        if value.in_memory {
            options = options.in_memory();
        } else if let Some(path) = value.path {
            options = options.path(path);
        }
        options
    }
}

impl From<SqliteConnectOptions> for ConnectOptions {
    fn from(value: SqliteConnectOptions) -> Self {
        let mut options = Self::new(Driver::Sqlite)
            .read_only(value.read_only)
            .create_if_missing(value.create_if_missing);
        if let Some(timeout) = value.busy_timeout {
            options = options.busy_timeout(timeout);
        }
        if value.in_memory {
            options = options.in_memory();
        } else if let Some(path) = value.path {
            options = options.path(path);
        }
        options
    }
}

fn parse_sqlite_url(rest: &str) -> Result<ConnectOptions> {
    if rest == ":memory:" {
        return Ok(ConnectOptions::new(Driver::Sqlite).in_memory());
    }
    if rest.is_empty() {
        return Err(Error::invalid_url("sqlite URL is missing a database path"));
    }
    if let Some(path) = rest.strip_prefix("///") {
        return Ok(ConnectOptions::new(Driver::Sqlite).path(format!("/{}", path)));
    }
    if let Some(path) = rest.strip_prefix("//") {
        if path.is_empty() {
            return Err(Error::invalid_url("sqlite URL is missing a database path"));
        }
        return Ok(ConnectOptions::new(Driver::Sqlite).path(path));
    }
    Ok(ConnectOptions::new(Driver::Sqlite).path(rest))
}

fn sqlite_path_from_url(url: &Url) -> Result<PathBuf> {
    if url.scheme() != "sqlite" {
        return Err(Error::invalid_url("expected sqlite URL"));
    }

    if url.host_str().is_some() && url.host_str() != Some("") {
        return Err(Error::invalid_url(
            "sqlite URL must not include a network host component",
        ));
    }

    let path = url.path();
    if path.is_empty() || path == "/" {
        return Err(Error::invalid_url("sqlite URL is missing a database path"));
    }

    Ok(PathBuf::from(path))
}
