/// Connection settings for mysql or mariadb.
#[derive(Debug, Clone, Default)]
pub struct ConnectOptions {
    pub(super) host: Option<String>,
    pub(super) user: Option<String>,
    pub(super) password: Option<String>,
    pub(super) database: Option<String>,
    pub(super) port: u32,
    pub(super) unix_socket: Option<String>,
}

impl ConnectOptions {
    /// Starts with libmariadb defaults.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the network host.
    #[inline]
    pub fn host(mut self, value: impl Into<String>) -> Self {
        self.host = Some(value.into());
        self
    }

    /// Sets the username.
    #[inline]
    pub fn user(mut self, value: impl Into<String>) -> Self {
        self.user = Some(value.into());
        self
    }

    /// Sets the password.
    #[inline]
    pub fn password(mut self, value: impl Into<String>) -> Self {
        self.password = Some(value.into());
        self
    }

    /// Sets the database name.
    #[inline]
    pub fn database(mut self, value: impl Into<String>) -> Self {
        self.database = Some(value.into());
        self
    }

    /// Sets the network port.
    #[inline]
    pub fn port(mut self, value: u32) -> Self {
        self.port = value;
        self
    }

    /// Sets a unix socket path.
    #[inline]
    pub fn unix_socket(mut self, value: impl Into<String>) -> Self {
        self.unix_socket = Some(value.into());
        self
    }
}
