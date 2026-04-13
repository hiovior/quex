/// Connection settings for postgres.
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    pub(super) host: Option<String>,
    pub(super) port: u16,
    pub(super) user: Option<String>,
    pub(super) password: Option<String>,
    pub(super) database: Option<String>,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            host: None,
            port: 5432,
            user: None,
            password: None,
            database: None,
        }
    }
}

impl ConnectOptions {
    /// Starts with libpq defaults and port 5432.
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

    /// Sets the network port.
    #[inline]
    pub fn port(mut self, value: u16) -> Self {
        self.port = value;
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
}
