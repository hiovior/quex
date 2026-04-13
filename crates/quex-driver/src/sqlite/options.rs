use std::path::PathBuf;
use std::time::Duration;

/// Connection settings for sqlite.
#[derive(Debug, Clone, Default)]
pub struct ConnectOptions {
    pub(super) path: Option<PathBuf>,
    pub(super) in_memory: bool,
    pub(super) read_only: bool,
    pub(super) create_if_missing: bool,
    pub(super) busy_timeout: Option<Duration>,
}

impl ConnectOptions {
    /// Starts with an on-disk sqlite database created when missing.
    #[inline]
    pub fn new() -> Self {
        Self {
            create_if_missing: true,
            ..Self::default()
        }
    }

    /// Sets the database path.
    #[inline]
    pub fn path(mut self, value: impl Into<PathBuf>) -> Self {
        self.path = Some(value.into());
        self
    }

    /// Uses an in-memory database.
    ///
    /// This clears any path already set on the options.
    #[inline]
    pub fn in_memory(mut self) -> Self {
        self.in_memory = true;
        self.path = None;
        self
    }

    /// Sets whether sqlite opens the database as read-only.
    #[inline]
    pub fn read_only(mut self, value: bool) -> Self {
        self.read_only = value;
        self
    }

    /// Sets whether sqlite creates the database file when missing.
    #[inline]
    pub fn create_if_missing(mut self, value: bool) -> Self {
        self.create_if_missing = value;
        self
    }

    /// Sets sqlite's busy timeout.
    #[inline]
    pub fn busy_timeout(mut self, value: Duration) -> Self {
        self.busy_timeout = Some(value);
        self
    }
}
