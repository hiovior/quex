#![cfg_attr(
    not(any(feature = "mysql", feature = "postgres", feature = "sqlite")),
    allow(unreachable_code, unused_variables)
)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{future::Future, ops::AsyncFn, pin::Pin};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::time::timeout;

use crate::connection::{
    ManagedConnection, commit_inner, connect_managed, prepare_inner, query_inner, rollback_inner,
    start_transaction_inner,
};
use crate::{
    BoundStatement, ConnectOptions, Driver, Encode, Error, ExecResult, Executor,
    MysqlConnectOptions, ParamSource, PostgresConnectOptions, PreparedStatement, Result, Rows,
    SqliteConnectOptions, Statement,
};

/// A simple handle for one database connection.
///
/// Use `Connection` when one connection is enough. Use [`Pool`] when many
/// tasks need to acquire their own connection.
///
/// `Connection` owns one connection for its whole lifetime. It does not do
/// pooling or background connection management.
pub struct Connection {
    pub(crate) conn: ManagedConnection,
}

/// Converts a value into generic connection options.
///
/// Strings are parsed as database URLs. Typed option builders are copied into
/// the generic option type.
///
/// This trait is mostly a convenience for constructors like
/// [`Connection::connect`]
/// and [`Pool::connect`].
pub trait IntoConnectOptions {
    /// Converts into [`ConnectOptions`].
    fn into_connect_options(self) -> Result<ConnectOptions>;
}

impl<T> IntoConnectOptions for T
where
    T: AsRef<str>,
{
    fn into_connect_options(self) -> Result<ConnectOptions> {
        ConnectOptions::from_url(self.as_ref())
    }
}

impl IntoConnectOptions for SqliteConnectOptions {
    fn into_connect_options(self) -> Result<ConnectOptions> {
        Ok(ConnectOptions::from(self))
    }
}

impl IntoConnectOptions for PostgresConnectOptions {
    fn into_connect_options(self) -> Result<ConnectOptions> {
        Ok(ConnectOptions::from(self))
    }
}

impl IntoConnectOptions for MysqlConnectOptions {
    fn into_connect_options(self) -> Result<ConnectOptions> {
        Ok(ConnectOptions::from(self))
    }
}

impl IntoConnectOptions for ConnectOptions {
    fn into_connect_options(self) -> Result<ConnectOptions> {
        Ok(self)
    }
}

impl Connection {
    /// Connects from a database URL or typed connection options.
    pub async fn connect(options: impl IntoConnectOptions) -> Result<Self> {
        let options = options.into_connect_options()?;
        Ok(Self {
            conn: connect_managed(options).await?,
        })
    }

    /// Returns the selected driver.
    pub fn driver(&self) -> Driver {
        self.conn.driver
    }

    /// Runs SQL and returns rows.
    ///
    /// For statements with parameters, prefer [`crate::query`].
    pub async fn query(&mut self, sql: &str) -> Result<Rows<'_>> {
        query_inner(&mut self.conn.inner, sql).await
    }

    /// Prepares a reusable statement.
    pub async fn prepare(&mut self, sql: &str) -> Result<Statement<'_>> {
        prepare_inner(&mut self.conn.inner, sql).await
    }

    /// Starts a transaction.
    ///
    /// Dropping the transaction without committing or rolling back follows the
    /// behavior of the selected driver.
    pub async fn begin(&mut self) -> Result<Transaction<'_>> {
        Transaction::begin(&mut self.conn).await
    }
}

impl Executor for &mut Connection {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;
    type Statement<'a>
        = Statement<'a>
    where
        Self: 'a;

    fn driver(&self) -> Driver {
        Connection::driver(self)
    }

    async fn query(&mut self, sql: &str) -> Result<Self::Rows<'_>> {
        Connection::query(*self, sql).await
    }

    async fn query_prepared_source<P>(&mut self, sql: &str, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut stmt = Connection::prepare(*self, sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement<'_>> {
        Connection::prepare(*self, sql).await
    }
}

impl Executor for Connection {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;
    type Statement<'a>
        = Statement<'a>
    where
        Self: 'a;

    fn driver(&self) -> Driver {
        self.driver()
    }

    async fn query(&mut self, sql: &str) -> Result<Self::Rows<'_>> {
        Connection::query(self, sql).await
    }

    async fn query_prepared_source<P>(&mut self, sql: &str, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut stmt = Connection::prepare(self, sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement<'_>> {
        Connection::prepare(self, sql).await
    }
}

/// A transaction on a [`Connection`] connection.
///
/// Dropping a transaction before commit or rollback follows the behavior of the
/// selected driver.
///
/// While the transaction is alive, it keeps the `Connection` borrowed.
pub struct Transaction<'a> {
    inner: Option<TransactionInner<'a>>,
}

enum TransactionInner<'a> {
    #[cfg(feature = "mysql")]
    Mysql(quex_driver::mysql::Transaction<'a>),
    #[cfg(feature = "postgres")]
    Postgres(quex_driver::postgres::Transaction<'a>),
    #[cfg(feature = "sqlite")]
    Sqlite(quex_driver::sqlite::Transaction<'a>),
    _Marker(std::marker::PhantomData<&'a ()>),
}

impl<'a> Transaction<'a> {
    async fn begin(conn: &'a mut ManagedConnection) -> Result<Self> {
        let inner = match &mut conn.inner {
            #[cfg(feature = "mysql")]
            crate::connection::ConnectionInner::Mysql(conn) => {
                TransactionInner::Mysql(conn.begin().await?)
            }
            #[cfg(feature = "postgres")]
            crate::connection::ConnectionInner::Postgres(conn) => {
                TransactionInner::Postgres(conn.begin().await?)
            }
            #[cfg(feature = "sqlite")]
            crate::connection::ConnectionInner::Sqlite(conn) => {
                TransactionInner::Sqlite(conn.begin().await?)
            }
            crate::connection::ConnectionInner::_Disabled => {
                unreachable!("disabled backend placeholder")
            }
        };
        Ok(Self { inner: Some(inner) })
    }

    fn inner_mut(&mut self) -> &mut TransactionInner<'a> {
        self.inner.as_mut().expect("transaction missing")
    }
}

/// A transaction started from a [`Pool`].
///
/// This holds one checked-out connection for the life of the transaction.
/// Dropping it before commit or rollback marks that connection as broken so it
/// will not be reused.
///
/// Use this when you want a transaction directly from the pool without first
/// calling [`Pool::acquire`].
pub struct PoolTransaction {
    conn: PooledConnection,
    finished: bool,
}

impl Transaction<'_> {
    /// Returns the selected driver.
    pub fn driver(&self) -> Driver {
        match self.inner.as_ref().expect("transaction missing") {
            #[cfg(feature = "mysql")]
            TransactionInner::Mysql(_) => Driver::Mysql,
            #[cfg(feature = "postgres")]
            TransactionInner::Postgres(_) => Driver::Pgsql,
            #[cfg(feature = "sqlite")]
            TransactionInner::Sqlite(_) => Driver::Sqlite,
            TransactionInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    /// Runs SQL inside the transaction.
    pub async fn query(&mut self, sql: &str) -> Result<Rows<'_>> {
        match self.inner_mut() {
            #[cfg(feature = "mysql")]
            TransactionInner::Mysql(tx) => Ok(Rows::mysql(tx.connection().query(sql).await?)),
            #[cfg(feature = "postgres")]
            TransactionInner::Postgres(tx) => Ok(Rows::postgres(tx.connection().query(sql).await?)),
            #[cfg(feature = "sqlite")]
            TransactionInner::Sqlite(tx) => Ok(Rows::sqlite(tx.connection().query(sql).await?)),
            TransactionInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    /// Prepares SQL inside the transaction.
    pub async fn prepare(&mut self, sql: &str) -> Result<Statement<'_>> {
        match self.inner_mut() {
            #[cfg(feature = "mysql")]
            TransactionInner::Mysql(tx) => {
                Ok(Statement::Mysql(tx.connection().prepare_cached(sql).await?))
            }
            #[cfg(feature = "postgres")]
            TransactionInner::Postgres(tx) => {
                let sql = crate::connection::rewrite_postgres_placeholders(sql);
                Ok(Statement::Postgres(
                    tx.connection().prepare_cached(&sql).await?,
                ))
            }
            #[cfg(feature = "sqlite")]
            TransactionInner::Sqlite(tx) => Ok(Statement::Sqlite(
                tx.connection().prepare_cached(sql).await?,
            )),
            TransactionInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    /// Commits the transaction.
    pub async fn commit(mut self) -> Result<()> {
        match self.inner.take().expect("transaction missing") {
            #[cfg(feature = "mysql")]
            TransactionInner::Mysql(tx) => tx.commit().await.map_err(Into::into),
            #[cfg(feature = "postgres")]
            TransactionInner::Postgres(tx) => tx.commit().await.map_err(Into::into),
            #[cfg(feature = "sqlite")]
            TransactionInner::Sqlite(tx) => tx.commit().await.map_err(Into::into),
            TransactionInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    /// Rolls the transaction back.
    pub async fn rollback(mut self) -> Result<()> {
        match self.inner.take().expect("transaction missing") {
            #[cfg(feature = "mysql")]
            TransactionInner::Mysql(tx) => tx.rollback().await.map_err(Into::into),
            #[cfg(feature = "postgres")]
            TransactionInner::Postgres(tx) => tx.rollback().await.map_err(Into::into),
            #[cfg(feature = "sqlite")]
            TransactionInner::Sqlite(tx) => tx.rollback().await.map_err(Into::into),
            TransactionInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }
}

impl PoolTransaction {
    /// Returns the selected driver.
    pub fn driver(&self) -> Driver {
        self.conn.driver()
    }

    /// Runs SQL inside the transaction.
    pub async fn query(&mut self, sql: &str) -> Result<Rows<'_>> {
        self.conn.query(sql).await
    }

    /// Prepares SQL inside the transaction.
    pub async fn prepare(&mut self, sql: &str) -> Result<PooledStatement<'_>> {
        self.conn.prepare(sql).await
    }

    /// Commits the transaction.
    pub async fn commit(mut self) -> Result<()> {
        self.finished = true;
        match commit_inner(&mut self.conn.conn_mut().inner).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.conn.mark_broken();
                Err(err)
            }
        }
    }

    /// Rolls the transaction back.
    pub async fn rollback(mut self) -> Result<()> {
        self.finished = true;
        match rollback_inner(&mut self.conn.conn_mut().inner).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.conn.mark_broken();
                Err(err)
            }
        }
    }
}

impl Executor for &mut Transaction<'_> {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;
    type Statement<'a>
        = Statement<'a>
    where
        Self: 'a;

    fn driver(&self) -> Driver {
        Transaction::driver(self)
    }

    async fn query(&mut self, sql: &str) -> Result<Self::Rows<'_>> {
        Transaction::query(*self, sql).await
    }

    async fn query_prepared_source<P>(&mut self, sql: &str, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut stmt = Transaction::prepare(*self, sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement<'_>> {
        Transaction::prepare(*self, sql).await
    }
}

impl Executor for Transaction<'_> {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;
    type Statement<'a>
        = Statement<'a>
    where
        Self: 'a;

    fn driver(&self) -> Driver {
        self.driver()
    }

    async fn query(&mut self, sql: &str) -> Result<Self::Rows<'_>> {
        Transaction::query(self, sql).await
    }

    async fn query_prepared_source<P>(&mut self, sql: &str, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut stmt = Transaction::prepare(self, sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement<'_>> {
        Transaction::prepare(self, sql).await
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        let _ = self.inner.take();
    }
}

impl Executor for &mut PoolTransaction {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;
    type Statement<'a>
        = PooledStatement<'a>
    where
        Self: 'a;

    fn driver(&self) -> Driver {
        PoolTransaction::driver(self)
    }

    async fn query(&mut self, sql: &str) -> Result<Self::Rows<'_>> {
        PoolTransaction::query(*self, sql).await
    }

    async fn query_prepared_source<P>(&mut self, sql: &str, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut stmt = PoolTransaction::prepare(*self, sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement<'_>> {
        PoolTransaction::prepare(*self, sql).await
    }
}

impl Executor for PoolTransaction {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;
    type Statement<'a>
        = PooledStatement<'a>
    where
        Self: 'a;

    fn driver(&self) -> Driver {
        self.driver()
    }

    async fn query(&mut self, sql: &str) -> Result<Self::Rows<'_>> {
        PoolTransaction::query(self, sql).await
    }

    async fn query_prepared_source<P>(&mut self, sql: &str, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut stmt = PoolTransaction::prepare(self, sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement<'_>> {
        PoolTransaction::prepare(self, sql).await
    }
}

impl Drop for PoolTransaction {
    fn drop(&mut self) {
        if !self.finished {
            self.conn.mark_broken();
        }
    }
}

/// A cloneable pool of database connections.
///
/// Use `Pool` when many tasks need database access at the same time. Cloning
/// the pool gives you another handle to the same shared pool state.
///
/// `Pool` is lazy by default. Building it does not open a connection or check
/// that the database is reachable unless [`PoolBuilder::min_connections`] asks
/// it to warm some connections up first. After that, [`Pool::acquire`],
/// [`Pool::query`], [`Pool::prepare`], and [`Pool::begin`] open a connection
/// only when the pool has no idle one ready to reuse.
///
/// The pool keeps up to `max_size` open connections. When that many
/// connections are already checked out, [`Pool::acquire`] waits until one is
/// returned. Idle connections stay in the pool and are reused by later
/// callers.
///
/// There is no background opener, health checker, or maintenance task. Idle
/// timeout and max lifetime limits are enforced lazily when a connection is
/// checked out or returned to the pool. Failed operations and unfinished pool
/// transactions mark that checked-out connection as broken so it will not be
/// reused.
///
/// Use [`Pool::with_hooks`] when the pool should run setup on fresh
/// connections or validate a connection before handing it out.
///
/// Use [`Connection`] when one long-lived connection is enough.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
    hooks: Option<Arc<Hooks>>,
}

struct PoolInner {
    driver: Driver,
    max_size: usize,
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<IdleConnection>>,
    options: Option<ConnectOptions>,
    acquire_timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
    max_lifetime: Option<Duration>,
}

struct IdleConnection {
    conn: ManagedConnection,
    created_at: Instant,
    idle_since: Instant,
    on_connect_ran: bool,
}

type HookFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + 'a>>;

trait OnConnectHook: Send + Sync {
    fn call<'a>(&'a self, conn: &'a mut PooledConnection) -> HookFuture<'a, ()>;
}

impl<F> OnConnectHook for F
where
    F: Send + Sync + 'static + for<'a> AsyncFn(&'a mut PooledConnection) -> Result<()>,
{
    fn call<'a>(&'a self, conn: &'a mut PooledConnection) -> HookFuture<'a, ()> {
        Box::pin(self(conn))
    }
}

trait BeforeAcquireHook: Send + Sync {
    fn call<'a>(&'a self, conn: &'a mut PooledConnection) -> HookFuture<'a, AcquireDecision>;
}

impl<F> BeforeAcquireHook for F
where
    F: Send + Sync + 'static + for<'a> AsyncFn(&'a mut PooledConnection) -> Result<AcquireDecision>,
{
    fn call<'a>(&'a self, conn: &'a mut PooledConnection) -> HookFuture<'a, AcquireDecision> {
        Box::pin(self(conn))
    }
}

/// Decides what the pool should do with a connection after `before_acquire`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireDecision {
    /// Hand the connection to the caller.
    Accept,
    /// Drop this connection and try another one.
    Retry,
}

/// Optional pool hooks.
///
/// Use this with [`Pool::with_hooks`] when you want connection setup or
/// validation around pool checkout.
///
/// This stays empty by default:
///
/// ```rust
/// # let hooks =
/// quex::Hooks::new();
/// # let _ = hooks;
/// ```
///
/// A more typical setup looks like this:
///
/// ```rust
/// # let hooks =
/// quex::Hooks::new()
///     .on_connect(async |conn| {
///         quex::query("create table if not exists users(id integer primary key)")
///             .execute(conn)
///             .await?;
///         Ok(())
///     })
///     .before_acquire(async |conn| {
///         conn.query("select 1").await?;
///         Ok(quex::AcquireDecision::Accept)
///     });
/// # let _ = hooks;
/// ```
#[derive(Default)]
pub struct Hooks {
    on_connect: Option<Arc<dyn OnConnectHook>>,
    before_acquire: Option<Arc<dyn BeforeAcquireHook>>,
}

impl Hooks {
    /// Creates an empty hook set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Runs after the pool opens a fresh connection.
    ///
    /// Use this for setup that should happen once per new connection.
    ///
    /// This does not run when the pool reuses an idle connection.
    pub fn on_connect<F>(mut self, hook: F) -> Self
    where
        F: Send + Sync + 'static + for<'a> AsyncFn(&'a mut PooledConnection) -> Result<()>,
    {
        self.on_connect = Some(Arc::new(hook));
        self
    }

    /// Runs before the pool returns a checked-out connection.
    ///
    /// Use this to validate the connection and decide whether the pool should
    /// hand it out or discard it and try again.
    ///
    /// Return [`AcquireDecision::Accept`] to hand the connection to the
    /// caller, or [`AcquireDecision::Retry`] to discard this candidate and let
    /// the pool try another one.
    pub fn before_acquire<F>(mut self, hook: F) -> Self
    where
        F: Send + Sync + 'static + for<'a> AsyncFn(&'a mut PooledConnection) -> Result<AcquireDecision>,
    {
        self.before_acquire = Some(Arc::new(hook));
        self
    }
}

impl Pool {
    /// Starts building a pool for the given connection options.
    pub fn connect(options: impl IntoConnectOptions) -> Result<PoolBuilder> {
        PoolBuilder::new().connect(options)
    }

    /// Returns a pool handle with hooks enabled.
    ///
    /// Use [`Hooks`] when new connections need setup work or checked-out
    /// connections need validation before they are returned.
    pub fn with_hooks(mut self, hooks: Hooks) -> Self {
        self.hooks = Some(Arc::new(hooks));
        self
    }

    /// Acquires one connection from the pool.
    ///
    /// This first tries to reuse an idle connection. If none is available and
    /// the pool is still below `max_size`, it opens a new one from the stored
    /// connection options.
    ///
    /// When all pooled connections are already checked out, this waits until
    /// one is returned.
    ///
    /// If hooks are configured, fresh connections run [`Hooks::on_connect`]
    /// and every candidate runs [`Hooks::before_acquire`] before this returns.
    pub async fn acquire(&self) -> Result<PooledConnection> {
        let deadline = self.inner.acquire_timeout.map(|timeout| Instant::now() + timeout);
        let permit = self.acquire_permit(deadline).await?;
        self.checkout(permit, deadline).await
    }

    /// Tries to acquire one connection from the pool without waiting.
    ///
    /// This follows the same reuse rules as [`Pool::acquire`], but returns
    /// [`Error::PoolExhausted`] immediately when all pooled connections are
    /// already checked out.
    ///
    /// Once the pool has a candidate connection, configured hooks still run
    /// before it is returned.
    pub async fn try_acquire(&self) -> Result<PooledConnection> {
        let permit = self
            .inner
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|err| match err {
                TryAcquireError::NoPermits => Error::PoolExhausted,
                TryAcquireError::Closed => Error::PoolClosed,
            })?;

        self.checkout(permit, None).await
    }

    async fn acquire_permit(&self, deadline: Option<Instant>) -> Result<OwnedSemaphorePermit> {
        match deadline {
            Some(deadline) => timeout(
                self.remaining_timeout(deadline)?,
                self.inner.permits.clone().acquire_owned(),
            )
            .await
            .map_err(|_| Error::PoolTimedOut)?
            .map_err(|_| Error::PoolClosed),
            None => self
                .inner
                .permits
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Error::PoolClosed),
        }
    }

    fn remaining_timeout(&self, deadline: Instant) -> Result<Duration> {
        deadline
            .checked_duration_since(Instant::now())
            .ok_or(Error::PoolTimedOut)
    }

    async fn checkout(
        &self,
        permit: OwnedSemaphorePermit,
        deadline: Option<Instant>,
    ) -> Result<PooledConnection> {
        let mut permit = Some(permit);
        loop {
            let (conn, created_at, on_connect_ran, fresh) = match self.take_idle_connection() {
                Some(entry) => (entry.conn, entry.created_at, entry.on_connect_ran, false),
                None => {
                    let options = self.inner.options.clone().ok_or_else(|| {
                        Error::Unsupported("pool cannot reopen this connection".into())
                    })?;
                    let conn = match deadline {
                        Some(deadline) => timeout(
                            self.remaining_timeout(deadline)?,
                            connect_managed(options),
                        )
                        .await
                        .map_err(|_| Error::PoolTimedOut)??,
                        None => connect_managed(options).await?,
                    };
                    (conn, Instant::now(), false, true)
                }
            };

            let mut pooled = PooledConnection {
                pool: Arc::clone(&self.inner),
                permit: Some(permit.take().expect("pool permit missing")),
                conn: Some(conn),
                reusable: true,
                created_at,
                on_connect_ran,
            };

            if !pooled.on_connect_ran {
                if let Some(hook) = self.hooks.as_ref().and_then(|hooks| hooks.on_connect.as_ref()) {
                    let result = match deadline {
                        Some(deadline) => timeout(
                            self.remaining_timeout(deadline)?,
                            hook.call(&mut pooled),
                        )
                        .await
                        .map_err(|_| Error::PoolTimedOut)?,
                        None => hook.call(&mut pooled).await,
                    };
                    if let Err(err) = result {
                        pooled.mark_broken();
                        return Err(err);
                    }
                    pooled.on_connect_ran = true;
                }
            }

            if let Some(hook) = self
                .hooks
                .as_ref()
                .and_then(|hooks| hooks.before_acquire.as_ref())
            {
                let decision = match deadline {
                    Some(deadline) => timeout(
                        self.remaining_timeout(deadline)?,
                        hook.call(&mut pooled),
                    )
                    .await
                    .map_err(|_| Error::PoolTimedOut)?,
                    None => hook.call(&mut pooled).await,
                };
                match decision {
                    Ok(AcquireDecision::Accept) => return Ok(pooled),
                    Ok(AcquireDecision::Retry) => {
                        pooled.mark_broken();
                        if fresh && deadline.is_none() {
                            return Err(Error::Unsupported(
                                "before_acquire rejected a fresh connection".into(),
                            ));
                        }
                        permit = Some(pooled.permit.take().expect("pool permit missing"));
                        drop(pooled);
                        continue;
                    }
                    Err(err) => {
                        pooled.mark_broken();
                        return Err(err);
                    }
                }
            }

            return Ok(pooled);
        }
    }

    fn take_idle_connection(&self) -> Option<IdleConnection> {
        let now = Instant::now();
        let mut idle = self.inner.idle.lock().expect("pool mutex poisoned");
        while let Some(entry) = idle.pop() {
            if self
                .inner
                .idle_timeout
                .is_some_and(|limit| now.duration_since(entry.idle_since) >= limit)
            {
                continue;
            }
            if self
                .inner
                .max_lifetime
                .is_some_and(|limit| now.duration_since(entry.created_at) >= limit)
            {
                continue;
            }
            return Some(entry);
        }
        None
    }

    /// Returns the maximum number of open connections.
    pub fn max_size(&self) -> usize {
        self.inner.max_size
    }

    /// Returns the selected driver.
    pub fn driver(&self) -> Driver {
        self.inner.driver
    }

    /// Returns the number of idle connections currently kept by the pool.
    pub fn idle_count(&self) -> usize {
        self.inner.idle.lock().expect("pool mutex poisoned").len()
    }

    /// Returns the number of connections that can be acquired without waiting.
    pub fn available_permits(&self) -> usize {
        self.inner.permits.available_permits()
    }

    /// Returns whether the pool has been closed.
    pub fn is_closed(&self) -> bool {
        self.inner.permits.is_closed()
    }

    /// Closes the pool to new acquire and begin calls.
    ///
    /// Idle connections are dropped immediately. Checked-out connections stay
    /// usable until they are dropped, and are discarded instead of being
    /// returned to the pool.
    pub fn close(&self) {
        self.inner.permits.close();
        self.inner.idle.lock().expect("pool mutex poisoned").clear();
    }

    /// Acquires a connection, runs SQL, and returns rows.
    pub async fn query(&self, sql: &str) -> Result<Rows<'_>> {
        let mut conn = self.acquire().await?;
        let rows = conn.query(sql).await?;
        Ok(rows.into_lifetime())
    }

    /// Creates a pool-owned prepared statement handle.
    ///
    /// Each execution acquires a connection and prepares the SQL on that
    /// connection.
    pub async fn prepare(&self, sql: &str) -> Result<PoolStatement> {
        Ok(PoolStatement::new(self.clone(), sql))
    }

    /// Starts a transaction on a checked-out connection.
    ///
    /// This is a convenience for calling [`Pool::acquire`] and then
    /// [`PooledConnection::begin`]. The transaction keeps that connection
    /// checked out until commit, rollback, or drop.
    pub async fn begin(&self) -> Result<PoolTransaction> {
        let mut conn = self.acquire().await?;
        start_transaction_inner(&mut conn.conn_mut().inner).await?;
        Ok(PoolTransaction {
            conn,
            finished: false,
        })
    }

    /// Tries to start a transaction without waiting for a connection.
    ///
    /// This is the non-blocking counterpart to [`Pool::begin`]. It returns
    /// [`Error::PoolExhausted`] immediately when all pooled connections are
    /// already checked out.
    pub async fn try_begin(&self) -> Result<PoolTransaction> {
        let mut conn = self.try_acquire().await?;
        start_transaction_inner(&mut conn.conn_mut().inner).await?;
        Ok(PoolTransaction {
            conn,
            finished: false,
        })
    }
}

impl Executor for &Pool {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;
    type Statement<'a>
        = PoolStatement
    where
        Self: 'a;

    fn driver(&self) -> Driver {
        Pool::driver(self)
    }

    async fn query(&mut self, sql: &str) -> Result<Self::Rows<'_>> {
        Pool::query(self, sql).await
    }

    async fn query_prepared_source<P>(&mut self, sql: &str, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut stmt = Pool::prepare(self, sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement<'_>> {
        Pool::prepare(self, sql).await
    }
}

impl Executor for &mut Pool {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;
    type Statement<'a>
        = PoolStatement
    where
        Self: 'a;

    fn driver(&self) -> Driver {
        Pool::driver(self)
    }

    async fn query(&mut self, sql: &str) -> Result<Self::Rows<'_>> {
        Pool::query(self, sql).await
    }

    async fn query_prepared_source<P>(&mut self, sql: &str, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut stmt = Pool::prepare(self, sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement<'_>> {
        Pool::prepare(self, sql).await
    }
}

impl Executor for Pool {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;
    type Statement<'a>
        = PoolStatement
    where
        Self: 'a;

    fn driver(&self) -> Driver {
        self.driver()
    }

    async fn query(&mut self, sql: &str) -> Result<Self::Rows<'_>> {
        Pool::query(self, sql).await
    }

    async fn query_prepared_source<P>(&mut self, sql: &str, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut stmt = Pool::prepare(self, sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement<'_>> {
        Pool::prepare(self, sql).await
    }
}

/// Builder for [`Pool`].
///
/// Start with [`Pool::connect`], adjust any settings you need, then call
/// [`PoolBuilder::build`].
pub struct PoolBuilder {
    max_size: usize,
    min_connections: usize,
    acquire_timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
    max_lifetime: Option<Duration>,
    options: Option<ConnectOptions>,
}

impl Default for PoolBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolBuilder {
    /// Creates a builder with a default maximum size of 10.
    pub fn new() -> Self {
        Self {
            max_size: 10,
            min_connections: 0,
            acquire_timeout: None,
            idle_timeout: None,
            max_lifetime: None,
            options: None,
        }
    }

    /// Sets the maximum number of open connections.
    ///
    /// `0` is rejected by [`Self::build`].
    pub fn max_size(mut self, value: usize) -> Self {
        self.max_size = value;
        self
    }

    /// Sets how many connections to open before the pool is returned.
    ///
    /// The default is `0`, which keeps pool creation lazy.
    pub fn min_connections(mut self, value: usize) -> Self {
        self.min_connections = value;
        self
    }

    /// Sets how long [`Pool::acquire`] and [`Pool::begin`] may wait.
    ///
    /// When the timeout is reached, they return [`Error::PoolTimedOut`].
    ///
    /// The default is no timeout.
    pub fn acquire_timeout(mut self, value: Duration) -> Self {
        self.acquire_timeout = Some(value);
        self
    }

    /// Sets how long an unused idle connection may stay in the pool.
    ///
    /// Expired idle connections are dropped lazily on the next checkout or
    /// return. The default is no idle timeout.
    pub fn idle_timeout(mut self, value: Duration) -> Self {
        self.idle_timeout = Some(value);
        self
    }

    /// Sets the maximum lifetime of a pooled connection.
    ///
    /// Expired connections are dropped lazily on the next checkout or return.
    /// The default is no lifetime limit.
    pub fn max_lifetime(mut self, value: Duration) -> Self {
        self.max_lifetime = Some(value);
        self
    }

    /// Stores connection options.
    ///
    /// Strings are parsed as database URLs.
    pub fn connect(mut self, url: impl IntoConnectOptions) -> Result<Self> {
        self.options = Some(url.into_connect_options()?);
        Ok(self)
    }

    /// Builds a pool.
    ///
    /// When `min_connections` is `0`, this only stores the configuration and
    /// stays lazy. When it is greater than `0`, this opens that many
    /// connections before returning.
    pub async fn build(self) -> Result<Pool> {
        if self.max_size == 0 {
            return Err(Error::Unsupported(
                "pool max_size must be greater than zero".into(),
            ));
        }
        if self.min_connections > self.max_size {
            return Err(Error::Unsupported(
                "pool min_connections cannot be greater than max_size".into(),
            ));
        }

        let options = self
            .options
            .ok_or_else(|| Error::Unsupported("pool requires connection options".into()))?;
        let driver = options
            .driver
            .ok_or_else(|| Error::Unsupported("pool requires a driver".into()))?;

        let now = Instant::now();
        let mut idle = Vec::with_capacity(self.min_connections);
        for _ in 0..self.min_connections {
            idle.push(IdleConnection {
                conn: connect_managed(options.clone()).await?,
                created_at: now,
                idle_since: now,
                on_connect_ran: false,
            });
        }

        Ok(Pool {
            inner: Arc::new(PoolInner {
                driver,
                max_size: self.max_size,
                permits: Arc::new(Semaphore::new(self.max_size)),
                idle: Mutex::new(idle),
                options: Some(options),
                acquire_timeout: self.acquire_timeout,
                idle_timeout: self.idle_timeout,
                max_lifetime: self.max_lifetime,
            }),
            hooks: None,
        })
    }
}

/// A connection checked out from a [`Pool`].
///
/// The connection returns to the pool on drop unless an operation failed or a
/// transaction was dropped without being finished.
///
/// Keep this when you need one connection for several operations or want to
/// prepare a statement that stays on the same connection.
pub struct PooledConnection {
    pool: Arc<PoolInner>,
    permit: Option<OwnedSemaphorePermit>,
    conn: Option<ManagedConnection>,
    reusable: bool,
    created_at: Instant,
    on_connect_ran: bool,
}

impl PooledConnection {
    fn conn_mut(&mut self) -> &mut ManagedConnection {
        self.conn.as_mut().expect("pooled connection missing")
    }

    fn mark_broken(&mut self) {
        self.reusable = false;
    }

    /// Returns the selected driver.
    pub fn driver(&self) -> Driver {
        self.conn
            .as_ref()
            .expect("pooled connection missing")
            .driver
    }

    /// Runs SQL on this checked-out connection.
    pub async fn query(&mut self, sql: &str) -> Result<Rows<'_>> {
        let (conn, reusable) = (&mut self.conn, &mut self.reusable);
        match query_inner(
            &mut conn.as_mut().expect("pooled connection missing").inner,
            sql,
        )
        .await
        {
            Ok(rows) => Ok(rows),
            Err(err) => {
                *reusable = false;
                Err(err)
            }
        }
    }

    /// Prepares SQL on this checked-out connection.
    pub async fn prepare(&mut self, sql: &str) -> Result<PooledStatement<'_>> {
        let (conn, reusable) = (&mut self.conn, &mut self.reusable);
        match prepare_inner(
            &mut conn.as_mut().expect("pooled connection missing").inner,
            sql,
        )
        .await
        {
            Ok(stmt) => Ok(PooledStatement { stmt, reusable }),
            Err(err) => {
                *reusable = false;
                Err(err)
            }
        }
    }

    /// Starts a transaction on this checked-out connection.
    ///
    /// The connection is kept until the transaction is committed, rolled back,
    /// or dropped.
    pub async fn begin(&mut self) -> Result<PooledTransaction<'_>> {
        let (conn, reusable) = (&mut self.conn, &mut self.reusable);
        match start_transaction_inner(&mut conn.as_mut().expect("pooled connection missing").inner)
            .await
        {
            Ok(()) => Ok(PooledTransaction {
                pooled: self,
                finished: false,
            }),
            Err(err) => {
                *reusable = false;
                Err(err)
            }
        }
    }
}

/// A statement that belongs to a pool.
///
/// Each execution acquires a connection from the pool. Use
/// [`PooledConnection::prepare`] when repeated executions must stay on the same
/// connection.
///
/// This is a good fit when the same SQL runs more than once but does not need
/// to stay pinned to one checked-out connection between calls.
pub struct PoolStatement {
    pool: Pool,
    sql: String,
}

impl PoolStatement {
    fn new(pool: Pool, sql: &str) -> Self {
        Self {
            pool,
            sql: sql.into(),
        }
    }

    /// Starts binding parameters for this statement.
    pub fn bind<T>(&mut self, value: T) -> BoundStatement<'_, Self>
    where
        T: Encode,
    {
        BoundStatement::new(self).bind(value)
    }

    /// Runs the statement and returns rows.
    pub async fn execute_source<P>(&mut self, params: &P) -> Result<Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut conn = self.pool.acquire().await?;
        let mut stmt = prepare_inner(&mut conn.conn_mut().inner, &self.sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    /// Runs the statement when no rows are expected.
    pub async fn exec_source<P>(&mut self, params: &P) -> Result<ExecResult>
    where
        P: ParamSource + ?Sized,
    {
        let mut conn = self.pool.acquire().await?;
        let mut stmt = prepare_inner(&mut conn.conn_mut().inner, &self.sql).await?;
        stmt.exec_source(params).await
    }
}

impl PreparedStatement for PoolStatement {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;

    async fn execute_source<P>(&mut self, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        PoolStatement::execute_source(self, params).await
    }

    async fn exec_source<P>(&mut self, params: &P) -> Result<ExecResult>
    where
        P: ParamSource + ?Sized,
    {
        PoolStatement::exec_source(self, params).await
    }
}

impl Executor for &mut PooledConnection {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;
    type Statement<'a>
        = PooledStatement<'a>
    where
        Self: 'a;

    fn driver(&self) -> Driver {
        PooledConnection::driver(self)
    }

    async fn query(&mut self, sql: &str) -> Result<Self::Rows<'_>> {
        PooledConnection::query(*self, sql).await
    }

    async fn query_prepared_source<P>(&mut self, sql: &str, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut stmt = PooledConnection::prepare(*self, sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement<'_>> {
        PooledConnection::prepare(*self, sql).await
    }
}

impl Executor for PooledConnection {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;
    type Statement<'a>
        = PooledStatement<'a>
    where
        Self: 'a;

    fn driver(&self) -> Driver {
        self.driver()
    }

    async fn query(&mut self, sql: &str) -> Result<Self::Rows<'_>> {
        PooledConnection::query(self, sql).await
    }

    async fn query_prepared_source<P>(&mut self, sql: &str, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut stmt = PooledConnection::prepare(self, sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement<'_>> {
        PooledConnection::prepare(self, sql).await
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        let conn = self.conn.take();
        if let Some(conn) = conn {
            let now = Instant::now();
            let expired_by_lifetime = self
                .pool
                .max_lifetime
                .is_some_and(|limit| now.duration_since(self.created_at) >= limit);
            if self.reusable && !self.pool.permits.is_closed() && !expired_by_lifetime {
                self.pool
                    .idle
                    .lock()
                    .expect("pool mutex poisoned")
                    .push(IdleConnection {
                        conn,
                        created_at: self.created_at,
                        idle_since: now,
                        on_connect_ran: self.on_connect_ran,
                    });
            }
        }
        self.permit.take();
    }
}

/// A transaction borrowed from a [`PooledConnection`].
///
/// Dropping it without commit or rollback marks the connection as broken.
///
/// While this value exists, the underlying [`PooledConnection`] stays borrowed.
pub struct PooledTransaction<'a> {
    pooled: &'a mut PooledConnection,
    finished: bool,
}

impl<'a> PooledTransaction<'a> {
    /// Returns the selected driver.
    pub fn driver(&self) -> Driver {
        self.pooled.driver()
    }

    /// Runs SQL inside the transaction.
    pub async fn query(&mut self, sql: &str) -> Result<Rows<'_>> {
        self.pooled.query(sql).await
    }

    /// Prepares SQL inside the transaction.
    pub async fn prepare(&mut self, sql: &str) -> Result<PooledStatement<'_>> {
        self.pooled.prepare(sql).await
    }

    /// Commits the transaction.
    pub async fn commit(mut self) -> Result<()> {
        self.finished = true;
        match commit_inner(&mut self.pooled.conn_mut().inner).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.pooled.mark_broken();
                Err(err)
            }
        }
    }

    /// Rolls the transaction back.
    pub async fn rollback(mut self) -> Result<()> {
        self.finished = true;
        match rollback_inner(&mut self.pooled.conn_mut().inner).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.pooled.mark_broken();
                Err(err)
            }
        }
    }

    /// Returns the underlying checked-out connection.
    pub fn connection(&mut self) -> &mut PooledConnection {
        self.pooled
    }
}

impl Executor for &mut PooledTransaction<'_> {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;
    type Statement<'a>
        = PooledStatement<'a>
    where
        Self: 'a;

    fn driver(&self) -> Driver {
        PooledTransaction::driver(self)
    }

    async fn query(&mut self, sql: &str) -> Result<Self::Rows<'_>> {
        PooledTransaction::query(*self, sql).await
    }

    async fn query_prepared_source<P>(&mut self, sql: &str, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut stmt = PooledTransaction::prepare(*self, sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement<'_>> {
        PooledTransaction::prepare(*self, sql).await
    }
}

impl Executor for PooledTransaction<'_> {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;
    type Statement<'a>
        = PooledStatement<'a>
    where
        Self: 'a;

    fn driver(&self) -> Driver {
        self.driver()
    }

    async fn query(&mut self, sql: &str) -> Result<Self::Rows<'_>> {
        PooledTransaction::query(self, sql).await
    }

    async fn query_prepared_source<P>(&mut self, sql: &str, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        let mut stmt = PooledTransaction::prepare(self, sql).await?;
        let rows = stmt.execute_source(params).await?;
        Ok(rows.into_lifetime())
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement<'_>> {
        PooledTransaction::prepare(self, sql).await
    }
}

impl Drop for PooledTransaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.pooled.mark_broken();
        }
    }
}

/// A statement prepared on a checked-out connection.
///
/// The statement stays tied to that connection for as long as it is borrowed.
pub struct PooledStatement<'a> {
    stmt: Statement<'a>,
    reusable: &'a mut bool,
}

impl PooledStatement<'_> {
    /// Starts binding parameters for this statement.
    pub fn bind<T>(&mut self, value: T) -> BoundStatement<'_, Self>
    where
        T: Encode,
    {
        BoundStatement::new(self).bind(value)
    }

    /// Runs the statement and returns rows.
    pub async fn execute_source<P>(&mut self, params: &P) -> Result<Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        match self.stmt.execute_source(params).await {
            Ok(rows) => Ok(rows),
            Err(err) => {
                *self.reusable = false;
                Err(err)
            }
        }
    }

    /// Runs the statement when no rows are expected.
    pub async fn exec_source<P>(&mut self, params: &P) -> Result<ExecResult>
    where
        P: ParamSource + ?Sized,
    {
        match self.stmt.exec_source(params).await {
            Ok(result) => Ok(result),
            Err(err) => {
                *self.reusable = false;
                Err(err)
            }
        }
    }
}

impl PreparedStatement for PooledStatement<'_> {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;

    async fn execute_source<P>(&mut self, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        PooledStatement::execute_source(self, params).await
    }

    async fn exec_source<P>(&mut self, params: &P) -> Result<ExecResult>
    where
        P: ParamSource + ?Sized,
    {
        PooledStatement::exec_source(self, params).await
    }
}
