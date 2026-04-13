use std::future::Future;

use crate::data::{Date, DateTime, DateTimeTz, EncodeTarget, Encoder, Time};
use crate::rows::{collect_decoded_rows, decode_one_rows, decode_optional_rows};
use crate::{Column, Driver, Encode, ExecResult, FromRow, ParamValue, Result, RowRef};

/// A borrowed SQL parameter value.
///
/// This is the low-level view used by [`ParamSource`].
///
/// Most code does not need to construct `ParamRef` directly. It is mainly for
/// custom [`ParamSource`] implementations that already have values stored in
/// their own format and want to expose borrowed parameters without first
/// copying them into [`Params`].
#[derive(Debug, Clone, Copy)]
pub enum ParamRef<'a> {
    /// A SQL `null`.
    Null,
    /// A signed integer.
    I64(&'a i64),
    /// An unsigned integer.
    U64(&'a u64),
    /// A floating-point value.
    F64(&'a f64),
    /// A date.
    Date(&'a Date),
    /// A time of day.
    Time(&'a Time),
    /// A date and time without an offset.
    DateTime(&'a DateTime),
    /// A date and time with an offset.
    DateTimeTz(&'a DateTimeTz),
    /// A 16-byte uuid.
    Uuid(&'a [u8; 16]),
    /// Text.
    Str(&'a str),
    /// Bytes.
    Bytes(&'a [u8]),
}

/// A borrowed source of positional SQL parameters.
///
/// Implement this when your parameters already live in your own storage and you
/// want to avoid copying them into [`Params`].
///
/// This trait is for advanced use. Most callers should use [`Query::bind`],
/// [`PreparedStatement::bind`], or [`Params`] instead.
///
/// Implementors must return the same number of parameters from [`Self::len`]
/// for the whole call, and [`Self::value_at`] must succeed for every index in
/// `0..len()`. The values returned by [`Self::value_at`] only need to live for
/// the duration of that call.
pub trait ParamSource {
    /// Number of parameters in the source.
    fn len(&self) -> usize;

    /// Returns whether the source has no parameters.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the parameter at `index`.
    fn value_at(&self, index: usize) -> ParamRef<'_>;
}

/// A prepared statement that can be run with borrowed parameters.
///
/// You usually use this trait through [`prepare`], [`Executor::prepare`], or
/// concrete types like [`crate::Statement`] and [`crate::PooledStatement`].
/// Implementing it is mostly for executor-like wrappers inside the crate or in
/// extension code.
///
/// `Rows<'a>` is the row stream type returned when the statement is executed
/// while borrowed for `'a`. Some statements return rows tied to the borrowed
/// statement or connection. Others may return an owned stream internally and
/// still expose it through this associated type.
pub trait PreparedStatement {
    /// Row stream returned by this statement.
    type Rows<'a>: RowStream + 'a
    where
        Self: 'a;

    /// Runs the statement and returns rows.
    ///
    /// `params` must match the SQL placeholder count and order expected by the
    /// underlying statement.
    fn execute_source<P>(&mut self, params: &P) -> impl Future<Output = Result<Self::Rows<'_>>>
    where
        P: ParamSource + ?Sized;

    /// Runs the statement when no rows are expected.
    fn exec_source<P>(&mut self, params: &P) -> impl Future<Output = Result<ExecResult>>
    where
        P: ParamSource + ?Sized;

    /// Starts a bound-statement builder.
    ///
    /// This is the usual way to build up parameters one value at a time before
    /// calling [`BoundStatement::execute`] or [`BoundStatement::exec`].
    fn bind<T>(&mut self, value: T) -> BoundStatement<'_, Self>
    where
        Self: Sized,
        T: Encode,
    {
        BoundStatement::new(self).bind(value)
    }
}

/// A forward-only stream of result rows.
///
/// This trait is what query helpers work with after a statement has been run.
/// Most code uses it through [`Rows`] or through methods like [`Query::fetch`].
///
/// Row streams are single-pass. Once a row is consumed, the stream does not go
/// back. Borrowed rows returned from [`Self::next`] stay valid only until the
/// next call to `next`.
pub trait RowStream {
    /// Returns column metadata for this stream.
    fn columns(&self) -> &[Column];

    /// Advances to the next row.
    ///
    /// The returned row is borrowed from the stream and is valid until the
    /// stream advances again.
    fn next(&mut self) -> impl Future<Output = Result<Option<RowRef<'_>>>>;
}

/// Something that can run SQL.
///
/// This is the trait behind [`query`] and [`prepare`]. [`Connection`], [`Pool`],
/// [`crate::PoolTransaction`], [`crate::PooledConnection`],
/// [`crate::PooledTransaction`], and [`crate::Transaction`] all implement it.
///
/// Use this trait when you want generic code that can work with either a plain
/// connection, a pool, or a transaction.
///
/// `Rows<'a>` is the stream type returned while the executor is borrowed for
/// `'a`. `Statement<'a>` is the prepared statement type returned from
/// [`Self::prepare`].
pub trait Executor {
    /// Row stream returned by this executor.
    type Rows<'a>: RowStream + 'a
    where
        Self: 'a;
    /// Prepared statement returned by this executor.
    type Statement<'a>: PreparedStatement<Rows<'a> = Self::Rows<'a>> + 'a
    where
        Self: 'a;

    /// Returns the selected database driver.
    fn driver(&self) -> Driver;

    /// Runs SQL without explicit bound parameters.
    ///
    /// For SQL with parameters, most code should use [`query`] or
    /// [`Self::prepare`] instead of calling [`Self::query_prepared_source`]
    /// directly.
    fn query(&mut self, sql: &str) -> impl Future<Output = Result<Self::Rows<'_>>>;

    /// Runs SQL with a borrowed parameter source.
    ///
    /// This is the low-level path used by [`Query`] when parameters were bound.
    /// Most callers should not need to use it directly.
    fn query_prepared_source<P>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> impl Future<Output = Result<Self::Rows<'_>>>
    where
        P: ParamSource + ?Sized;

    /// Prepares SQL for repeated use.
    ///
    /// Reuse this when the same SQL runs many times on the same executor.
    fn prepare(&mut self, sql: &str) -> impl Future<Output = Result<Self::Statement<'_>>>;
}

/// A query with optional bound parameters.
///
/// Write placeholders as `?` for every supported driver. postgres statements
/// are rewritten to the `$1`, `$2`, ... placeholder form before they reach
/// libpq.
pub struct Query<'a> {
    sql: &'a str,
    params: Params,
}

#[derive(Debug, Clone)]
enum ParamSlot {
    Null,
    I64(i64),
    U64(u64),
    F64(f64),
    Date(Date),
    Time(Time),
    DateTime(DateTime),
    DateTimeTz(DateTimeTz),
    Uuid([u8; 16]),
    Str { start: usize, len: usize },
    Bytes { start: usize, len: usize },
}

/// A request to prepare SQL against an executor.
///
/// This is the prepared-statement counterpart to [`Query`]. Build it with
/// [`prepare`] and then call [`Prepare::run`].
pub struct Prepare<'a> {
    sql: &'a str,
}

/// An owned list of positional parameters.
///
/// `Params` stores text and bytes in an internal arena, so values passed to
/// [`Params::bind`] do not need to live as long as the query execution.
///
/// Use this when you want to build a parameter list separately from the query
/// or statement that will run it.
#[derive(Debug, Clone, Default)]
pub struct Params {
    params: Vec<ParamSlot>,
    arena: Vec<u8>,
}

/// A prepared statement plus parameters collected through `.bind(...)`.
///
/// You usually get this from [`PreparedStatement::bind`] or
/// [`prepare(...).run(...).await?.bind(...)`].
pub struct BoundStatement<'s, S: PreparedStatement + ?Sized> {
    stmt: &'s mut S,
    params: Params,
}

/// Starts a query.
///
/// ```
/// # async fn run(db: &mut quex::Connection) -> quex::Result<()> {
/// quex::query("insert into users(name) values(?)")
///     .bind("ada")
///     .execute(db)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub fn query(sql: &str) -> Query<'_> {
    Query {
        sql,
        params: Params::new(),
    }
}

/// Starts a prepared-statement builder.
///
/// Use this when the same SQL will run more than once on the same executor.
pub fn prepare(sql: &str) -> Prepare<'_> {
    Prepare { sql }
}

impl<'a> Query<'a> {
    /// Adds one positional parameter.
    ///
    /// Parameters are bound in the order they appear in the SQL.
    pub fn bind<T>(mut self, value: T) -> Self
    where
        T: Encode,
    {
        self.params.push(value);
        self
    }

    /// Runs the query and returns a row stream.
    ///
    /// The stream must be consumed before the borrowed executor can be used
    /// again if the returned rows borrow from it.
    pub async fn fetch<E: Executor>(self, exec: &mut E) -> Result<E::Rows<'_>> {
        if self.params.is_empty() {
            exec.query(self.sql).await
        } else {
            exec.query_prepared_source(self.sql, &self).await
        }
    }

    /// Returns the first decoded row.
    ///
    /// This errors if the query returns no rows. Extra rows are ignored.
    pub async fn one<T>(self, mut exec: impl Executor) -> Result<T>
    where
        T: FromRow,
    {
        let mut rows = if self.params.is_empty() {
            exec.query(self.sql).await?
        } else {
            exec.query_prepared_source(self.sql, &self).await?
        };
        decode_one_rows(&mut rows).await
    }

    /// Returns zero or one decoded row.
    ///
    /// This returns `Ok(None)` when the query returns no rows. Extra rows are
    /// ignored.
    pub async fn optional<T>(self, mut exec: impl Executor) -> Result<Option<T>>
    where
        T: FromRow,
    {
        let mut rows = if self.params.is_empty() {
            exec.query(self.sql).await?
        } else {
            exec.query_prepared_source(self.sql, &self).await?
        };
        decode_optional_rows(&mut rows).await
    }

    /// Collects every row and decodes it.
    pub async fn all<T>(self, mut exec: impl Executor) -> Result<Vec<T>>
    where
        T: FromRow,
    {
        let rows = if self.params.is_empty() {
            exec.query(self.sql).await?
        } else {
            exec.query_prepared_source(self.sql, &self).await?
        };
        collect_decoded_rows(rows).await
    }

    /// Executes the query when no rows are expected.
    ///
    /// Use [`Self::fetch`] when the statement returns rows.
    pub async fn execute(self, mut exec: impl Executor) -> Result<ExecResult> {
        let mut stmt = exec.prepare(self.sql).await?;
        stmt.exec_source(&self).await
    }
}

impl ParamSource for Query<'_> {
    fn len(&self) -> usize {
        self.params.len()
    }

    fn value_at(&self, index: usize) -> ParamRef<'_> {
        self.params.value_at(index)
    }
}

impl Params {
    /// Creates an empty parameter list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one parameter and returns the updated list.
    pub fn bind<T>(mut self, value: T) -> Self
    where
        T: Encode,
    {
        self.push(value);
        self
    }

    /// Returns whether the list is empty.
    pub fn is_empty(&self) -> bool {
        self.params.is_empty()
    }

    fn len(&self) -> usize {
        self.params.len()
    }

    fn push<T>(&mut self, value: T)
    where
        T: Encode,
    {
        let before = self.params.len();
        let mut target = ParamsEncoder { params: self };
        let out = Encoder::new(&mut target);
        value.encode(out);
        let written = target.params.len() - before;
        match written {
            1 => {}
            0 => {
                panic!(
                    "Encode implementations must write exactly one SQL parameter, but wrote none"
                )
            }
            _ => unreachable!("ownership-based Encoder should prevent multiple parameter writes"),
        }
    }

    fn push_str(&mut self, value: &str) {
        let start = self.arena.len();
        self.arena.extend_from_slice(value.as_bytes());
        self.params.push(ParamSlot::Str {
            start,
            len: value.len(),
        });
    }

    fn push_bytes(&mut self, value: &[u8]) {
        let start = self.arena.len();
        self.arena.extend_from_slice(value);
        self.params.push(ParamSlot::Bytes {
            start,
            len: value.len(),
        });
    }
}

impl ParamSource for Params {
    fn len(&self) -> usize {
        self.len()
    }

    fn value_at(&self, index: usize) -> ParamRef<'_> {
        match &self.params[index] {
            ParamSlot::Null => ParamRef::Null,
            ParamSlot::I64(value) => ParamRef::I64(value),
            ParamSlot::U64(value) => ParamRef::U64(value),
            ParamSlot::F64(value) => ParamRef::F64(value),
            ParamSlot::Date(value) => ParamRef::Date(value),
            ParamSlot::Time(value) => ParamRef::Time(value),
            ParamSlot::DateTime(value) => ParamRef::DateTime(value),
            ParamSlot::DateTimeTz(value) => ParamRef::DateTimeTz(value),
            ParamSlot::Uuid(value) => ParamRef::Uuid(value),
            ParamSlot::Str { start, len } => {
                let bytes = &self.arena[*start..*start + *len];
                let value = std::str::from_utf8(bytes).expect("params arena stored invalid utf-8");
                ParamRef::Str(value)
            }
            ParamSlot::Bytes { start, len } => ParamRef::Bytes(&self.arena[*start..*start + *len]),
        }
    }
}

struct ParamsEncoder<'a> {
    params: &'a mut Params,
}

impl EncodeTarget for ParamsEncoder<'_> {
    fn encode_param(&mut self, value: ParamValue<'_>) {
        match value {
            ParamValue::Null => self.encode_null(),
            ParamValue::I64(value) => self.encode_i64(value),
            ParamValue::U64(value) => self.encode_u64(value),
            ParamValue::F64(value) => self.encode_f64(value),
            ParamValue::Str(value) => self.encode_str(value.as_ref()),
            ParamValue::Bytes(value) => self.encode_bytes(value.as_ref()),
        }
    }

    fn encode_null(&mut self) {
        self.params.params.push(ParamSlot::Null);
    }

    fn encode_i64(&mut self, value: i64) {
        self.params.params.push(ParamSlot::I64(value));
    }

    fn encode_u64(&mut self, value: u64) {
        self.params.params.push(ParamSlot::U64(value));
    }

    fn encode_f64(&mut self, value: f64) {
        self.params.params.push(ParamSlot::F64(value));
    }

    fn encode_bool(&mut self, value: bool) {
        self.encode_i64(i64::from(value));
    }

    fn encode_date(&mut self, value: Date) {
        self.params.params.push(ParamSlot::Date(value));
    }

    fn encode_time(&mut self, value: Time) {
        self.params.params.push(ParamSlot::Time(value));
    }

    fn encode_datetime(&mut self, value: DateTime) {
        self.params.params.push(ParamSlot::DateTime(value));
    }

    fn encode_datetime_tz(&mut self, value: DateTimeTz) {
        self.params.params.push(ParamSlot::DateTimeTz(value));
    }

    fn encode_uuid(&mut self, value: [u8; 16]) {
        self.params.params.push(ParamSlot::Uuid(value));
    }

    fn encode_str(&mut self, value: &str) {
        self.params.push_str(value);
    }

    fn encode_string(&mut self, value: String) {
        self.params.push_str(&value);
    }

    fn encode_bytes(&mut self, value: &[u8]) {
        self.params.push_bytes(value);
    }

    fn encode_bytes_owned(&mut self, value: Vec<u8>) {
        self.params.push_bytes(&value);
    }
}

impl<'s, S> BoundStatement<'s, S>
where
    S: PreparedStatement + ?Sized,
{
    /// Creates a bound-statement builder for an existing prepared statement.
    pub fn new(stmt: &'s mut S) -> Self {
        Self {
            stmt,
            params: Params::new(),
        }
    }

    /// Adds one positional parameter.
    pub fn bind<T>(mut self, value: T) -> Self
    where
        T: Encode,
    {
        self.params.push(value);
        self
    }

    /// Runs the statement and returns rows.
    pub async fn execute(self) -> Result<S::Rows<'s>> {
        self.stmt.execute_source(&self.params).await
    }

    /// Runs the statement when no rows are expected.
    pub async fn exec(self) -> Result<ExecResult> {
        self.stmt.exec_source(&self.params).await
    }

    /// Runs the statement and decodes the first row.
    ///
    /// This errors if the statement returns no rows. Extra rows are ignored.
    pub async fn one<T>(self) -> Result<T>
    where
        T: FromRow,
    {
        let mut rows = self.execute().await?;
        decode_one_rows(&mut rows).await
    }

    /// Runs the statement and decodes zero or one row.
    ///
    /// Extra rows are ignored.
    pub async fn optional<T>(self) -> Result<Option<T>>
    where
        T: FromRow,
    {
        let mut rows = self.execute().await?;
        decode_optional_rows(&mut rows).await
    }

    /// Runs the statement and decodes every row.
    pub async fn all<T>(self) -> Result<Vec<T>>
    where
        T: FromRow,
    {
        collect_decoded_rows(self.execute().await?).await
    }
}

impl<'a> Prepare<'a> {
    /// Prepares the statement against an executor.
    pub async fn run<'e, E: Executor>(self, exec: &'e mut E) -> Result<E::Statement<'e>> {
        exec.prepare(self.sql).await
    }
}

impl ParamSource for [ParamValue<'_>] {
    fn len(&self) -> usize {
        <[ParamValue<'_>]>::len(self)
    }

    fn value_at(&self, index: usize) -> ParamRef<'_> {
        param_value_ref(&self[index])
    }
}

impl ParamSource for Vec<ParamValue<'_>> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn value_at(&self, index: usize) -> ParamRef<'_> {
        param_value_ref(&self[index])
    }
}

fn param_value_ref<'a, 'p>(value: &'a ParamValue<'p>) -> ParamRef<'a>
where
    'p: 'a,
{
    match value {
        ParamValue::Null => ParamRef::Null,
        ParamValue::I64(value) => ParamRef::I64(value),
        ParamValue::U64(value) => ParamRef::U64(value),
        ParamValue::F64(value) => ParamRef::F64(value),
        ParamValue::Str(value) => ParamRef::Str(value.as_ref()),
        ParamValue::Bytes(value) => ParamRef::Bytes(value.as_ref()),
    }
}

#[cfg(feature = "mysql")]
pub(crate) struct MysqlParamSource<'a, P: ?Sized>(pub(crate) &'a P);

#[cfg(feature = "mysql")]
impl<P> quex_driver::mysql::ParamSource for MysqlParamSource<'_, P>
where
    P: ParamSource + ?Sized,
{
    fn len(&self) -> usize {
        self.0.len()
    }

    fn value_at(&self, index: usize) -> quex_driver::mysql::ValueRef<'_> {
        match self.0.value_at(index) {
            ParamRef::Null => quex_driver::mysql::ValueRef::Null,
            ParamRef::I64(value) => quex_driver::mysql::ValueRef::I64(value),
            ParamRef::U64(value) => quex_driver::mysql::ValueRef::U64(value),
            ParamRef::F64(value) => quex_driver::mysql::ValueRef::F64(value),
            ParamRef::Date(value) => {
                quex_driver::mysql::ValueRef::Date(quex_driver::mysql::DateValue {
                    year: value.year,
                    month: value.month,
                    day: value.day,
                })
            }
            ParamRef::Time(value) => {
                quex_driver::mysql::ValueRef::Time(quex_driver::mysql::TimeValue {
                    hour: value.hour,
                    minute: value.minute,
                    second: value.second,
                    microsecond: value.microsecond,
                })
            }
            ParamRef::DateTime(value) => {
                quex_driver::mysql::ValueRef::DateTime(quex_driver::mysql::DateTimeValue {
                    date: quex_driver::mysql::DateValue {
                        year: value.date.year,
                        month: value.date.month,
                        day: value.date.day,
                    },
                    time: quex_driver::mysql::TimeValue {
                        hour: value.time.hour,
                        minute: value.time.minute,
                        second: value.time.second,
                        microsecond: value.time.microsecond,
                    },
                })
            }
            ParamRef::DateTimeTz(value) => {
                quex_driver::mysql::ValueRef::DateTimeTz(quex_driver::mysql::DateTimeTzValue {
                    datetime: quex_driver::mysql::DateTimeValue {
                        date: quex_driver::mysql::DateValue {
                            year: value.datetime.date.year,
                            month: value.datetime.date.month,
                            day: value.datetime.date.day,
                        },
                        time: quex_driver::mysql::TimeValue {
                            hour: value.datetime.time.hour,
                            minute: value.datetime.time.minute,
                            second: value.datetime.time.second,
                            microsecond: value.datetime.time.microsecond,
                        },
                    },
                    offset_seconds: value.offset_seconds,
                })
            }
            ParamRef::Uuid(value) => quex_driver::mysql::ValueRef::Uuid(value),
            ParamRef::Str(value) => quex_driver::mysql::ValueRef::String(value),
            ParamRef::Bytes(value) => quex_driver::mysql::ValueRef::Bytes(value),
        }
    }
}

#[cfg(feature = "postgres")]
pub(crate) struct PostgresParamSource<'a, P: ?Sized>(pub(crate) &'a P);

#[cfg(feature = "postgres")]
impl<P> quex_driver::postgres::ParamSource for PostgresParamSource<'_, P>
where
    P: ParamSource + ?Sized,
{
    fn len(&self) -> usize {
        self.0.len()
    }

    fn value_at(&self, index: usize) -> quex_driver::postgres::ValueRef<'_> {
        match self.0.value_at(index) {
            ParamRef::Null => quex_driver::postgres::ValueRef::Null,
            ParamRef::I64(value) => quex_driver::postgres::ValueRef::I64(value),
            ParamRef::U64(value) => quex_driver::postgres::ValueRef::U64(value),
            ParamRef::F64(value) => quex_driver::postgres::ValueRef::F64(value),
            ParamRef::Date(value) => {
                quex_driver::postgres::ValueRef::Date(quex_driver::postgres::DateValue {
                    year: value.year,
                    month: value.month,
                    day: value.day,
                })
            }
            ParamRef::Time(value) => {
                quex_driver::postgres::ValueRef::Time(quex_driver::postgres::TimeValue {
                    hour: value.hour,
                    minute: value.minute,
                    second: value.second,
                    microsecond: value.microsecond,
                })
            }
            ParamRef::DateTime(value) => {
                quex_driver::postgres::ValueRef::DateTime(quex_driver::postgres::DateTimeValue {
                    date: quex_driver::postgres::DateValue {
                        year: value.date.year,
                        month: value.date.month,
                        day: value.date.day,
                    },
                    time: quex_driver::postgres::TimeValue {
                        hour: value.time.hour,
                        minute: value.time.minute,
                        second: value.time.second,
                        microsecond: value.time.microsecond,
                    },
                })
            }
            ParamRef::DateTimeTz(value) => quex_driver::postgres::ValueRef::DateTimeTz(
                quex_driver::postgres::DateTimeTzValue {
                    datetime: quex_driver::postgres::DateTimeValue {
                        date: quex_driver::postgres::DateValue {
                            year: value.datetime.date.year,
                            month: value.datetime.date.month,
                            day: value.datetime.date.day,
                        },
                        time: quex_driver::postgres::TimeValue {
                            hour: value.datetime.time.hour,
                            minute: value.datetime.time.minute,
                            second: value.datetime.time.second,
                            microsecond: value.datetime.time.microsecond,
                        },
                    },
                    offset_seconds: value.offset_seconds,
                },
            ),
            ParamRef::Uuid(value) => quex_driver::postgres::ValueRef::Uuid(value),
            ParamRef::Str(value) => quex_driver::postgres::ValueRef::String(value),
            ParamRef::Bytes(value) => quex_driver::postgres::ValueRef::Bytes(value),
        }
    }
}

impl<T> Encode for &T
where
    T: Encode + ?Sized,
{
    fn encode(&self, out: Encoder<'_>) {
        (*self).encode(out);
    }
}
