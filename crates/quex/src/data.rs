use std::borrow::Cow;

#[cfg(feature = "chrono")]
use chrono::{Offset as _, Timelike as _};

use crate::rows::RowRef;
use crate::{Error, Result};

/// The outcome of an `insert`, `update`, `delete`, or other statement that does
/// not return rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExecResult {
    /// Number of rows reported as changed by the database.
    pub rows_affected: u64,
    /// Last generated id when the driver can report one.
    ///
    /// sqlite and postgres may not have an id for a statement, so this is
    /// optional at the facade level.
    pub last_insert_id: Option<u64>,
}

/// An owned database value.
///
/// This is the common value representation used by the facade when rows are
/// collected or when parameters need to be stored independently of borrowed
/// input data.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// A SQL `null`.
    Null,
    /// A signed integer.
    I64(i64),
    /// An unsigned integer.
    U64(u64),
    /// A floating-point value.
    F64(f64),
    /// A date without a time of day.
    Date(Date),
    /// A time of day with microsecond precision.
    Time(Time),
    /// A date and time without an offset.
    DateTime(DateTime),
    /// A date and time with an offset in seconds.
    DateTimeTz(DateTimeTz),
    /// A 16-byte uuid value.
    Uuid([u8; 16]),
    /// Binary data.
    Bytes(Vec<u8>),
    /// Text data.
    String(String),
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

macro_rules! impl_signed_value_from {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for Value {
                fn from(value: $ty) -> Self {
                    Self::I64(value as i64)
                }
            }
        )*
    };
}

macro_rules! impl_unsigned_value_from {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for Value {
                fn from(value: $ty) -> Self {
                    Self::U64(value as u64)
                }
            }
        )*
    };
}

impl_signed_value_from!(i8, i16, i32, isize);
impl_unsigned_value_from!(u8, u16, u32, usize);

/// A value borrowed or owned long enough to bind to a prepared statement.
///
/// Use this when you want to build a parameter list without implementing
/// [`Encode`] for a custom type.
#[derive(Debug, Clone, PartialEq)]
pub enum ParamValue<'a> {
    /// A SQL `null`.
    Null,
    /// A signed integer.
    I64(i64),
    /// An unsigned integer.
    U64(u64),
    /// A floating-point value.
    F64(f64),
    /// Text data, either borrowed or owned.
    Str(Cow<'a, str>),
    /// Binary data, either borrowed or owned.
    Bytes(Cow<'a, [u8]>),
}

/// A calendar date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date {
    /// The full year.
    pub year: i32,
    /// The month number, from 1 to 12.
    pub month: u8,
    /// The day number, from 1 to 31.
    pub day: u8,
}

/// A time of day with microsecond precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Time {
    /// Hour of day, from 0 to 23.
    pub hour: u8,
    /// Minute, from 0 to 59.
    pub minute: u8,
    /// Second, from 0 to 59.
    pub second: u8,
    /// Fractional seconds in microseconds.
    pub microsecond: u32,
}

/// A date and time without a timezone offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTime {
    /// Date part.
    pub date: Date,
    /// Time part.
    pub time: Time,
}

/// A date and time with a fixed offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTimeTz {
    /// Local date and time.
    pub datetime: DateTime,
    /// Offset from utc in seconds.
    pub offset_seconds: i32,
}

pub(crate) trait EncodeTarget {
    fn encode_param(&mut self, value: ParamValue<'_>);
    fn encode_null(&mut self);
    fn encode_i64(&mut self, value: i64);
    fn encode_u64(&mut self, value: u64);
    fn encode_f64(&mut self, value: f64);
    fn encode_bool(&mut self, value: bool);
    fn encode_date(&mut self, value: Date);
    fn encode_time(&mut self, value: Time);
    fn encode_datetime(&mut self, value: DateTime);
    fn encode_datetime_tz(&mut self, value: DateTimeTz);
    fn encode_uuid(&mut self, value: [u8; 16]);
    fn encode_str(&mut self, value: &str);
    fn encode_string(&mut self, value: String);
    fn encode_bytes(&mut self, value: &[u8]);
    fn encode_bytes_owned(&mut self, value: Vec<u8>);
}

/// Encodes one application value into a SQL parameter sink.
///
/// You normally see this only when implementing [`Encode`] for one of your own
/// types.
pub struct Encoder<'a> {
    target: &'a mut dyn EncodeTarget,
}

impl<'a> Encoder<'a> {
    pub(crate) fn new(target: &'a mut dyn EncodeTarget) -> Self {
        Self { target }
    }

    /// Encodes an already built parameter value.
    pub fn encode_param(self, value: ParamValue<'_>) {
        self.target.encode_param(value);
    }

    /// Encodes a SQL `null`.
    pub fn encode_null(self) {
        self.target.encode_null();
    }

    /// Encodes a signed integer.
    pub fn encode_i64(self, value: i64) {
        self.target.encode_i64(value);
    }

    /// Encodes an unsigned integer.
    pub fn encode_u64(self, value: u64) {
        self.target.encode_u64(value);
    }

    /// Encodes a floating-point value.
    pub fn encode_f64(self, value: f64) {
        self.target.encode_f64(value);
    }

    /// Encodes a boolean as an integer value.
    pub fn encode_bool(self, value: bool) {
        self.target.encode_bool(value);
    }

    /// Encodes a date.
    pub fn encode_date(self, value: Date) {
        self.target.encode_date(value);
    }

    /// Encodes a time of day.
    pub fn encode_time(self, value: Time) {
        self.target.encode_time(value);
    }

    /// Encodes a date and time without an offset.
    pub fn encode_datetime(self, value: DateTime) {
        self.target.encode_datetime(value);
    }

    /// Encodes a date and time with an offset.
    pub fn encode_datetime_tz(self, value: DateTimeTz) {
        self.target.encode_datetime_tz(value);
    }

    /// Encodes a 16-byte uuid value.
    pub fn encode_uuid(self, value: [u8; 16]) {
        self.target.encode_uuid(value);
    }

    /// Encodes borrowed text.
    pub fn encode_str(self, value: &str) {
        self.target.encode_str(value);
    }

    /// Encodes owned text.
    pub fn encode_string(self, value: String) {
        self.target.encode_string(value);
    }

    /// Encodes borrowed bytes.
    pub fn encode_bytes(self, value: &[u8]) {
        self.target.encode_bytes(value);
    }

    /// Encodes owned bytes.
    pub fn encode_bytes_owned(self, value: Vec<u8>) {
        self.target.encode_bytes_owned(value);
    }
}

/// Converts application values into SQL parameters.
///
/// Implement this for domain types when you want to pass them directly to
/// [`crate::Query::bind`].
///
/// `Encode` is for one positional parameter value.
///
/// Implement this when your type should be accepted anywhere `quex` expects a
/// bound value:
///
/// - [`crate::Query::bind`]
/// - [`crate::Params::bind`]
/// - [`crate::PreparedStatement::bind`]
///
/// Keep implementations simple. Pick the SQL representation you want and write
/// exactly one logical value into the [`Encoder`]. In most cases that means
/// calling one encoder method such as [`Encoder::encode_str`] or
/// [`Encoder::encode_i64`].
///
/// `Encode` should not try to write multiple SQL parameters. If your type maps
/// to more than one placeholder, split it at the call site and bind each value
/// separately.
///
/// This trait is about how values are sent to the database. The matching trait
/// on the read side is [`Decode`], which reads one column value.
///
/// `quex` expects each `Encode` implementation to write exactly one SQL
/// parameter. If `encode()` writes none, binding the value panics. Writing more
/// than one is prevented by the ownership-based [`Encoder`] API.
///
/// ```
/// use quex::{Encode, Encoder};
///
/// struct Email(String);
///
/// impl Encode for Email {
///     fn encode(&self, out: Encoder<'_>) {
///         out.encode_str(&self.0);
///     }
/// }
/// ```
pub trait Encode {
    /// Writes this value into the encoder.
    ///
    /// Implementations should always write the same logical SQL value for the
    /// same Rust value.
    ///
    /// This must write exactly one SQL parameter. Writing none causes a panic
    /// when the value is bound. Writing more than one is prevented by the
    /// ownership-based [`Encoder`] API.
    fn encode(&self, out: Encoder<'_>);
}

/// Reads one SQL column value for [`Decode`].
///
/// The decoder may read from an owned [`Value`] or directly from a borrowed row
/// column.
///
/// Most code does not construct `Decoder` directly. It is the helper passed to
/// [`Decode::decode`].
///
/// Use the typed `decode_*` methods that match the SQL representation you
/// expect. If you need to accept more than one representation, handle that in
/// your [`Decode`] implementation.
///
/// `Decoder` does not consume the value. Calling `decode_*` more than once
/// reads the same SQL value each time. That means implementations can inspect
/// the value in more than one way, for example by trying one representation and
/// then falling back to another.
pub struct Decoder<'r> {
    input: ValueInput<'r>,
}

enum ValueInput<'r> {
    Value(&'r Value),
    RowColumn { row: &'r RowRef<'r>, index: usize },
}

impl<'r> Decoder<'r> {
    /// Builds a decoder for an owned value.
    pub fn value(value: &'r Value) -> Self {
        Self {
            input: ValueInput::Value(value),
        }
    }

    /// Builds a decoder for a borrowed row column.
    pub fn row_column(row: &'r RowRef<'r>, index: usize) -> Self {
        Self {
            input: ValueInput::RowColumn { row, index },
        }
    }

    /// Returns whether the value is SQL `null`.
    pub fn is_null(&self) -> Result<bool> {
        match self.input {
            ValueInput::Value(value) => Ok(matches!(value, Value::Null)),
            ValueInput::RowColumn { row, index } => row.is_null(index),
        }
    }

    /// Decodes a signed integer.
    ///
    /// Unsigned values are accepted only when they fit in `i64`.
    pub fn decode_i64(&self) -> Result<i64> {
        match self.input {
            ValueInput::Value(value) => match value {
                Value::I64(value) => Ok(*value),
                Value::U64(value) => i64::try_from(*value)
                    .map_err(|_| Error::Unsupported("u64 value is out of i64 range".into())),
                Value::Null => Err(Error::Unsupported("column is null".into())),
                other => Err(Error::Unsupported(format!("column is not i64: {other:?}"))),
            },
            ValueInput::RowColumn { row, index } => row.get_i64(index),
        }
    }

    /// Decodes an unsigned integer.
    ///
    /// Signed values are accepted only when they are non-negative.
    pub fn decode_u64(&self) -> Result<u64> {
        match self.input {
            ValueInput::Value(value) => match value {
                Value::U64(value) => Ok(*value),
                Value::I64(value) if *value >= 0 => Ok(*value as u64),
                Value::I64(_) => Err(Error::Unsupported(
                    "negative i64 value cannot be represented as u64".into(),
                )),
                Value::Null => Err(Error::Unsupported("column is null".into())),
                other => Err(Error::Unsupported(format!("column is not u64: {other:?}"))),
            },
            ValueInput::RowColumn { row, index } => row.get_u64(index),
        }
    }

    /// Decodes a floating-point value.
    ///
    /// Integer values are also accepted and converted with Rust's normal numeric
    /// cast.
    pub fn decode_f64(&self) -> Result<f64> {
        match self.input {
            ValueInput::Value(value) => match value {
                Value::F64(value) => Ok(*value),
                Value::I64(value) => Ok(*value as f64),
                Value::U64(value) => Ok(*value as f64),
                Value::Null => Err(Error::Unsupported("column is null".into())),
                other => Err(Error::Unsupported(format!("column is not f64: {other:?}"))),
            },
            ValueInput::RowColumn { row, index } => row.get_f64(index),
        }
    }

    /// Decodes a boolean.
    ///
    /// Integers use zero as false and any non-zero value as true. Text accepts
    /// `1`, `0`, `true`, `false`, `t`, and `f`, ignoring ascii case for words.
    pub fn decode_bool(&self) -> Result<bool> {
        if self.is_null()? {
            return Err(Error::Unsupported("column is null".into()));
        }
        match self.input {
            ValueInput::Value(value) => match value {
                Value::I64(value) => Ok(bool_from_i64(*value)),
                Value::U64(value) => Ok(*value != 0),
                Value::String(value) => parse_bool(value),
                Value::Bytes(value) => std::str::from_utf8(value)
                    .map_err(|_| Error::Unsupported("column is not valid utf-8 bool".into()))
                    .and_then(parse_bool),
                Value::Null => Err(Error::Unsupported("column is null".into())),
                other => Err(Error::Unsupported(format!("column is not bool: {other:?}"))),
            },
            ValueInput::RowColumn { row, index } => match row.get_i64(index) {
                Ok(value) => Ok(bool_from_i64(value)),
                Err(_) => parse_bool(row.get_str(index)?),
            },
        }
    }

    /// Decodes text as UTF-8.
    pub fn decode_str(&self) -> Result<&'r str> {
        match self.input {
            ValueInput::Value(value) => match value {
                Value::String(value) => Ok(value),
                Value::Bytes(value) => std::str::from_utf8(value)
                    .map_err(|_| Error::Unsupported("column is not valid utf-8 text".into())),
                Value::Null => Err(Error::Unsupported("column is null".into())),
                other => Err(Error::Unsupported(format!("column is not text: {other:?}"))),
            },
            ValueInput::RowColumn { row, index } => row.get_str(index),
        }
    }

    /// Decodes bytes.
    ///
    /// Text is returned as its UTF-8 bytes.
    pub fn decode_bytes(&self) -> Result<&'r [u8]> {
        match self.input {
            ValueInput::Value(value) => match value {
                Value::Bytes(value) => Ok(value),
                Value::String(value) => Ok(value.as_bytes()),
                Value::Null => Err(Error::Unsupported("column is null".into())),
                other => Err(Error::Unsupported(format!(
                    "column is not bytes: {other:?}"
                ))),
            },
            ValueInput::RowColumn { row, index } => row.get_bytes(index),
        }
    }

    /// Copies the value into the facade's owned [`Value`] type.
    pub fn decode_owned(&self) -> Result<Value> {
        match self.input {
            ValueInput::Value(value) => Ok(value.clone()),
            ValueInput::RowColumn { row, index } => {
                let owned = row.to_owned()?;
                owned
                    .values
                    .get(index)
                    .cloned()
                    .ok_or_else(|| Error::Unsupported("column index out of bounds".into()))
            }
        }
    }
}

/// Decodes a single SQL value into an application type.
///
/// `Decode` is for one column value.
///
/// Implement this for scalar or scalar-like types that fit in a single SQL
/// column. [`Row::get`] and [`RowRef::get`] both use this trait.
///
/// If your type needs more than one column, use [`FromRow`] or
/// [`FromRowRef`] instead.
///
/// The usual shape is:
///
/// - choose the SQL representation you expect
/// - read it from the [`Decoder`]
/// - validate or convert it
/// - return an error when the value is missing or has the wrong shape
///
/// The decoder is not a cursor. Calling multiple `decode_*` methods reads the
/// same underlying value again each time.
///
/// Implementations should treat unsupported SQL types as an error instead of
/// silently coercing them to something surprising.
///
/// This trait is the read-side counterpart to [`Encode`].
///
/// ```
/// use quex::{Decode, Decoder};
///
/// struct Email(String);
///
/// impl Decode for Email {
///     fn decode(value: &mut Decoder<'_>) -> quex::Result<Self> {
///         let text = value.decode_str()?;
///         if text.contains('@') {
///             Ok(Self(text.to_owned()))
///         } else {
///             Err(quex::Error::Unsupported("invalid email".into()))
///         }
///     }
/// }
/// ```
pub trait Decode: Sized {
    /// Reads `Self` from the decoder.
    ///
    /// The decoder represents one SQL value, either borrowed from a row or
    /// coming from an owned [`Value`].
    fn decode(value: &mut Decoder<'_>) -> Result<Self>;
}

pub(crate) fn decode_value<T>(value: &Value) -> Result<T>
where
    T: Decode,
{
    let mut value = Decoder::value(value);
    T::decode(&mut value)
}

pub(crate) fn decode_row_column<T>(row: &RowRef<'_>, index: usize) -> Result<T>
where
    T: Decode,
{
    let mut value = Decoder::row_column(row, index);
    T::decode(&mut value)
}

/// Decodes an owned row into an application type.
///
/// Most applications implement this trait for their record structs and then use
/// [`Query::one`], [`Query::optional`], or [`Query::all`].
///
/// `from_row` takes an owned [`Row`]. That makes it a good fit for types that
/// own their decoded data.
///
/// The default [`Self::from_row_ref`] implementation first copies the borrowed
/// row into an owned row and then calls [`Self::from_row`]. Override it if you
/// can decode more directly from a borrowed row and want to skip that copy.
///
/// ```
/// use quex::{FromRow, Row};
///
/// struct User {
///     id: i64,
///     name: String,
/// }
///
/// impl FromRow for User {
///     fn from_row(row: &Row) -> quex::Result<Self> {
///         Ok(Self {
///             id: row.get("id")?,
///             name: row.get("name")?,
///         })
///     }
/// }
/// ```
pub trait FromRow: Sized {
    /// Decodes an owned row.
    fn from_row(row: &Row) -> Result<Self>;

    /// Decodes a borrowed row by first copying it into an owned row.
    ///
    /// Override this when you need a faster path for borrowed rows.
    fn from_row_ref(row: &RowRef<'_>) -> Result<Self> {
        let owned = row.to_owned()?;
        Self::from_row(&owned)
    }
}

/// Decodes one column directly from a borrowed row.
///
/// This is the trait behind [`RowRef::get`]. Most types do not need to
/// implement it manually because many implementations can go through
/// [`Decode`].
///
/// Implementors may borrow from `row`, so the returned value must not outlive
/// `'r`.
pub trait FromColumnRef<'r>: Sized {
    /// Decodes the column at `index`.
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self>;
}

/// Decodes a borrowed row without first collecting it into an owned row.
///
/// This is useful for types that borrow text or bytes from the current row.
/// The decoded value cannot outlive that row.
///
/// Use this when you want zero-copy row decoding for borrowed application
/// types. Use [`FromRow`] when the decoded type owns its data.
pub trait FromRowRef<'r>: Sized {
    /// Decodes `Self` from the borrowed row.
    fn from_row_ref(row: &'r RowRef<'r>) -> Result<Self>;
}

/// Driver-specific column type metadata.
#[derive(Debug, Clone)]
pub enum ColumnType {
    /// mysql or mariadb native column type code.
    Mysql(u32),
    /// postgres type oid.
    Postgres(u32),
    /// sqlite declared type, when sqlite reports one.
    Sqlite(Option<String>),
}

/// Metadata for one result column.
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name reported by the driver.
    pub name: String,
    /// Whether the driver reported this column as nullable.
    pub nullable: bool,
    /// Driver-specific type metadata.
    pub kind: ColumnType,
}

/// A row whose column metadata and values are owned by Rust.
///
/// Owned rows are convenient for application-level decoding because they can be
/// kept after the result stream advances.
#[derive(Debug, Clone)]
pub struct Row {
    /// Column metadata in result order.
    pub columns: Vec<Column>,
    /// Values in the same order as [`Self::columns`].
    pub values: Vec<Value>,
}

impl Row {
    /// Decodes the row through [`FromRow`].
    pub fn decode<T>(&self) -> Result<T>
    where
        T: FromRow,
    {
        T::from_row(self)
    }

    /// Reads and decodes one column by index or name.
    pub fn get<T>(&self, index: impl ColumnIndex) -> Result<T>
    where
        T: Decode,
    {
        let idx = index.index(&self.columns)?;
        let value = self
            .values
            .get(idx)
            .ok_or_else(|| Error::Unsupported("column index out of bounds".into()))?;
        decode_value(value)
    }

    /// Reads a column as `i64`.
    pub fn get_i64(&self, index: impl ColumnIndex) -> Result<i64> {
        match self.values.get(index.index(&self.columns)?) {
            Some(Value::I64(value)) => Ok(*value),
            Some(Value::U64(value)) => i64::try_from(*value)
                .map_err(|_| Error::Unsupported("u64 value is out of i64 range".into())),
            Some(Value::Null) => Err(Error::Unsupported("column is null".into())),
            Some(other) => Err(Error::Unsupported(format!("column is not i64: {other:?}"))),
            None => Err(Error::Unsupported("column index out of bounds".into())),
        }
    }

    /// Reads a column as `u64`.
    pub fn get_u64(&self, index: impl ColumnIndex) -> Result<u64> {
        match self.values.get(index.index(&self.columns)?) {
            Some(Value::U64(value)) => Ok(*value),
            Some(Value::I64(value)) if *value >= 0 => Ok(*value as u64),
            Some(Value::I64(_)) => Err(Error::Unsupported(
                "negative i64 value cannot be represented as u64".into(),
            )),
            Some(Value::Null) => Err(Error::Unsupported("column is null".into())),
            Some(other) => Err(Error::Unsupported(format!("column is not u64: {other:?}"))),
            None => Err(Error::Unsupported("column index out of bounds".into())),
        }
    }

    /// Reads a column as `f64`.
    pub fn get_f64(&self, index: impl ColumnIndex) -> Result<f64> {
        match self.values.get(index.index(&self.columns)?) {
            Some(Value::F64(value)) => Ok(*value),
            Some(Value::I64(value)) => Ok(*value as f64),
            Some(Value::U64(value)) => Ok(*value as f64),
            Some(Value::Null) => Err(Error::Unsupported("column is null".into())),
            Some(other) => Err(Error::Unsupported(format!("column is not f64: {other:?}"))),
            None => Err(Error::Unsupported("column index out of bounds".into())),
        }
    }

    /// Reads a text column.
    pub fn get_str(&self, index: impl ColumnIndex) -> Result<&str> {
        match self.values.get(index.index(&self.columns)?) {
            Some(Value::String(value)) => Ok(value),
            Some(Value::Null) => Err(Error::Unsupported("column is null".into())),
            Some(other) => Err(Error::Unsupported(format!("column is not text: {other:?}"))),
            None => Err(Error::Unsupported("column index out of bounds".into())),
        }
    }

    /// Reads a byte column.
    ///
    /// Text and uuid values can also be read as bytes.
    pub fn get_bytes(&self, index: impl ColumnIndex) -> Result<&[u8]> {
        match self.values.get(index.index(&self.columns)?) {
            Some(Value::Bytes(value)) => Ok(value),
            Some(Value::String(value)) => Ok(value.as_bytes()),
            Some(Value::Uuid(value)) => Ok(value),
            Some(Value::Null) => Err(Error::Unsupported("column is null".into())),
            Some(other) => Err(Error::Unsupported(format!(
                "column is not bytes: {other:?}"
            ))),
            None => Err(Error::Unsupported("column index out of bounds".into())),
        }
    }
}

/// A column lookup accepted by row accessors.
///
/// `usize` indexes from zero, and `&str` looks up by column name.
///
/// Most users do not need to implement this trait. It exists so row accessors
/// can accept either a numeric index or a column name with one method.
pub trait ColumnIndex {
    /// Resolves the lookup to a zero-based column index.
    fn index(&self, columns: &[Column]) -> Result<usize>;
}

impl ColumnIndex for usize {
    fn index(&self, columns: &[Column]) -> Result<usize> {
        if *self < columns.len() {
            Ok(*self)
        } else {
            Err(Error::Unsupported(format!("column {self} out of bounds")))
        }
    }
}

impl ColumnIndex for &str {
    fn index(&self, columns: &[Column]) -> Result<usize> {
        columns
            .iter()
            .position(|column| column.name == *self)
            .ok_or_else(|| Error::Unsupported(format!("unknown column {}", self)))
    }
}

impl Encode for Value {
    fn encode(&self, out: Encoder<'_>) {
        match self {
            Value::Null => out.encode_null(),
            Value::I64(value) => out.encode_i64(*value),
            Value::U64(value) => out.encode_u64(*value),
            Value::F64(value) => out.encode_f64(*value),
            Value::Date(value) => out.encode_date(*value),
            Value::Time(value) => out.encode_time(*value),
            Value::DateTime(value) => out.encode_datetime(*value),
            Value::DateTimeTz(value) => out.encode_datetime_tz(*value),
            Value::Uuid(value) => out.encode_uuid(*value),
            Value::Bytes(value) => out.encode_bytes(value.as_slice()),
            Value::String(value) => out.encode_str(value.as_str()),
        }
    }
}

macro_rules! impl_signed_to_param {
    ($($ty:ty),* $(,)?) => {
        $(
            impl Encode for $ty {
                fn encode(&self, out: Encoder<'_>) {
                    out.encode_i64(*self as i64);
                }
            }
        )*
    };
}

macro_rules! impl_unsigned_to_param {
    ($($ty:ty),* $(,)?) => {
        $(
            impl Encode for $ty {
                fn encode(&self, out: Encoder<'_>) {
                    out.encode_u64(*self as u64);
                }
            }
        )*
    };
}

impl_signed_to_param!(i8, i16, i32, i64, isize);
impl_unsigned_to_param!(u8, u16, u32, u64, usize);

impl Encode for f32 {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_f64(*self as f64);
    }
}

impl Encode for f64 {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_f64(*self);
    }
}

impl Encode for str {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_str(self);
    }
}

impl Encode for String {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_str(self.as_str());
    }
}

impl Encode for [u8] {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_bytes(self);
    }
}

impl Encode for Vec<u8> {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_bytes(self.as_slice());
    }
}

impl Encode for bool {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_bool(*self);
    }
}

impl Encode for Date {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_date(*self);
    }
}

impl Encode for Time {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_time(*self);
    }
}

impl Encode for DateTime {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_datetime(*self);
    }
}

impl Encode for DateTimeTz {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_datetime_tz(*self);
    }
}

#[cfg(feature = "json")]
impl Encode for serde_json::Value {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_string(self.to_string());
    }
}

#[cfg(feature = "uuid")]
impl Encode for uuid::Uuid {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_uuid(*self.as_bytes());
    }
}

impl Encode for ParamValue<'_> {
    fn encode(&self, out: Encoder<'_>) {
        match self {
            ParamValue::Null => out.encode_null(),
            ParamValue::I64(value) => out.encode_i64(*value),
            ParamValue::U64(value) => out.encode_u64(*value),
            ParamValue::F64(value) => out.encode_f64(*value),
            ParamValue::Str(value) => out.encode_str(value.as_ref()),
            ParamValue::Bytes(value) => out.encode_bytes(value.as_ref()),
        }
    }
}

impl Decode for Value {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        value.decode_owned()
    }
}

impl Decode for String {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        Ok(value.decode_str()?.to_owned())
    }
}

impl Decode for Vec<u8> {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        Ok(value.decode_bytes()?.to_vec())
    }
}

impl Decode for i64 {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        value.decode_i64()
    }
}

impl Decode for u64 {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        value.decode_u64()
    }
}

impl Decode for f64 {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        value.decode_f64()
    }
}

impl Decode for bool {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        value.decode_bool()
    }
}

impl Decode for Date {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        if let Ok(Value::Date(value)) = value.decode_owned() {
            return Ok(value);
        }
        decode_textual(value, parse_date, "date")
    }
}

impl Decode for Time {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        if let Ok(Value::Time(value)) = value.decode_owned() {
            return Ok(value);
        }
        decode_textual(value, parse_time, "time")
    }
}

impl Decode for DateTime {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        if let Ok(Value::DateTime(value)) = value.decode_owned() {
            return Ok(value);
        }
        decode_textual(value, parse_datetime, "datetime")
    }
}

impl Decode for DateTimeTz {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        if let Ok(Value::DateTimeTz(value)) = value.decode_owned() {
            return Ok(value);
        }
        decode_textual(value, parse_datetime_tz, "datetime with offset")
    }
}

#[cfg(feature = "json")]
impl Decode for serde_json::Value {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        serde_json::from_str(value.decode_str()?).map_err(|err| Error::Unsupported(err.to_string()))
    }
}

#[cfg(feature = "uuid")]
impl Decode for uuid::Uuid {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        if let Ok(Value::Uuid(bytes)) = value.decode_owned() {
            return Ok(uuid::Uuid::from_bytes(bytes));
        }

        if let Ok(bytes) = value.decode_bytes() {
            if let Ok(raw) = <[u8; 16]>::try_from(bytes) {
                return Ok(uuid::Uuid::from_bytes(raw));
            }

            if let Ok(text) = std::str::from_utf8(bytes) {
                return uuid::Uuid::parse_str(text)
                    .map_err(|err| Error::Unsupported(err.to_string()));
            }
        }

        uuid::Uuid::parse_str(value.decode_str()?)
            .map_err(|err| Error::Unsupported(err.to_string()))
    }
}

fn bool_from_i64(value: i64) -> bool {
    value != 0
}

fn parse_bool(value: &str) -> Result<bool> {
    match value {
        "1" => Ok(true),
        "0" => Ok(false),
        _ if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("t") => Ok(true),
        _ if value.eq_ignore_ascii_case("false") || value.eq_ignore_ascii_case("f") => Ok(false),
        _ => Err(Error::Unsupported(format!("column is not bool: {value:?}"))),
    }
}

fn decode_textual<T>(
    value: &mut Decoder<'_>,
    parse: impl Fn(&str) -> Option<T>,
    kind: &'static str,
) -> Result<T> {
    if let Ok(bytes) = value.decode_bytes() {
        if let Ok(text) = std::str::from_utf8(bytes) {
            if let Some(parsed) = parse(text) {
                return Ok(parsed);
            }
        }
    }

    let text = value.decode_str()?;
    parse(text).ok_or_else(|| Error::Unsupported(format!("column is not {kind}")))
}

pub(crate) fn parse_date(text: &str) -> Option<Date> {
    let [year, month, day] = text.split('-').collect::<Vec<_>>().try_into().ok()?;
    Some(Date {
        year: year.parse().ok()?,
        month: month.parse().ok()?,
        day: day.parse().ok()?,
    })
}

pub(crate) fn parse_time(text: &str) -> Option<Time> {
    let (time, microsecond) = match text.split_once('.') {
        Some((time, fraction)) => {
            let digits = fraction.as_bytes();
            if digits.is_empty() || digits.len() > 6 || !digits.iter().all(u8::is_ascii_digit) {
                return None;
            }
            let mut micros = fraction.parse::<u32>().ok()?;
            for _ in digits.len()..6 {
                micros *= 10;
            }
            (time, micros)
        }
        None => (text, 0),
    };

    let [hour, minute, second] = time.split(':').collect::<Vec<_>>().try_into().ok()?;
    Some(Time {
        hour: hour.parse().ok()?,
        minute: minute.parse().ok()?,
        second: second.parse().ok()?,
        microsecond,
    })
}

pub(crate) fn parse_datetime(text: &str) -> Option<DateTime> {
    let (date, time) = text.split_once(' ').or_else(|| text.split_once('T'))?;
    Some(DateTime {
        date: parse_date(date)?,
        time: parse_time(time)?,
    })
}

pub(crate) fn parse_datetime_tz(text: &str) -> Option<DateTimeTz> {
    let (datetime, offset_seconds) = parse_offset_datetime_parts(text)?;
    Some(DateTimeTz {
        datetime: parse_datetime(datetime)?,
        offset_seconds,
    })
}

fn parse_offset_datetime_parts(text: &str) -> Option<(&str, i32)> {
    if let Some(datetime) = text.strip_suffix('Z') {
        return Some((datetime, 0));
    }

    let split = text
        .char_indices()
        .rev()
        .find(|(_, ch)| matches!(ch, '+' | '-'))?
        .0;
    let (datetime, offset) = text.split_at(split);
    let sign = if offset.starts_with('-') { -1 } else { 1 };
    let offset = &offset[1..];
    let (hours, minutes) = offset.split_once(':')?;
    let total = hours.parse::<i32>().ok()? * 3600 + minutes.parse::<i32>().ok()? * 60;
    Some((datetime, sign * total))
}

#[cfg(feature = "chrono")]
impl Encode for chrono::NaiveDate {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_date(Date {
            year: chrono::Datelike::year(self),
            month: chrono::Datelike::month(self) as u8,
            day: chrono::Datelike::day(self) as u8,
        });
    }
}

#[cfg(feature = "chrono")]
impl Encode for chrono::NaiveTime {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_time(Time {
            hour: chrono::Timelike::hour(self) as u8,
            minute: chrono::Timelike::minute(self) as u8,
            second: chrono::Timelike::second(self) as u8,
            microsecond: self.nanosecond() / 1_000,
        });
    }
}

#[cfg(feature = "chrono")]
impl Encode for chrono::NaiveDateTime {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_datetime(DateTime {
            date: Date {
                year: chrono::Datelike::year(self),
                month: chrono::Datelike::month(self) as u8,
                day: chrono::Datelike::day(self) as u8,
            },
            time: Time {
                hour: chrono::Timelike::hour(self) as u8,
                minute: chrono::Timelike::minute(self) as u8,
                second: chrono::Timelike::second(self) as u8,
                microsecond: self.nanosecond() / 1_000,
            },
        });
    }
}

#[cfg(feature = "chrono")]
impl<Tz> Encode for chrono::DateTime<Tz>
where
    Tz: chrono::TimeZone,
    Tz::Offset: ::core::fmt::Display,
{
    fn encode(&self, out: Encoder<'_>) {
        let local = self.naive_local();
        out.encode_datetime_tz(DateTimeTz {
            datetime: DateTime {
                date: Date {
                    year: chrono::Datelike::year(&local),
                    month: chrono::Datelike::month(&local) as u8,
                    day: chrono::Datelike::day(&local) as u8,
                },
                time: Time {
                    hour: chrono::Timelike::hour(&local) as u8,
                    minute: chrono::Timelike::minute(&local) as u8,
                    second: chrono::Timelike::second(&local) as u8,
                    microsecond: local.nanosecond() / 1_000,
                },
            },
            offset_seconds: self.offset().fix().local_minus_utc(),
        });
    }
}

#[cfg(feature = "chrono")]
impl Decode for chrono::NaiveDate {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        let date = Date::decode(value)?;
        chrono::NaiveDate::from_ymd_opt(date.year, date.month.into(), date.day.into())
            .ok_or_else(|| Error::Unsupported("invalid date".into()))
    }
}

#[cfg(feature = "chrono")]
impl Decode for chrono::NaiveTime {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        let time = Time::decode(value)?;
        chrono::NaiveTime::from_hms_micro_opt(
            time.hour.into(),
            time.minute.into(),
            time.second.into(),
            time.microsecond,
        )
        .ok_or_else(|| Error::Unsupported("invalid time".into()))
    }
}

#[cfg(feature = "chrono")]
impl Decode for chrono::NaiveDateTime {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        let datetime = DateTime::decode(value)?;
        let date = chrono::NaiveDate::from_ymd_opt(
            datetime.date.year,
            datetime.date.month.into(),
            datetime.date.day.into(),
        )
        .ok_or_else(|| Error::Unsupported("invalid date".into()))?;
        let time = chrono::NaiveTime::from_hms_micro_opt(
            datetime.time.hour.into(),
            datetime.time.minute.into(),
            datetime.time.second.into(),
            datetime.time.microsecond,
        )
        .ok_or_else(|| Error::Unsupported("invalid time".into()))?;
        Ok(chrono::NaiveDateTime::new(date, time))
    }
}

#[cfg(feature = "chrono")]
impl Decode for chrono::DateTime<chrono::Utc> {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        let value = chrono::DateTime::<chrono::FixedOffset>::decode(value)?;
        Ok(value.with_timezone(&chrono::Utc))
    }
}

#[cfg(feature = "chrono")]
impl Decode for chrono::DateTime<chrono::FixedOffset> {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        let datetime = DateTimeTz::decode(value)?;
        let date = chrono::NaiveDate::from_ymd_opt(
            datetime.datetime.date.year,
            datetime.datetime.date.month.into(),
            datetime.datetime.date.day.into(),
        )
        .ok_or_else(|| Error::Unsupported("invalid date".into()))?;
        let time = chrono::NaiveTime::from_hms_micro_opt(
            datetime.datetime.time.hour.into(),
            datetime.datetime.time.minute.into(),
            datetime.datetime.time.second.into(),
            datetime.datetime.time.microsecond,
        )
        .ok_or_else(|| Error::Unsupported("invalid time".into()))?;
        let offset = chrono::FixedOffset::east_opt(datetime.offset_seconds)
            .ok_or_else(|| Error::Unsupported("invalid datetime offset".into()))?;
        Ok(chrono::DateTime::from_naive_utc_and_offset(
            chrono::NaiveDateTime::new(date, time)
                - chrono::TimeDelta::seconds(i64::from(datetime.offset_seconds)),
            offset,
        ))
    }
}

#[cfg(feature = "time")]
impl Encode for time::Date {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_date(Date {
            year: self.year(),
            month: u8::from(self.month()),
            day: self.day(),
        });
    }
}

#[cfg(feature = "time")]
impl Encode for time::Time {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_time(Time {
            hour: self.hour(),
            minute: self.minute(),
            second: self.second(),
            microsecond: self.microsecond(),
        });
    }
}

#[cfg(feature = "time")]
impl Encode for time::PrimitiveDateTime {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_datetime(DateTime {
            date: Date {
                year: self.year(),
                month: u8::from(self.month()),
                day: self.day(),
            },
            time: Time {
                hour: self.hour(),
                minute: self.minute(),
                second: self.second(),
                microsecond: self.microsecond(),
            },
        });
    }
}

#[cfg(feature = "time")]
impl Encode for time::OffsetDateTime {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_datetime_tz(DateTimeTz {
            datetime: DateTime {
                date: Date {
                    year: self.year(),
                    month: u8::from(self.month()),
                    day: self.day(),
                },
                time: Time {
                    hour: self.hour(),
                    minute: self.minute(),
                    second: self.second(),
                    microsecond: self.microsecond(),
                },
            },
            offset_seconds: self.offset().whole_seconds(),
        });
    }
}

#[cfg(feature = "time")]
impl Decode for time::Date {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        let date = Date::decode(value)?;
        let month =
            time::Month::try_from(date.month).map_err(|err| Error::Unsupported(err.to_string()))?;
        time::Date::from_calendar_date(date.year, month, date.day)
            .map_err(|err| Error::Unsupported(err.to_string()))
    }
}

#[cfg(feature = "time")]
impl Decode for time::Time {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        let time = Time::decode(value)?;
        time::Time::from_hms_micro(time.hour, time.minute, time.second, time.microsecond)
            .map_err(|err| Error::Unsupported(err.to_string()))
    }
}

#[cfg(feature = "time")]
impl Decode for time::PrimitiveDateTime {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        let datetime = DateTime::decode(value)?;
        Ok(time::PrimitiveDateTime::new(
            time::Date::decode(&mut Decoder::value(&Value::Date(datetime.date)))?,
            time::Time::decode(&mut Decoder::value(&Value::Time(datetime.time)))?,
        ))
    }
}

#[cfg(feature = "time")]
impl Decode for time::OffsetDateTime {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        let datetime = DateTimeTz::decode(value)?;
        let date = time::Date::from_calendar_date(
            datetime.datetime.date.year,
            time::Month::try_from(datetime.datetime.date.month)
                .map_err(|err| Error::Unsupported(err.to_string()))?,
            datetime.datetime.date.day,
        )
        .map_err(|err| Error::Unsupported(err.to_string()))?;
        let time = time::Time::from_hms_micro(
            datetime.datetime.time.hour,
            datetime.datetime.time.minute,
            datetime.datetime.time.second,
            datetime.datetime.time.microsecond,
        )
        .map_err(|err| Error::Unsupported(err.to_string()))?;
        let offset = time::UtcOffset::from_whole_seconds(datetime.offset_seconds)
            .map_err(|err| Error::Unsupported(err.to_string()))?;
        Ok(time::PrimitiveDateTime::new(date, time).assume_offset(offset))
    }
}

macro_rules! impl_scalar_from_row {
    ($($ty:ty),* $(,)?) => {
        $(
            impl FromRow for $ty {
                fn from_row(row: &Row) -> Result<Self> {
                    if row.values.len() != 1 {
                        return Err(Error::Unsupported(format!(
                            "expected one column for scalar row, got {}",
                            row.values.len()
                        )));
                    }
                    row.get(0)
                }

                fn from_row_ref(row: &RowRef<'_>) -> Result<Self> {
                    if row.columns().len() != 1 {
                        return Err(Error::Unsupported(format!(
                            "expected one column for scalar row, got {}",
                            row.columns().len()
                        )));
                    }
                    decode_row_column::<$ty>(row, 0)
                }
            }
        )*
    };
}

impl_scalar_from_row!(
    Value,
    String,
    Vec<u8>,
    i64,
    u64,
    f64,
    bool,
    Date,
    Time,
    DateTime,
    DateTimeTz
);

#[cfg(feature = "json")]
impl_scalar_from_row!(serde_json::Value);

#[cfg(feature = "uuid")]
impl_scalar_from_row!(uuid::Uuid);

#[cfg(feature = "chrono")]
impl_scalar_from_row!(
    chrono::NaiveDate,
    chrono::NaiveTime,
    chrono::NaiveDateTime,
    chrono::DateTime<chrono::Utc>,
    chrono::DateTime<chrono::FixedOffset>,
);

#[cfg(feature = "time")]
impl_scalar_from_row!(
    time::Date,
    time::Time,
    time::PrimitiveDateTime,
    time::OffsetDateTime,
);

macro_rules! impl_tuple_from_row {
    ($len:expr, $($idx:tt => $name:ident),+ $(,)?) => {
        impl<$($name),+> FromRow for ($($name,)+)
        where
            $($name: Decode,)+
        {
            fn from_row(row: &Row) -> Result<Self> {
                if row.values.len() != $len {
                    return Err(Error::Unsupported(format!(
                        "expected {} columns for tuple row, got {}",
                        $len,
                        row.values.len()
                    )));
                }
                Ok(($(row.get::<$name>($idx)?,)+))
            }

            fn from_row_ref(row: &RowRef<'_>) -> Result<Self> {
                if row.columns().len() != $len {
                    return Err(Error::Unsupported(format!(
                        "expected {} columns for tuple row, got {}",
                        $len,
                        row.columns().len()
                    )));
                }
                Ok(($(decode_row_column::<$name>(row, $idx)?,)+))
            }
        }
    };
}

impl_tuple_from_row!(1, 0 => A);
impl_tuple_from_row!(2, 0 => A, 1 => B);
impl_tuple_from_row!(3, 0 => A, 1 => B, 2 => C);
impl_tuple_from_row!(4, 0 => A, 1 => B, 2 => C, 3 => D);
impl_tuple_from_row!(5, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E);
impl_tuple_from_row!(6, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F);
impl_tuple_from_row!(7, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G);
impl_tuple_from_row!(8, 0 => A, 1 => B, 2 => C, 3 => D, 4 => E, 5 => F, 6 => G, 7 => H);

impl<'r> FromColumnRef<'r> for i64 {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        row.get_i64(index)
    }
}

impl<'r> FromColumnRef<'r> for u64 {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        row.get_u64(index)
    }
}

impl<'r> FromColumnRef<'r> for f64 {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        row.get_f64(index)
    }
}

impl<'r> FromColumnRef<'r> for bool {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        if row.is_null(index)? {
            return Err(Error::Unsupported("column is null".into()));
        }
        match row.get_i64(index) {
            Ok(value) => Ok(bool_from_i64(value)),
            Err(_) => parse_bool(row.get_str(index)?),
        }
    }
}

impl<'r> FromColumnRef<'r> for String {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        Ok(row.get_str(index)?.to_owned())
    }
}

impl<'r> FromColumnRef<'r> for Vec<u8> {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        Ok(row.get_bytes(index)?.to_vec())
    }
}

impl<'r> FromColumnRef<'r> for Value {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        Ok(row.to_owned()?.values[index].clone())
    }
}

impl<'r> FromColumnRef<'r> for Date {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        row.get_date(index)
    }
}

impl<'r> FromColumnRef<'r> for Time {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        row.get_time_value(index)
    }
}

impl<'r> FromColumnRef<'r> for DateTime {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        row.get_datetime(index)
    }
}

impl<'r> FromColumnRef<'r> for DateTimeTz {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        row.get_datetimetz(index)
    }
}

#[cfg(feature = "uuid")]
impl<'r> FromColumnRef<'r> for uuid::Uuid {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        match row.get_uuid_bytes(index) {
            Ok(bytes) => Ok(uuid::Uuid::from_bytes(bytes)),
            Err(_) => uuid::Uuid::parse_str(row.get_str(index)?)
                .map_err(|err| Error::Unsupported(err.to_string())),
        }
    }
}

#[cfg(feature = "chrono")]
impl<'r> FromColumnRef<'r> for chrono::NaiveDate {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        chrono::NaiveDate::decode(&mut Decoder::row_column(row, index))
    }
}

#[cfg(feature = "chrono")]
impl<'r> FromColumnRef<'r> for chrono::NaiveTime {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        chrono::NaiveTime::decode(&mut Decoder::row_column(row, index))
    }
}

#[cfg(feature = "chrono")]
impl<'r> FromColumnRef<'r> for chrono::NaiveDateTime {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        chrono::NaiveDateTime::decode(&mut Decoder::row_column(row, index))
    }
}

#[cfg(feature = "chrono")]
impl<'r> FromColumnRef<'r> for chrono::DateTime<chrono::Utc> {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        chrono::DateTime::<chrono::Utc>::decode(&mut Decoder::row_column(row, index))
    }
}

#[cfg(feature = "chrono")]
impl<'r> FromColumnRef<'r> for chrono::DateTime<chrono::FixedOffset> {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        chrono::DateTime::<chrono::FixedOffset>::decode(&mut Decoder::row_column(row, index))
    }
}

#[cfg(feature = "time")]
impl<'r> FromColumnRef<'r> for time::Date {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        time::Date::decode(&mut Decoder::row_column(row, index))
    }
}

#[cfg(feature = "time")]
impl<'r> FromColumnRef<'r> for time::Time {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        time::Time::decode(&mut Decoder::row_column(row, index))
    }
}

#[cfg(feature = "time")]
impl<'r> FromColumnRef<'r> for time::PrimitiveDateTime {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        time::PrimitiveDateTime::decode(&mut Decoder::row_column(row, index))
    }
}

#[cfg(feature = "time")]
impl<'r> FromColumnRef<'r> for time::OffsetDateTime {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        time::OffsetDateTime::decode(&mut Decoder::row_column(row, index))
    }
}

impl<'r> FromColumnRef<'r> for &'r str {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        row.get_str(index)
    }
}

impl<'r> FromColumnRef<'r> for &'r [u8] {
    fn from_column_ref(row: &'r RowRef<'r>, index: usize) -> Result<Self> {
        row.get_bytes(index)
    }
}
