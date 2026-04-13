/// An owned mysql or mariadb value.
#[derive(Debug, Clone)]
pub enum Value {
    /// A SQL `null`.
    Null,
    /// A signed integer.
    I64(i64),
    /// An unsigned integer.
    U64(u64),
    /// A floating-point value.
    F64(f64),
    /// A date.
    Date(DateValue),
    /// A time of day.
    Time(TimeValue),
    /// A date and time without an offset.
    DateTime(DateTimeValue),
    /// A date and time with an offset.
    DateTimeTz(DateTimeTzValue),
    /// A 16-byte uuid.
    Uuid([u8; 16]),
    /// Binary data.
    Bytes(Vec<u8>),
    /// Text data.
    String(String),
}

/// A date value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateValue {
    /// The full year.
    pub year: i32,
    /// The month number, from 1 to 12.
    pub month: u8,
    /// The day number, from 1 to 31.
    pub day: u8,
}

/// A time of day with microsecond precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeValue {
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
pub struct DateTimeValue {
    /// Date part.
    pub date: DateValue,
    /// Time part.
    pub time: TimeValue,
}

/// A date and time with a fixed offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTimeTzValue {
    /// Local date and time.
    pub datetime: DateTimeValue,
    /// Offset from utc in seconds.
    pub offset_seconds: i32,
}

/// A borrowed mysql or mariadb parameter value.
#[derive(Debug, Clone, Copy)]
pub enum ValueRef<'a> {
    /// A SQL `null`.
    Null,
    /// A signed integer.
    I64(&'a i64),
    /// An unsigned integer.
    U64(&'a u64),
    /// A floating-point value.
    F64(&'a f64),
    /// A date.
    Date(DateValue),
    /// A time of day.
    Time(TimeValue),
    /// A date and time without an offset.
    DateTime(DateTimeValue),
    /// A date and time with an offset.
    DateTimeTz(DateTimeTzValue),
    /// A 16-byte uuid.
    Uuid(&'a [u8; 16]),
    /// Binary data.
    Bytes(&'a [u8]),
    /// Text data.
    String(&'a str),
}

/// A borrowed source of positional mysql or mariadb parameters.
pub trait ParamSource {
    /// Number of parameters in the source.
    fn len(&self) -> usize;

    /// Returns whether the source has no parameters.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the parameter at `index`.
    fn value_at(&self, index: usize) -> ValueRef<'_>;
}

pub(super) struct ParamRefSlice<'a>(pub(super) &'a [ValueRef<'a>]);

impl ParamSource for ParamRefSlice<'_> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn value_at(&self, index: usize) -> ValueRef<'_> {
        self.0[index]
    }
}

pub(super) fn values_as_refs(values: &[Value]) -> Vec<ValueRef<'_>> {
    values
        .iter()
        .map(|value| match value {
            Value::Null => ValueRef::Null,
            Value::I64(value) => ValueRef::I64(value),
            Value::U64(value) => ValueRef::U64(value),
            Value::F64(value) => ValueRef::F64(value),
            Value::Date(value) => ValueRef::Date(*value),
            Value::Time(value) => ValueRef::Time(*value),
            Value::DateTime(value) => ValueRef::DateTime(*value),
            Value::DateTimeTz(value) => ValueRef::DateTimeTz(*value),
            Value::Uuid(value) => ValueRef::Uuid(value),
            Value::Bytes(value) => ValueRef::Bytes(value),
            Value::String(value) => ValueRef::String(value),
        })
        .collect()
}
