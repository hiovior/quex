/// An owned sqlite value.
#[derive(Debug, Clone)]
pub enum Value {
    /// A SQL `null`.
    Null,
    /// A signed integer.
    I64(i64),
    /// A floating-point value.
    F64(f64),
    /// Binary data.
    Bytes(Vec<u8>),
    /// Text data.
    String(String),
}
