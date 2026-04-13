use std::sync::Arc;

use super::error::{Error, Result};
use super::value::Value;

/// Metadata for one sqlite result column.
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// Declared sqlite type, when one is available.
    pub declared_type: Option<String>,
    /// Whether the column may contain `null`.
    pub nullable: bool,
}

/// An owned sqlite row.
#[derive(Debug, Clone)]
pub struct Row {
    /// Column metadata in result order.
    pub columns: Vec<Column>,
    /// Values in the same order as [`Self::columns`].
    pub values: Vec<Value>,
}

impl Row {
    #[inline]
    /// Reads a column as `i64`.
    pub fn get_i64(&self, index: impl ColumnIndex) -> Result<i64> {
        match self.values.get(index.index(&self.columns)?) {
            Some(Value::I64(value)) => Ok(*value),
            Some(Value::Null) => Err(Error::new("column is null")),
            Some(other) => Err(Error::new(format!("column is not i64: {other:?}"))),
            None => Err(Error::new("column index out of bounds")),
        }
    }

    #[inline]
    /// Reads a column as `f64`.
    pub fn get_f64(&self, index: impl ColumnIndex) -> Result<f64> {
        match self.values.get(index.index(&self.columns)?) {
            Some(Value::F64(value)) => Ok(*value),
            Some(Value::I64(value)) => Ok(*value as f64),
            Some(Value::Null) => Err(Error::new("column is null")),
            Some(other) => Err(Error::new(format!("column is not f64: {other:?}"))),
            None => Err(Error::new("column index out of bounds")),
        }
    }

    #[inline]
    /// Reads a text column.
    pub fn get_str(&self, index: impl ColumnIndex) -> Result<&str> {
        match self.values.get(index.index(&self.columns)?) {
            Some(Value::String(value)) => Ok(value),
            Some(Value::Null) => Err(Error::new("column is null")),
            Some(other) => Err(Error::new(format!("column is not text: {other:?}"))),
            None => Err(Error::new("column index out of bounds")),
        }
    }

    #[inline]
    /// Reads a byte column.
    ///
    /// Text values can also be read as bytes.
    pub fn get_bytes(&self, index: impl ColumnIndex) -> Result<&[u8]> {
        match self.values.get(index.index(&self.columns)?) {
            Some(Value::Bytes(value)) => Ok(value),
            Some(Value::String(value)) => Ok(value.as_bytes()),
            Some(Value::Null) => Err(Error::new("column is null")),
            Some(other) => Err(Error::new(format!("column is not bytes: {other:?}"))),
            None => Err(Error::new("column index out of bounds")),
        }
    }
}

/// A borrowed sqlite row.
///
/// Rows are backed by owned values inside the result set, so converting to an
/// owned row is a cheap clone of the current row data.
pub struct RowRef<'a> {
    columns: &'a [Column],
    values: &'a [Value],
}

impl<'a> RowRef<'a> {
    #[inline]
    /// Reads a column as `i64`.
    pub fn get_i64(&self, index: impl ColumnIndex) -> Result<i64> {
        match self.values.get(index.index(self.columns)?) {
            Some(Value::I64(value)) => Ok(*value),
            Some(Value::Null) => Err(Error::new("column is null")),
            Some(other) => Err(Error::new(format!("column is not i64: {other:?}"))),
            None => Err(Error::new("column index out of bounds")),
        }
    }

    #[inline]
    /// Reads a column as `f64`.
    pub fn get_f64(&self, index: impl ColumnIndex) -> Result<f64> {
        match self.values.get(index.index(self.columns)?) {
            Some(Value::F64(value)) => Ok(*value),
            Some(Value::I64(value)) => Ok(*value as f64),
            Some(Value::Null) => Err(Error::new("column is null")),
            Some(other) => Err(Error::new(format!("column is not f64: {other:?}"))),
            None => Err(Error::new("column index out of bounds")),
        }
    }

    #[inline]
    /// Reads a text column.
    pub fn get_str(&self, index: impl ColumnIndex) -> Result<&str> {
        match self.values.get(index.index(self.columns)?) {
            Some(Value::String(value)) => Ok(value),
            Some(Value::Null) => Err(Error::new("column is null")),
            Some(other) => Err(Error::new(format!("column is not text: {other:?}"))),
            None => Err(Error::new("column index out of bounds")),
        }
    }

    #[inline]
    /// Reads a byte column.
    ///
    /// Text values can also be read as bytes.
    pub fn get_bytes(&self, index: impl ColumnIndex) -> Result<&[u8]> {
        match self.values.get(index.index(self.columns)?) {
            Some(Value::Bytes(value)) => Ok(value),
            Some(Value::String(value)) => Ok(value.as_bytes()),
            Some(Value::Null) => Err(Error::new("column is null")),
            Some(other) => Err(Error::new(format!("column is not bytes: {other:?}"))),
            None => Err(Error::new("column index out of bounds")),
        }
    }

    #[inline]
    /// Copies this row into owned column metadata and values.
    pub fn to_owned(&self) -> Row {
        Row {
            columns: self.columns.to_vec(),
            values: self.values.to_vec(),
        }
    }
}

/// A column lookup accepted by sqlite row accessors.
pub trait ColumnIndex {
    /// Resolves the lookup to a zero-based column index.
    fn index(&self, columns: &[Column]) -> Result<usize>;
}

impl ColumnIndex for usize {
    fn index(&self, columns: &[Column]) -> Result<usize> {
        if *self < columns.len() {
            Ok(*self)
        } else {
            Err(Error::new(format!("column {} out of bounds", self)))
        }
    }
}

impl ColumnIndex for &str {
    fn index(&self, columns: &[Column]) -> Result<usize> {
        columns
            .iter()
            .position(|column| column.name == *self)
            .ok_or_else(|| Error::new(format!("unknown column {}", self)))
    }
}

/// A forward-only sqlite result set.
pub struct ResultSet {
    columns: Arc<[Column]>,
    rows: Vec<Vec<Value>>,
    pos: usize,
}

impl ResultSet {
    pub(crate) fn new(columns: Vec<Column>, rows: Vec<Vec<Value>>) -> Self {
        Self {
            columns: Arc::from(columns),
            rows,
            pos: 0,
        }
    }

    #[inline]
    /// Returns column metadata for this result set.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Advances to the next row.
    ///
    /// The returned row is borrowed from this result set and is valid until the
    /// next call to `next`.
    pub async fn next(&mut self) -> Result<Option<RowRef<'_>>> {
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let values = &self.rows[self.pos];
        self.pos += 1;
        Ok(Some(RowRef {
            columns: &self.columns,
            values,
        }))
    }
}
