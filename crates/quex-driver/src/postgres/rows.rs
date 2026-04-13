use std::ffi::CStr;
use std::slice;
use std::str;
use std::sync::Arc;

use quex_pq_sys as ffi;

use super::error::{Error, Result};
use super::runtime::{
    ResultHandle, decode_value, parse_binary_date, parse_binary_datetime, parse_binary_datetimetz,
    parse_binary_f64, parse_binary_i64, parse_binary_time, parse_binary_u64, parse_binary_uuid,
    parse_i64_ascii, parse_number, parse_text_date, parse_text_datetime, parse_text_datetimetz,
    parse_text_time, parse_text_uuid, parse_u64_ascii,
};
use super::value::{DateTimeTzValue, DateTimeValue, DateValue, TimeValue, Value};

/// Metadata for one postgres result column.
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// postgres type oid.
    pub type_oid: u32,
    /// postgres result format. `0` is text and `1` is binary.
    pub format: i32,
    /// Whether the column may contain `null`.
    ///
    /// libpq result metadata does not reliably report this for arbitrary
    /// queries, so this is currently conservative.
    pub nullable: bool,
}

#[derive(Clone)]
pub(crate) struct Metadata {
    pub(crate) columns: Vec<Column>,
    pub(crate) name_order: Box<[usize]>,
}

impl Metadata {
    pub(crate) fn from_result(result: ResultHandle) -> Self {
        // SAFETY: result is a live PG result owned by the caller.
        unsafe {
            let count = ffi::PQnfields(result.as_ptr()) as usize;
            let mut columns = Vec::with_capacity(count);
            for index in 0..count {
                let name_ptr = ffi::PQfname(result.as_ptr(), index as i32);
                let name = if name_ptr.is_null() {
                    String::new()
                } else {
                    CStr::from_ptr(name_ptr).to_string_lossy().into_owned()
                };
                columns.push(Column {
                    name,
                    type_oid: ffi::PQftype(result.as_ptr(), index as i32) as u32,
                    format: ffi::PQfformat(result.as_ptr(), index as i32),
                    nullable: true,
                });
            }

            let mut name_order: Vec<usize> = (0..count).collect();
            name_order
                .sort_unstable_by(|&left, &right| columns[left].name.cmp(&columns[right].name));

            Self {
                columns,
                name_order: name_order.into_boxed_slice(),
            }
        }
    }
}

/// A forward-only postgres result set.
pub struct ResultSet {
    result: Option<ResultHandle>,
    metadata: Arc<Metadata>,
    row_count: i32,
    next_row: i32,
}

impl ResultSet {
    pub(crate) fn new(result: ResultHandle, metadata: Arc<Metadata>) -> Self {
        // SAFETY: result is a live PG result owned by the ResultSet.
        let row_count = unsafe { ffi::PQntuples(result.as_ptr()) };
        Self {
            result: Some(result),
            metadata,
            row_count,
            next_row: 0,
        }
    }

    #[inline]
    /// Returns column metadata for this result set.
    pub fn columns(&self) -> &[Column] {
        &self.metadata.columns
    }

    /// Advances to the next row.
    ///
    /// The returned row is borrowed from this result set and is valid until the
    /// next call to `next`.
    pub async fn next(&mut self) -> Result<Option<RowRef<'_>>> {
        if self.next_row >= self.row_count {
            return Ok(None);
        }
        let row = self.next_row;
        self.next_row += 1;
        Ok(Some(RowRef {
            result: self.result.expect("result handle missing"),
            metadata: &self.metadata,
            row,
        }))
    }
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        if let Some(result) = self.result.take() {
            // SAFETY: result was returned by libpq and is cleared exactly once here.
            unsafe {
                ffi::PQclear(result.as_ptr());
            }
        }
    }
}

/// A borrowed postgres row.
///
/// The row is valid until the owning [`ResultSet`] advances or is dropped.
pub struct RowRef<'a> {
    result: ResultHandle,
    metadata: &'a Metadata,
    row: i32,
}

impl<'a> RowRef<'a> {
    #[inline]
    /// Returns whether a column is SQL `null`.
    pub fn is_null(&self, index: impl ColumnIndex) -> Result<bool> {
        let index = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        // SAFETY: row and column indexes are bounds-checked by ResultSet and ColumnIndex.
        Ok(unsafe { ffi::PQgetisnull(self.result.as_ptr(), self.row, index as i32) != 0 })
    }

    #[inline]
    /// Reads a column as UTF-8 text.
    pub fn get_str(&self, index: impl ColumnIndex) -> Result<&str> {
        str::from_utf8(self.get_bytes(index)?).map_err(|err| Error::new(err.to_string()))
    }

    #[inline]
    /// Reads a column as bytes.
    pub fn get_bytes(&self, index: impl ColumnIndex) -> Result<&[u8]> {
        let index = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        // SAFETY: row and column indexes are bounds-checked by ResultSet and ColumnIndex.
        if unsafe { ffi::PQgetisnull(self.result.as_ptr(), self.row, index as i32) } != 0 {
            return Err(Error::new("column is null"));
        }

        // SAFETY: libpq returns a pointer valid while the result handle is live.
        let ptr = unsafe { ffi::PQgetvalue(self.result.as_ptr(), self.row, index as i32) };
        // SAFETY: row and column indexes are valid for this result.
        let len =
            unsafe { ffi::PQgetlength(self.result.as_ptr(), self.row, index as i32) } as usize;
        // SAFETY: ptr and len describe the cell bytes owned by the live PG result.
        Ok(unsafe { pq_bytes(ptr.cast::<u8>(), len) })
    }

    #[inline]
    /// Reads a column as `i64`.
    pub fn get_i64(&self, index: impl ColumnIndex) -> Result<i64> {
        let index = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        let column = &self.metadata.columns[index];
        let bytes = self.get_bytes(index)?;
        if column.format == 1 {
            parse_binary_i64(bytes, column.type_oid)
        } else {
            parse_i64_ascii(bytes)
        }
    }

    #[inline]
    /// Reads a column as `u64`.
    pub fn get_u64(&self, index: impl ColumnIndex) -> Result<u64> {
        let index = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        let column = &self.metadata.columns[index];
        let bytes = self.get_bytes(index)?;
        if column.format == 1 {
            parse_binary_u64(bytes, column.type_oid)
        } else {
            parse_u64_ascii(bytes)
        }
    }

    #[inline]
    /// Reads a column as `f64`.
    pub fn get_f64(&self, index: impl ColumnIndex) -> Result<f64> {
        let index = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        let column = &self.metadata.columns[index];
        let bytes = self.get_bytes(index)?;
        if column.format == 1 {
            parse_binary_f64(bytes, column.type_oid)
        } else {
            parse_number::<f64>(bytes, "f64")
        }
    }

    #[inline]
    /// Reads a column as a date.
    pub fn get_date(&self, index: impl ColumnIndex) -> Result<DateValue> {
        let index = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        let column = &self.metadata.columns[index];
        let bytes = self.get_bytes(index)?;
        if column.format == 1 {
            parse_binary_date(bytes)
        } else {
            parse_text_date(bytes)
        }
    }

    #[inline]
    /// Reads a column as a time of day.
    pub fn get_time_value(&self, index: impl ColumnIndex) -> Result<TimeValue> {
        let index = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        let column = &self.metadata.columns[index];
        let bytes = self.get_bytes(index)?;
        if column.format == 1 {
            parse_binary_time(bytes)
        } else {
            parse_text_time(bytes)
        }
    }

    #[inline]
    /// Reads a column as a date and time.
    pub fn get_datetime(&self, index: impl ColumnIndex) -> Result<DateTimeValue> {
        let index = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        let column = &self.metadata.columns[index];
        let bytes = self.get_bytes(index)?;
        if column.format == 1 {
            parse_binary_datetime(bytes)
        } else {
            parse_text_datetime(bytes)
        }
    }

    #[inline]
    /// Reads a column as a date and time with offset.
    pub fn get_datetimetz(&self, index: impl ColumnIndex) -> Result<DateTimeTzValue> {
        let index = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        let column = &self.metadata.columns[index];
        let bytes = self.get_bytes(index)?;
        if column.format == 1 {
            parse_binary_datetimetz(bytes)
        } else {
            parse_text_datetimetz(bytes)
        }
    }

    #[inline]
    /// Reads a column as a 16-byte uuid.
    pub fn get_uuid(&self, index: impl ColumnIndex) -> Result<[u8; 16]> {
        let index = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        let column = &self.metadata.columns[index];
        let bytes = self.get_bytes(index)?;
        if column.format == 1 {
            parse_binary_uuid(bytes)
        } else {
            parse_text_uuid(bytes)
        }
    }

    /// Copies this row into owned column metadata and values.
    pub fn to_owned(&self) -> Result<Row> {
        let mut values = Vec::with_capacity(self.metadata.columns.len());
        for (index, column) in self.metadata.columns.iter().enumerate() {
            let value = if self.is_null(index)? {
                Value::Null
            } else {
                decode_value(column, self.get_bytes(index)?)?
            };
            values.push(value);
        }

        Ok(Row {
            columns: self.metadata.columns.clone(),
            values,
        })
    }
}

pub(crate) unsafe fn pq_bytes<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if len == 0 {
        &[]
    } else {
        debug_assert!(!ptr.is_null());
        // SAFETY: callers guarantee `ptr` points to `len` live bytes owned by the current PGresult.
        unsafe { slice::from_raw_parts(ptr, len) }
    }
}

/// An owned postgres row.
#[derive(Debug, Clone)]
pub struct Row {
    /// Column metadata in result order.
    pub columns: Vec<Column>,
    /// Values in the same order as [`Self::columns`].
    pub values: Vec<Value>,
}

/// A column lookup accepted by postgres row accessors.
pub trait ColumnIndex {
    /// Resolves the lookup to a zero-based column index.
    fn index(&self, columns: &[Column], name_order: &[usize]) -> Result<usize>;
}

impl ColumnIndex for usize {
    fn index(&self, columns: &[Column], _name_order: &[usize]) -> Result<usize> {
        if *self < columns.len() {
            Ok(*self)
        } else {
            Err(Error::new(format!("column {} out of bounds", self)))
        }
    }
}

impl ColumnIndex for &str {
    fn index(&self, columns: &[Column], name_order: &[usize]) -> Result<usize> {
        match name_order.binary_search_by(|&idx| columns[idx].name.as_str().cmp(self)) {
            Ok(pos) => Ok(name_order[pos]),
            Err(_) => Err(Error::new(format!("unknown column {}", self))),
        }
    }
}

// SAFETY: `ResultSet` owns the `PGresult` backing all row views and can only be advanced through a
// mutable borrow.
unsafe impl Send for ResultSet {}
