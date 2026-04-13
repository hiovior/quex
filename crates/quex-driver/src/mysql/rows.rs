use std::ptr::NonNull;
use std::str;
use std::sync::Arc;

use quex_mariadb_sys as ffi;

use super::error::{Error, Result};
use super::runtime::{
    NOT_NULL_FLAG, ResultHandle, StmtHandle, bind_buffer_type, c_opt_string, cell_bytes,
    column_is_unsigned, decode_statement_value, decode_value, mysql_bind_init, mysql_cell_bytes,
    mysql_time_to_date, mysql_time_to_datetime, mysql_time_to_time, parse_i64_ascii,
    parse_mysql_date_text, parse_mysql_datetime_text, parse_mysql_time_text, parse_number,
    parse_statement_mysql_time, parse_u64_ascii, statement_buffer_len_for_column, statement_f64,
    statement_i64, statement_u64,
};
use super::value::{DateTimeValue, DateValue, TimeValue, Value};

/// Metadata for one mysql or mariadb result column.
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// Native mysql column type code.
    pub column_type: u32,
    /// Native mysql field flags.
    pub flags: u32,
    /// Declared or measured column length reported by libmariadb.
    pub declared_length: u64,
    /// Whether the column may contain `null`.
    pub nullable: bool,
}

impl Column {
    pub(crate) fn from_field(field: &ffi::MYSQL_FIELD) -> Self {
        let name = c_opt_string(field.name).unwrap_or_default();
        let declared_length = if field.max_length > 0 {
            field.max_length
        } else {
            field.length
        };

        Self {
            name,
            column_type: field.type_,
            flags: field.flags,
            declared_length,
            nullable: (field.flags & NOT_NULL_FLAG) == 0,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Metadata {
    pub(crate) columns: Vec<Column>,
    pub(crate) name_order: Box<[usize]>,
}

impl Metadata {
    pub(crate) fn from_result(result: NonNull<ffi::MYSQL_RES>) -> Self {
        // SAFETY: result is a live MYSQL_RES and fields remain valid while metadata is copied.
        unsafe {
            let count = ffi::mysql_num_fields(result.as_ptr()) as usize;
            let fields = ffi::mysql_fetch_fields(result.as_ptr());
            let mut columns = Vec::with_capacity(count);
            for idx in 0..count {
                columns.push(Column::from_field(&*fields.add(idx)));
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

/// A forward-only mysql or mariadb result set.
pub struct ResultSet {
    metadata: Arc<Metadata>,
    inner: ResultSetInner,
}

enum ResultSetInner {
    Empty,
    Text {
        result: ResultHandle,
        row: Vec<Cell>,
    },
    Statement(StatementResultState),
}

pub(crate) struct StatementResultState {
    stmt: StmtHandle,
    _binds: Vec<ffi::MYSQL_BIND>,
    slots: Vec<StatementCell>,
}

pub(crate) struct StatementCell {
    pub(crate) buffer: Vec<u8>,
    pub(crate) length: u64,
    pub(crate) is_null: ffi::my_bool,
    pub(crate) error: ffi::my_bool,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct Cell {
    pub(crate) ptr: *const u8,
    pub(crate) len: usize,
    pub(crate) is_null: bool,
}

impl ResultSet {
    pub(crate) fn empty() -> Self {
        Self {
            metadata: Metadata {
                columns: Vec::new(),
                name_order: Box::new([]),
            }
            .into(),
            inner: ResultSetInner::Empty,
        }
    }

    pub(crate) fn text(result: NonNull<ffi::MYSQL_RES>, metadata: Arc<Metadata>) -> Self {
        let row = vec![Cell::default(); metadata.columns.len()];
        Self {
            metadata,
            inner: ResultSetInner::Text {
                result: ResultHandle(result),
                row,
            },
        }
    }

    pub(crate) fn statement(stmt: StmtHandle, metadata: Arc<Metadata>) -> Result<Self> {
        let mut slots = Vec::with_capacity(metadata.columns.len());
        let mut binds = Vec::with_capacity(metadata.columns.len());

        for column in &metadata.columns {
            slots.push(StatementCell {
                buffer: vec![0; statement_buffer_len_for_column(column)],
                length: 0,
                is_null: 0,
                error: 0,
            });
            binds.push(mysql_bind_init());
        }

        for (bind, (column, slot)) in binds
            .iter_mut()
            .zip(metadata.columns.iter().zip(slots.iter_mut()))
        {
            bind.buffer_type = bind_buffer_type(column.column_type);
            bind.buffer = slot.buffer.as_mut_ptr().cast();
            bind.buffer_length = slot.buffer.len() as u64;
            bind.length = &mut slot.length;
            bind.is_null = &mut slot.is_null;
            bind.error = &mut slot.error;
            bind.is_unsigned = column_is_unsigned(column.flags);
        }

        // SAFETY: stmt is live and bind pointers refer to slots kept in ResultSet state.
        unsafe {
            if ffi::mysql_stmt_bind_result(stmt.as_ptr(), binds.as_mut_ptr()) != 0 {
                return Err(Error::from_stmt(
                    stmt.as_ptr(),
                    "mysql_stmt_bind_result failed",
                ));
            }
        }

        Ok(Self {
            metadata,
            inner: ResultSetInner::Statement(StatementResultState {
                stmt,
                _binds: binds,
                slots,
            }),
        })
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
        match &mut self.inner {
            ResultSetInner::Empty => Ok(None),
            ResultSetInner::Text { result, row } => {
                // SAFETY: result is a live MYSQL_RES and row pointers remain valid until next fetch.
                unsafe {
                    let raw_row = ffi::mysql_fetch_row(result.as_ptr());
                    if raw_row.is_null() {
                        return Ok(None);
                    }

                    let lengths = ffi::mysql_fetch_lengths(result.as_ptr());
                    let field_count = self.metadata.columns.len();
                    row.clear();
                    row.reserve(field_count.saturating_sub(row.capacity()));
                    for idx in 0..field_count {
                        let cell = *raw_row.add(idx);
                        if cell.is_null() {
                            row.push(Cell::default());
                        } else {
                            row.push(Cell {
                                ptr: cell.cast(),
                                len: *lengths.add(idx) as usize,
                                is_null: false,
                            });
                        }
                    }

                    Ok(Some(RowRef {
                        metadata: &self.metadata,
                        row: RowRefInner::Text(row),
                    }))
                }
            }
            ResultSetInner::Statement(state) => {
                // SAFETY: state owns the live statement and result buffers bound to it.
                unsafe {
                    let fetch = ffi::mysql_stmt_fetch(state.stmt.as_ptr());
                    if fetch == ffi::MYSQL_NO_DATA as i32 {
                        return Ok(None);
                    }
                    if fetch != 0 && fetch != ffi::MYSQL_DATA_TRUNCATED as i32 {
                        return Err(Error::from_stmt(
                            state.stmt.as_ptr(),
                            "mysql_stmt_fetch failed",
                        ));
                    }
                    if fetch == ffi::MYSQL_DATA_TRUNCATED as i32 {
                        return Err(Error::new(
                            "statement result buffer was truncated; max_length sizing was insufficient",
                        ));
                    }

                    Ok(Some(RowRef {
                        metadata: &self.metadata,
                        row: RowRefInner::Statement(&state.slots),
                    }))
                }
            }
        }
    }
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        // SAFETY: Result resources are owned by this ResultSet and freed exactly once here.
        unsafe {
            match &self.inner {
                ResultSetInner::Empty => {}
                ResultSetInner::Text { result, .. } => ffi::mysql_free_result(result.as_ptr()),
                ResultSetInner::Statement(state) => {
                    let _ = ffi::mysql_stmt_free_result(state.stmt.as_ptr());
                }
            }
        }
    }
}

/// A borrowed mysql or mariadb row.
///
/// The row is valid until the owning [`ResultSet`] advances.
pub struct RowRef<'a> {
    pub(crate) metadata: &'a Metadata,
    pub(crate) row: RowRefInner<'a>,
}

impl<'a> RowRef<'a> {
    #[inline]
    /// Returns whether a column is SQL `null`.
    pub fn is_null(&self, index: impl ColumnIndex) -> Result<bool> {
        let idx = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        Ok(match self.row {
            RowRefInner::Text(row) => row[idx].is_null,
            RowRefInner::Statement(row) => row[idx].is_null != 0,
        })
    }

    #[inline]
    /// Reads a column as UTF-8 text.
    pub fn get_str(&self, index: impl ColumnIndex) -> Result<&str> {
        str::from_utf8(self.get_bytes(index)?).map_err(|err| Error::new(err.to_string()))
    }

    #[inline]
    /// Reads a column as bytes.
    pub fn get_bytes(&self, index: impl ColumnIndex) -> Result<&[u8]> {
        let idx = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        match self.row {
            RowRefInner::Text(row) => {
                let cell = &row[idx];
                if cell.is_null {
                    Err(Error::new("column is null"))
                } else {
                    // SAFETY: Text row cell points into the live MYSQL_RES for this RowRef.
                    Ok(unsafe { mysql_cell_bytes(cell.ptr, cell.len) })
                }
            }
            RowRefInner::Statement(row) => {
                let cell = &row[idx];
                if cell.is_null != 0 {
                    Err(Error::new("column is null"))
                } else {
                    Ok(&cell.buffer[..cell.length as usize])
                }
            }
        }
    }

    #[inline]
    /// Reads a column as `i64`.
    pub fn get_i64(&self, index: impl ColumnIndex) -> Result<i64> {
        let idx = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        match self.row {
            RowRefInner::Text(row) => parse_i64_ascii(cell_bytes(&row[idx])?),
            RowRefInner::Statement(row) => statement_i64(&row[idx], &self.metadata.columns[idx]),
        }
    }

    #[inline]
    /// Reads a column as `u64`.
    pub fn get_u64(&self, index: impl ColumnIndex) -> Result<u64> {
        let idx = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        match self.row {
            RowRefInner::Text(row) => parse_u64_ascii(cell_bytes(&row[idx])?),
            RowRefInner::Statement(row) => statement_u64(&row[idx], &self.metadata.columns[idx]),
        }
    }

    #[inline]
    /// Reads a column as `f64`.
    pub fn get_f64(&self, index: impl ColumnIndex) -> Result<f64> {
        let idx = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        match self.row {
            RowRefInner::Text(row) => parse_number::<f64>(cell_bytes(&row[idx])?, "f64"),
            RowRefInner::Statement(row) => statement_f64(&row[idx], &self.metadata.columns[idx]),
        }
    }

    #[inline]
    /// Reads a column as a date.
    pub fn get_date(&self, index: impl ColumnIndex) -> Result<DateValue> {
        let idx = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        match self.row {
            RowRefInner::Text(row) => parse_mysql_date_text(cell_bytes(&row[idx])?),
            RowRefInner::Statement(row) => {
                parse_statement_mysql_time(&row[idx]).map(mysql_time_to_date)
            }
        }
    }

    #[inline]
    /// Reads a column as a time of day.
    pub fn get_time_value(&self, index: impl ColumnIndex) -> Result<TimeValue> {
        let idx = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        match self.row {
            RowRefInner::Text(row) => parse_mysql_time_text(cell_bytes(&row[idx])?),
            RowRefInner::Statement(row) => {
                parse_statement_mysql_time(&row[idx]).map(mysql_time_to_time)
            }
        }
    }

    #[inline]
    /// Reads a column as a date and time.
    pub fn get_datetime(&self, index: impl ColumnIndex) -> Result<DateTimeValue> {
        let idx = index.index(&self.metadata.columns, &self.metadata.name_order)?;
        match self.row {
            RowRefInner::Text(row) => parse_mysql_datetime_text(cell_bytes(&row[idx])?),
            RowRefInner::Statement(row) => {
                parse_statement_mysql_time(&row[idx]).map(mysql_time_to_datetime)
            }
        }
    }

    /// Copies this row into owned column metadata and values.
    pub fn to_owned(&self) -> Result<Row> {
        let mut values = Vec::with_capacity(self.metadata.columns.len());
        match self.row {
            RowRefInner::Text(_) => {
                for (index, column) in self.metadata.columns.iter().enumerate() {
                    let value = match self.cell_by_index(index).bytes() {
                        None => Value::Null,
                        Some(bytes) => decode_value(column, bytes)?,
                    };
                    values.push(value);
                }
            }
            RowRefInner::Statement(row) => {
                for (cell, column) in row.iter().zip(&self.metadata.columns) {
                    values.push(decode_statement_value(column, cell)?);
                }
            }
        }

        Ok(Row {
            columns: self.metadata.columns.clone(),
            values,
        })
    }

    #[inline]
    fn cell_by_index(&self, index: usize) -> RowCellRef<'_> {
        match self.row {
            RowRefInner::Text(row) => RowCellRef::Text(&row[index]),
            RowRefInner::Statement(row) => RowCellRef::Statement(&row[index]),
        }
    }
}

/// An owned mysql or mariadb row.
#[derive(Debug, Clone)]
pub struct Row {
    /// Column metadata in result order.
    pub columns: Vec<Column>,
    /// Values in the same order as [`Self::columns`].
    pub values: Vec<Value>,
}

/// A column lookup accepted by mysql and mariadb row accessors.
pub trait ColumnIndex {
    /// Resolves the lookup to a zero-based column index.
    fn index(&self, columns: &[Column], name_order: &[usize]) -> Result<usize>;
}

impl ColumnIndex for usize {
    #[inline]
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

#[derive(Clone, Copy)]
pub(crate) enum RowRefInner<'a> {
    Text(&'a [Cell]),
    Statement(&'a [StatementCell]),
}

enum RowCellRef<'a> {
    Text(&'a Cell),
    Statement(&'a StatementCell),
}

impl RowCellRef<'_> {
    #[inline]
    fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Text(cell) => {
                if cell.is_null {
                    None
                } else {
                    // SAFETY: Text row cell points into the live MYSQL_RES for this RowRef.
                    Some(unsafe { mysql_cell_bytes(cell.ptr, cell.len) })
                }
            }
            Self::Statement(cell) => {
                if cell.is_null != 0 {
                    None
                } else {
                    Some(&cell.buffer[..cell.length as usize])
                }
            }
        }
    }
}

// SAFETY: `Cell` is a plain pointer/length pair copied out of libmariadb row metadata; sending it
// moves the descriptor only. The pointed-to bytes remain owned by the live result set.
unsafe impl Send for Cell {}
// SAFETY: Statement result state owns its buffers and statement handle and moves together with the
// owning `ResultSet`.
unsafe impl Send for StatementResultState {}
// SAFETY: `ResultSet` owns the result resources backing its row views and can only be advanced via
// unique borrows.
unsafe impl Send for ResultSet {}
