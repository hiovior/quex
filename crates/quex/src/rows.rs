#![cfg_attr(
    not(any(feature = "mysql", feature = "postgres", feature = "sqlite")),
    allow(unused_imports, unused_variables)
)]

use crate::{
    Column, ColumnIndex, Date, DateTime, DateTimeTz, Error, FromColumnRef, FromRow, FromRowRef,
    Result, Row, RowStream, Time,
};
#[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlite"))]
use crate::ColumnType;
#[cfg(any(feature = "mysql", feature = "postgres", feature = "sqlite"))]
use crate::Value;

/// A forward-only stream of rows returned by a query.
///
/// Each [`RowRef`] borrowed from the stream is valid until the next call to
/// [`Rows::next`].
///
/// Use [`Rows::collect_owned`] when the rows need to outlive the stream, or
/// [`Rows::collect_decoded`] when you want to decode everything into owned
/// application types.
pub struct Rows<'a> {
    columns: Vec<Column>,
    inner: RowsInner<'a>,
}

impl RowStream for Rows<'_> {
    fn columns(&self) -> &[Column] {
        self.columns()
    }

    async fn next(&mut self) -> Result<Option<RowRef<'_>>> {
        Rows::next(self).await
    }
}

enum RowsInner<'a> {
    #[cfg(feature = "mysql")]
    Mysql(quex_driver::mysql::ResultSet),
    #[cfg(feature = "postgres")]
    Postgres(quex_driver::postgres::ResultSet),
    #[cfg(feature = "sqlite")]
    Sqlite(quex_driver::sqlite::ResultSet),
    _Marker(std::marker::PhantomData<&'a ()>),
}

impl<'a> Rows<'a> {
    pub(crate) fn into_lifetime<'b>(self) -> Rows<'b> {
        let inner = match self.inner {
            #[cfg(feature = "mysql")]
            RowsInner::Mysql(rows) => RowsInner::Mysql(rows),
            #[cfg(feature = "postgres")]
            RowsInner::Postgres(rows) => RowsInner::Postgres(rows),
            #[cfg(feature = "sqlite")]
            RowsInner::Sqlite(rows) => RowsInner::Sqlite(rows),
            RowsInner::_Marker(_) => RowsInner::_Marker(std::marker::PhantomData),
        };
        Rows {
            columns: self.columns,
            inner,
        }
    }

    #[cfg(feature = "mysql")]
    pub(crate) fn mysql(rows: quex_driver::mysql::ResultSet) -> Self {
        let columns = rows
            .columns()
            .iter()
            .map(|column| Column {
                name: column.name.clone(),
                nullable: column.nullable,
                kind: ColumnType::Mysql(column.column_type),
            })
            .collect();
        Self {
            columns,
            inner: RowsInner::Mysql(rows),
        }
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn postgres(rows: quex_driver::postgres::ResultSet) -> Self {
        let columns = rows
            .columns()
            .iter()
            .map(|column| Column {
                name: column.name.clone(),
                nullable: column.nullable,
                kind: ColumnType::Postgres(column.type_oid),
            })
            .collect();
        Self {
            columns,
            inner: RowsInner::Postgres(rows),
        }
    }

    #[cfg(feature = "sqlite")]
    pub(crate) fn sqlite(rows: quex_driver::sqlite::ResultSet) -> Self {
        let columns = rows
            .columns()
            .iter()
            .map(|column| Column {
                name: column.name.clone(),
                nullable: column.nullable,
                kind: ColumnType::Sqlite(column.declared_type.clone()),
            })
            .collect();
        Self {
            columns,
            inner: RowsInner::Sqlite(rows),
        }
    }

    /// Returns metadata for the result columns.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Advances the stream by one row.
    ///
    /// Returns `Ok(None)` when there are no rows left.
    pub async fn next(&mut self) -> Result<Option<RowRef<'_>>> {
        match &mut self.inner {
            #[cfg(feature = "mysql")]
            RowsInner::Mysql(rows) => Ok(rows.next().await?.map(|row| RowRef {
                columns: &self.columns,
                inner: RowRefInner::Mysql(row),
            })),
            #[cfg(feature = "postgres")]
            RowsInner::Postgres(rows) => Ok(rows.next().await?.map(|row| RowRef {
                columns: &self.columns,
                inner: RowRefInner::Postgres(row),
            })),
            #[cfg(feature = "sqlite")]
            RowsInner::Sqlite(rows) => Ok(rows.next().await?.map(|row| RowRef {
                columns: &self.columns,
                inner: RowRefInner::Sqlite(row),
            })),
            RowsInner::_Marker(_) => Ok(None),
        }
    }

    /// Collects the remaining rows into owned rows.
    ///
    /// Use this when rows need to outlive the stream.
    pub async fn collect_owned(mut self) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        while let Some(row) = self.next().await? {
            rows.push(row.to_owned()?);
        }
        Ok(rows)
    }

    /// Advances the stream and decodes the next row.
    pub async fn next_decoded<T>(&mut self) -> Result<Option<T>>
    where
        T: FromRow,
    {
        match self.next().await? {
            Some(row) => Ok(Some(row.decode()?)),
            None => Ok(None),
        }
    }

    /// Collects and decodes all remaining rows.
    pub async fn collect_decoded<T>(mut self) -> Result<Vec<T>>
    where
        T: FromRow,
    {
        let mut rows = Vec::new();
        while let Some(row) = self.next().await? {
            rows.push(row.decode()?);
        }
        Ok(rows)
    }
}

/// A borrowed row from a result stream.
///
/// The row is valid until the stream advances again.
///
/// Use [`RowRef::decode_ref`] for borrowed application types and
/// [`RowRef::to_owned`] when you need an owned [`crate::Row`].
pub struct RowRef<'a> {
    columns: &'a [Column],
    inner: RowRefInner<'a>,
}

enum RowRefInner<'a> {
    #[cfg(feature = "mysql")]
    Mysql(quex_driver::mysql::RowRef<'a>),
    #[cfg(feature = "postgres")]
    Postgres(quex_driver::postgres::RowRef<'a>),
    #[cfg(feature = "sqlite")]
    Sqlite(quex_driver::sqlite::RowRef<'a>),
    _Marker(std::marker::PhantomData<&'a ()>),
}

impl<'a> RowRef<'a> {
    /// Reads and decodes a column by index or name.
    pub fn get<T>(&'a self, index: impl ColumnIndex) -> Result<T>
    where
        T: FromColumnRef<'a>,
    {
        let idx = index.index(self.columns)?;
        T::from_column_ref(self, idx)
    }

    /// Returns metadata for the row columns.
    pub fn columns(&self) -> &[Column] {
        self.columns
    }

    /// Decodes this row through [`FromRow`].
    pub fn decode<T>(&self) -> Result<T>
    where
        T: FromRow,
    {
        T::from_row_ref(self)
    }

    /// Decodes this row without first owning its values.
    ///
    /// This is useful for decoded types that borrow from the row.
    pub fn decode_ref<T>(&'a self) -> Result<T>
    where
        T: FromRowRef<'a>,
    {
        T::from_row_ref(self)
    }

    /// Returns whether a column is SQL `null`.
    pub fn is_null(&self, index: impl ColumnIndex) -> Result<bool> {
        let idx = index.index(self.columns)?;
        match &self.inner {
            #[cfg(feature = "mysql")]
            RowRefInner::Mysql(row) => row.is_null(idx).map_err(Into::into),
            #[cfg(feature = "postgres")]
            RowRefInner::Postgres(row) => row.is_null(idx).map_err(Into::into),
            #[cfg(feature = "sqlite")]
            RowRefInner::Sqlite(row) => match sqlite_owned_value(row, idx)? {
                Value::Null => Ok(true),
                _ => Ok(false),
            },
            RowRefInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    /// Reads a column as `i64`.
    pub fn get_i64(&self, index: impl ColumnIndex) -> Result<i64> {
        let idx = index.index(self.columns)?;
        match &self.inner {
            #[cfg(feature = "mysql")]
            RowRefInner::Mysql(row) => row.get_i64(idx).map_err(Into::into),
            #[cfg(feature = "postgres")]
            RowRefInner::Postgres(row) => row.get_i64(idx).map_err(Into::into),
            #[cfg(feature = "sqlite")]
            RowRefInner::Sqlite(row) => row.get_i64(idx).map_err(Into::into),
            RowRefInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    /// Reads a column as `f64`.
    pub fn get_f64(&self, index: impl ColumnIndex) -> Result<f64> {
        let idx = index.index(self.columns)?;
        match &self.inner {
            #[cfg(feature = "mysql")]
            RowRefInner::Mysql(row) => row.get_f64(idx).map_err(Into::into),
            #[cfg(feature = "postgres")]
            RowRefInner::Postgres(row) => row.get_f64(idx).map_err(Into::into),
            #[cfg(feature = "sqlite")]
            RowRefInner::Sqlite(row) => row.get_f64(idx).map_err(Into::into),
            RowRefInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    /// Reads a column as `u64`.
    ///
    /// sqlite integer values must be non-negative.
    pub fn get_u64(&self, index: impl ColumnIndex) -> Result<u64> {
        let idx = index.index(self.columns)?;
        match &self.inner {
            #[cfg(feature = "mysql")]
            RowRefInner::Mysql(row) => row.get_u64(idx).map_err(Into::into),
            #[cfg(feature = "postgres")]
            RowRefInner::Postgres(row) => row.get_u64(idx).map_err(Into::into),
            #[cfg(feature = "sqlite")]
            RowRefInner::Sqlite(row) => match row.get_i64(idx).map_err(Error::from)? {
                value if value >= 0 => Ok(value as u64),
                _ => Err(Error::Unsupported(
                    "negative SQLite integer cannot be represented as u64".into(),
                )),
            },
            RowRefInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    /// Reads a text column.
    pub fn get_str(&self, index: impl ColumnIndex) -> Result<&str> {
        let idx = index.index(self.columns)?;
        match &self.inner {
            #[cfg(feature = "mysql")]
            RowRefInner::Mysql(row) => row.get_str(idx).map_err(Into::into),
            #[cfg(feature = "postgres")]
            RowRefInner::Postgres(row) => row.get_str(idx).map_err(Into::into),
            #[cfg(feature = "sqlite")]
            RowRefInner::Sqlite(row) => row.get_str(idx).map_err(Into::into),
            RowRefInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    /// Reads a byte column.
    pub fn get_bytes(&self, index: impl ColumnIndex) -> Result<&[u8]> {
        let idx = index.index(self.columns)?;
        match &self.inner {
            #[cfg(feature = "mysql")]
            RowRefInner::Mysql(row) => row.get_bytes(idx).map_err(Into::into),
            #[cfg(feature = "postgres")]
            RowRefInner::Postgres(row) => row.get_bytes(idx).map_err(Into::into),
            #[cfg(feature = "sqlite")]
            RowRefInner::Sqlite(row) => row.get_bytes(idx).map_err(Into::into),
            RowRefInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    pub(crate) fn get_date(&self, index: impl ColumnIndex) -> Result<Date> {
        let idx = index.index(self.columns)?;
        match &self.inner {
            #[cfg(feature = "mysql")]
            RowRefInner::Mysql(row) => row
                .get_date(idx)
                .map(|value| Date {
                    year: value.year,
                    month: value.month,
                    day: value.day,
                })
                .map_err(Into::into),
            #[cfg(feature = "postgres")]
            RowRefInner::Postgres(row) => row
                .get_date(idx)
                .map(|value| Date {
                    year: value.year,
                    month: value.month,
                    day: value.day,
                })
                .map_err(Into::into),
            #[cfg(feature = "sqlite")]
            RowRefInner::Sqlite(row) => super::data::parse_date(row.get_str(idx)?)
                .ok_or_else(|| Error::Unsupported("column is not date".into())),
            RowRefInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    pub(crate) fn get_time_value(&self, index: impl ColumnIndex) -> Result<Time> {
        let idx = index.index(self.columns)?;
        match &self.inner {
            #[cfg(feature = "mysql")]
            RowRefInner::Mysql(row) => row
                .get_time_value(idx)
                .map(|value| Time {
                    hour: value.hour,
                    minute: value.minute,
                    second: value.second,
                    microsecond: value.microsecond,
                })
                .map_err(Into::into),
            #[cfg(feature = "postgres")]
            RowRefInner::Postgres(row) => row
                .get_time_value(idx)
                .map(|value| Time {
                    hour: value.hour,
                    minute: value.minute,
                    second: value.second,
                    microsecond: value.microsecond,
                })
                .map_err(Into::into),
            #[cfg(feature = "sqlite")]
            RowRefInner::Sqlite(row) => super::data::parse_time(row.get_str(idx)?)
                .ok_or_else(|| Error::Unsupported("column is not time".into())),
            RowRefInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    pub(crate) fn get_datetime(&self, index: impl ColumnIndex) -> Result<DateTime> {
        let idx = index.index(self.columns)?;
        match &self.inner {
            #[cfg(feature = "mysql")]
            RowRefInner::Mysql(row) => row
                .get_datetime(idx)
                .map(|value| DateTime {
                    date: Date {
                        year: value.date.year,
                        month: value.date.month,
                        day: value.date.day,
                    },
                    time: Time {
                        hour: value.time.hour,
                        minute: value.time.minute,
                        second: value.time.second,
                        microsecond: value.time.microsecond,
                    },
                })
                .map_err(Into::into),
            #[cfg(feature = "postgres")]
            RowRefInner::Postgres(row) => row
                .get_datetime(idx)
                .map(|value| DateTime {
                    date: Date {
                        year: value.date.year,
                        month: value.date.month,
                        day: value.date.day,
                    },
                    time: Time {
                        hour: value.time.hour,
                        minute: value.time.minute,
                        second: value.time.second,
                        microsecond: value.time.microsecond,
                    },
                })
                .map_err(Into::into),
            #[cfg(feature = "sqlite")]
            RowRefInner::Sqlite(row) => super::data::parse_datetime(row.get_str(idx)?)
                .ok_or_else(|| Error::Unsupported("column is not datetime".into())),
            RowRefInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    pub(crate) fn get_datetimetz(&self, index: impl ColumnIndex) -> Result<DateTimeTz> {
        let idx = index.index(self.columns)?;
        match &self.inner {
            #[cfg(feature = "mysql")]
            RowRefInner::Mysql(row) => {
                super::data::parse_datetime_tz(row.get_str(idx).map_err(Error::from)?)
                    .ok_or_else(|| Error::Unsupported("column is not datetime with offset".into()))
            }
            #[cfg(feature = "postgres")]
            RowRefInner::Postgres(row) => row
                .get_datetimetz(idx)
                .map(|value| DateTimeTz {
                    datetime: DateTime {
                        date: Date {
                            year: value.datetime.date.year,
                            month: value.datetime.date.month,
                            day: value.datetime.date.day,
                        },
                        time: Time {
                            hour: value.datetime.time.hour,
                            minute: value.datetime.time.minute,
                            second: value.datetime.time.second,
                            microsecond: value.datetime.time.microsecond,
                        },
                    },
                    offset_seconds: value.offset_seconds,
                })
                .map_err(Into::into),
            #[cfg(feature = "sqlite")]
            RowRefInner::Sqlite(row) => super::data::parse_datetime_tz(row.get_str(idx)?)
                .ok_or_else(|| Error::Unsupported("column is not datetime with offset".into())),
            RowRefInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    #[cfg(feature = "uuid")]
    pub(crate) fn get_uuid_bytes(&self, index: impl ColumnIndex) -> Result<[u8; 16]> {
        let idx = index.index(self.columns)?;
        match &self.inner {
            #[cfg(feature = "postgres")]
            RowRefInner::Postgres(row) => row.get_uuid(idx).map_err(Into::into),
            #[cfg(feature = "mysql")]
            RowRefInner::Mysql(row) => {
                let bytes = row.get_bytes(idx).map_err(Error::from)?;
                bytes
                    .try_into()
                    .map_err(|_| Error::Unsupported("column is not a 16-byte uuid".into()))
            }
            #[cfg(feature = "sqlite")]
            RowRefInner::Sqlite(row) => {
                let bytes = row.get_bytes(idx).map_err(Error::from)?;
                bytes
                    .try_into()
                    .map_err(|_| Error::Unsupported("column is not a 16-byte uuid".into()))
            }
            RowRefInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    /// Copies this borrowed row into an owned [`Row`].
    pub fn to_owned(&self) -> Result<Row> {
        match &self.inner {
            #[cfg(feature = "mysql")]
            RowRefInner::Mysql(row) => Ok(mysql_owned_row(row)?),
            #[cfg(feature = "postgres")]
            RowRefInner::Postgres(row) => Ok(postgres_owned_row(row)?),
            #[cfg(feature = "sqlite")]
            RowRefInner::Sqlite(row) => Ok(sqlite_owned_row(row)?),
            RowRefInner::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }
}
pub(crate) async fn decode_one_rows<R, T>(rows: &mut R) -> Result<T>
where
    R: RowStream + ?Sized,
    T: FromRow,
{
    rows.next()
        .await?
        .map(|row| row.decode())
        .transpose()?
        .ok_or_else(|| Error::Unsupported("expected at least one row, got zero".into()))
}

pub(crate) async fn decode_optional_rows<R, T>(rows: &mut R) -> Result<Option<T>>
where
    R: RowStream + ?Sized,
    T: FromRow,
{
    let first = rows.next().await?.map(|row| row.decode()).transpose()?;
    if first.is_none() {
        return Ok(None);
    }
    Ok(first)
}

pub(crate) async fn collect_decoded_rows<R, T>(mut rows: R) -> Result<Vec<T>>
where
    R: RowStream,
    T: FromRow,
{
    let mut decoded = Vec::new();
    while let Some(row) = rows.next().await? {
        decoded.push(row.decode()?);
    }
    Ok(decoded)
}
#[cfg(feature = "mysql")]
fn mysql_owned_row(row: &quex_driver::mysql::RowRef<'_>) -> Result<Row> {
    let owned = row.to_owned()?;
    Ok(Row {
        columns: owned
            .columns
            .into_iter()
            .map(|column| Column {
                name: column.name,
                nullable: column.nullable,
                kind: ColumnType::Mysql(column.column_type),
            })
            .collect(),
        values: owned.values.into_iter().map(mysql_value).collect(),
    })
}

#[cfg(feature = "postgres")]
fn postgres_owned_row(row: &quex_driver::postgres::RowRef<'_>) -> Result<Row> {
    let owned = row.to_owned()?;
    Ok(Row {
        columns: owned
            .columns
            .into_iter()
            .map(|column| Column {
                name: column.name,
                nullable: column.nullable,
                kind: ColumnType::Postgres(column.type_oid),
            })
            .collect(),
        values: owned.values.into_iter().map(postgres_value).collect(),
    })
}

#[cfg(feature = "sqlite")]
fn sqlite_owned_row(row: &quex_driver::sqlite::RowRef<'_>) -> Result<Row> {
    let owned = row.to_owned();
    Ok(Row {
        columns: owned
            .columns
            .into_iter()
            .map(|column| Column {
                name: column.name,
                nullable: column.nullable,
                kind: ColumnType::Sqlite(column.declared_type),
            })
            .collect(),
        values: owned.values.into_iter().map(sqlite_value).collect(),
    })
}

#[cfg(feature = "sqlite")]
fn sqlite_owned_value(row: &quex_driver::sqlite::RowRef<'_>, index: usize) -> Result<Value> {
    Ok(sqlite_value(row.to_owned().values[index].clone()))
}

#[cfg(feature = "mysql")]
fn mysql_value(value: quex_driver::mysql::Value) -> Value {
    match value {
        quex_driver::mysql::Value::Null => Value::Null,
        quex_driver::mysql::Value::I64(v) => Value::I64(v),
        quex_driver::mysql::Value::U64(v) => Value::U64(v),
        quex_driver::mysql::Value::F64(v) => Value::F64(v),
        quex_driver::mysql::Value::Date(v) => Value::Date(crate::Date {
            year: v.year,
            month: v.month,
            day: v.day,
        }),
        quex_driver::mysql::Value::Time(v) => Value::Time(crate::Time {
            hour: v.hour,
            minute: v.minute,
            second: v.second,
            microsecond: v.microsecond,
        }),
        quex_driver::mysql::Value::DateTime(v) => Value::DateTime(crate::DateTime {
            date: crate::Date {
                year: v.date.year,
                month: v.date.month,
                day: v.date.day,
            },
            time: crate::Time {
                hour: v.time.hour,
                minute: v.time.minute,
                second: v.time.second,
                microsecond: v.time.microsecond,
            },
        }),
        quex_driver::mysql::Value::DateTimeTz(v) => Value::DateTimeTz(crate::DateTimeTz {
            datetime: crate::DateTime {
                date: crate::Date {
                    year: v.datetime.date.year,
                    month: v.datetime.date.month,
                    day: v.datetime.date.day,
                },
                time: crate::Time {
                    hour: v.datetime.time.hour,
                    minute: v.datetime.time.minute,
                    second: v.datetime.time.second,
                    microsecond: v.datetime.time.microsecond,
                },
            },
            offset_seconds: v.offset_seconds,
        }),
        quex_driver::mysql::Value::Uuid(v) => Value::Uuid(v),
        quex_driver::mysql::Value::Bytes(v) => Value::Bytes(v),
        quex_driver::mysql::Value::String(v) => Value::String(v),
    }
}

#[cfg(feature = "postgres")]
fn postgres_value(value: quex_driver::postgres::Value) -> Value {
    match value {
        quex_driver::postgres::Value::Null => Value::Null,
        quex_driver::postgres::Value::I64(v) => Value::I64(v),
        quex_driver::postgres::Value::U64(v) => Value::U64(v),
        quex_driver::postgres::Value::F64(v) => Value::F64(v),
        quex_driver::postgres::Value::Date(v) => Value::Date(crate::Date {
            year: v.year,
            month: v.month,
            day: v.day,
        }),
        quex_driver::postgres::Value::Time(v) => Value::Time(crate::Time {
            hour: v.hour,
            minute: v.minute,
            second: v.second,
            microsecond: v.microsecond,
        }),
        quex_driver::postgres::Value::DateTime(v) => Value::DateTime(crate::DateTime {
            date: crate::Date {
                year: v.date.year,
                month: v.date.month,
                day: v.date.day,
            },
            time: crate::Time {
                hour: v.time.hour,
                minute: v.time.minute,
                second: v.time.second,
                microsecond: v.time.microsecond,
            },
        }),
        quex_driver::postgres::Value::DateTimeTz(v) => Value::DateTimeTz(crate::DateTimeTz {
            datetime: crate::DateTime {
                date: crate::Date {
                    year: v.datetime.date.year,
                    month: v.datetime.date.month,
                    day: v.datetime.date.day,
                },
                time: crate::Time {
                    hour: v.datetime.time.hour,
                    minute: v.datetime.time.minute,
                    second: v.datetime.time.second,
                    microsecond: v.datetime.time.microsecond,
                },
            },
            offset_seconds: v.offset_seconds,
        }),
        quex_driver::postgres::Value::Uuid(v) => Value::Uuid(v),
        quex_driver::postgres::Value::Bytes(v) => Value::Bytes(v),
        quex_driver::postgres::Value::String(v) => Value::String(v),
    }
}

#[cfg(feature = "sqlite")]
fn sqlite_value(value: quex_driver::sqlite::Value) -> Value {
    match value {
        quex_driver::sqlite::Value::Null => Value::Null,
        quex_driver::sqlite::Value::I64(v) => Value::I64(v),
        quex_driver::sqlite::Value::F64(v) => Value::F64(v),
        quex_driver::sqlite::Value::Bytes(v) => Value::Bytes(v),
        quex_driver::sqlite::Value::String(v) => Value::String(v),
    }
}
