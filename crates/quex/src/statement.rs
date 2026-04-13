#![cfg_attr(
    not(any(feature = "mysql", feature = "postgres", feature = "sqlite")),
    allow(dead_code, unused_imports, unused_variables)
)]

#[cfg(all(test, feature = "postgres"))]
use crate::Value;
#[cfg(feature = "mysql")]
use crate::executor::MysqlParamSource;
#[cfg(feature = "sqlite")]
use crate::executor::ParamRef;
use crate::executor::ParamSource;
#[cfg(feature = "postgres")]
use crate::executor::PostgresParamSource;
use crate::{BoundStatement, Encode, ExecResult, PreparedStatement, Result, Rows};
#[cfg(any(feature = "sqlite", all(test, feature = "postgres")))]
use crate::Error;
#[cfg(feature = "sqlite")]
use crate::{Date, DateTime, DateTimeTz, Time};

/// A driver-specific prepared statement behind the facade.
pub enum Statement<'a> {
    #[cfg(feature = "mysql")]
    /// mysql or mariadb statement.
    Mysql(quex_driver::mysql::CachedStatement<'a>),
    #[cfg(feature = "postgres")]
    /// postgres statement.
    Postgres(quex_driver::postgres::CachedStatement<'a>),
    #[cfg(feature = "sqlite")]
    /// sqlite statement.
    Sqlite(quex_driver::sqlite::CachedStatement<'a>),
    #[doc(hidden)]
    _Marker(std::marker::PhantomData<&'a ()>),
}

impl Statement<'_> {
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
        match self {
            #[cfg(feature = "mysql")]
            Statement::Mysql(stmt) => {
                let source = MysqlParamSource(params);
                Ok(Rows::mysql(stmt.execute_source(&source).await?))
            }
            #[cfg(feature = "postgres")]
            Statement::Postgres(stmt) => Ok(Rows::postgres({
                let source = PostgresParamSource(params);
                stmt.execute_source(&source).await?
            })),
            #[cfg(feature = "sqlite")]
            Statement::Sqlite(stmt) => Ok(Rows::sqlite(
                stmt.execute(&to_sqlite_params_source(params)?).await?,
            )),
            Statement::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }

    /// Runs the statement when no rows are expected.
    pub async fn exec_source<P>(&mut self, params: &P) -> Result<ExecResult>
    where
        P: ParamSource + ?Sized,
    {
        match self {
            #[cfg(feature = "mysql")]
            Statement::Mysql(stmt) => {
                let source = MysqlParamSource(params);
                stmt.exec_source(&source)
                    .await
                    .map(mysql_exec_result)
                    .map_err(Into::into)
            }
            #[cfg(feature = "postgres")]
            Statement::Postgres(stmt) => {
                let source = PostgresParamSource(params);
                stmt.exec_source(&source)
                    .await
                    .map(postgres_exec_result)
                    .map_err(Into::into)
            }
            #[cfg(feature = "sqlite")]
            Statement::Sqlite(stmt) => stmt
                .exec(&to_sqlite_params_source(params)?)
                .await
                .map(sqlite_exec_result)
                .map_err(Into::into),
            Statement::_Marker(_) => unreachable!("disabled backend placeholder"),
        }
    }
}

impl PreparedStatement for Statement<'_> {
    type Rows<'a>
        = Rows<'a>
    where
        Self: 'a;

    async fn execute_source<P>(&mut self, params: &P) -> Result<Self::Rows<'_>>
    where
        P: ParamSource + ?Sized,
    {
        Statement::execute_source(self, params).await
    }

    async fn exec_source<P>(&mut self, params: &P) -> Result<ExecResult>
    where
        P: ParamSource + ?Sized,
    {
        Statement::exec_source(self, params).await
    }
}

/// Rows returned by a query.
///
#[cfg(feature = "mysql")]
pub(crate) fn mysql_exec_result(result: quex_driver::mysql::ExecuteResult) -> ExecResult {
    ExecResult {
        rows_affected: result.rows_affected,
        last_insert_id: Some(result.last_insert_id).filter(|id| *id != 0),
    }
}

#[cfg(feature = "postgres")]
pub(crate) fn postgres_exec_result(result: quex_driver::postgres::ExecuteResult) -> ExecResult {
    ExecResult {
        rows_affected: result.rows_affected,
        last_insert_id: result.last_insert_id,
    }
}

#[cfg(feature = "sqlite")]
pub(crate) fn sqlite_exec_result(result: quex_driver::sqlite::ExecuteResult) -> ExecResult {
    ExecResult {
        rows_affected: result.rows_affected,
        last_insert_id: u64::try_from(result.last_insert_rowid)
            .ok()
            .filter(|id| *id != 0),
    }
}

#[cfg(all(test, feature = "postgres"))]
pub(crate) fn to_postgres_values(values: &[Value]) -> Result<Vec<quex_driver::postgres::Value>> {
    values
        .iter()
        .cloned()
        .map(|value| match value {
            Value::Null => Ok(quex_driver::postgres::Value::Null),
            Value::I64(v) => Ok(quex_driver::postgres::Value::I64(v)),
            Value::U64(v) => i64::try_from(v)
                .map(quex_driver::postgres::Value::I64)
                .map_err(|_| {
                    Error::Unsupported(
                    "u64 value exceeds PostgreSQL bigint range; pass a string explicitly instead"
                        .into(),
                )
                }),
            Value::F64(v) => Ok(quex_driver::postgres::Value::F64(v)),
            Value::Date(v) => Ok(quex_driver::postgres::Value::Date(
                quex_driver::postgres::DateValue {
                    year: v.year,
                    month: v.month,
                    day: v.day,
                },
            )),
            Value::Time(v) => Ok(quex_driver::postgres::Value::Time(
                quex_driver::postgres::TimeValue {
                    hour: v.hour,
                    minute: v.minute,
                    second: v.second,
                    microsecond: v.microsecond,
                },
            )),
            Value::DateTime(v) => Ok(quex_driver::postgres::Value::DateTime(
                quex_driver::postgres::DateTimeValue {
                    date: quex_driver::postgres::DateValue {
                        year: v.date.year,
                        month: v.date.month,
                        day: v.date.day,
                    },
                    time: quex_driver::postgres::TimeValue {
                        hour: v.time.hour,
                        minute: v.time.minute,
                        second: v.time.second,
                        microsecond: v.time.microsecond,
                    },
                },
            )),
            Value::DateTimeTz(v) => Ok(quex_driver::postgres::Value::DateTimeTz(
                quex_driver::postgres::DateTimeTzValue {
                    datetime: quex_driver::postgres::DateTimeValue {
                        date: quex_driver::postgres::DateValue {
                            year: v.datetime.date.year,
                            month: v.datetime.date.month,
                            day: v.datetime.date.day,
                        },
                        time: quex_driver::postgres::TimeValue {
                            hour: v.datetime.time.hour,
                            minute: v.datetime.time.minute,
                            second: v.datetime.time.second,
                            microsecond: v.datetime.time.microsecond,
                        },
                    },
                    offset_seconds: v.offset_seconds,
                },
            )),
            Value::Uuid(v) => Ok(quex_driver::postgres::Value::Uuid(v)),
            Value::Bytes(v) => Ok(quex_driver::postgres::Value::Bytes(v)),
            Value::String(v) => Ok(quex_driver::postgres::Value::String(v)),
        })
        .collect()
}

#[cfg(feature = "sqlite")]
pub(crate) fn to_sqlite_params_source<P>(params: &P) -> Result<Vec<quex_driver::sqlite::Value>>
where
    P: ParamSource + ?Sized,
{
    (0..params.len())
        .map(|index| match params.value_at(index) {
            ParamRef::Null => Ok(quex_driver::sqlite::Value::Null),
            ParamRef::I64(value) => Ok(quex_driver::sqlite::Value::I64(*value)),
            ParamRef::U64(value) => i64::try_from(*value).map(quex_driver::sqlite::Value::I64).map_err(|_| {
                Error::Unsupported(
                    "u64 value exceeds SQLite signed integer range; pass a string explicitly instead"
                        .into(),
                )
            }),
            ParamRef::F64(value) => Ok(quex_driver::sqlite::Value::F64(*value)),
            ParamRef::Date(value) => Ok(quex_driver::sqlite::Value::String(format_date(*value))),
            ParamRef::Time(value) => Ok(quex_driver::sqlite::Value::String(format_time(*value))),
            ParamRef::DateTime(value) => {
                Ok(quex_driver::sqlite::Value::String(format_datetime(*value)))
            }
            ParamRef::DateTimeTz(value) => {
                Ok(quex_driver::sqlite::Value::String(format_datetime_tz(*value)))
            }
            ParamRef::Uuid(value) => Ok(quex_driver::sqlite::Value::String(format_uuid(*value))),
            ParamRef::Str(value) => Ok(quex_driver::sqlite::Value::String(value.to_owned())),
            ParamRef::Bytes(value) => Ok(quex_driver::sqlite::Value::Bytes(value.to_vec())),
        })
        .collect()
}

#[cfg(feature = "sqlite")]
fn format_date(value: Date) -> String {
    format!("{:04}-{:02}-{:02}", value.year, value.month, value.day)
}

#[cfg(feature = "sqlite")]
fn format_time(value: Time) -> String {
    if value.microsecond == 0 {
        format!("{:02}:{:02}:{:02}", value.hour, value.minute, value.second)
    } else {
        let mut micros = format!("{:06}", value.microsecond);
        while micros.ends_with('0') {
            micros.pop();
        }
        format!(
            "{:02}:{:02}:{:02}.{}",
            value.hour, value.minute, value.second, micros
        )
    }
}

#[cfg(feature = "sqlite")]
fn format_datetime(value: DateTime) -> String {
    format!("{} {}", format_date(value.date), format_time(value.time))
}

#[cfg(feature = "sqlite")]
fn format_datetime_tz(value: DateTimeTz) -> String {
    format!(
        "{}T{}{}",
        format_date(value.datetime.date),
        format_time(value.datetime.time),
        format_offset(value.offset_seconds)
    )
}

#[cfg(feature = "sqlite")]
fn format_offset(offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let offset_seconds = offset_seconds.abs();
    let hours = offset_seconds / 3600;
    let minutes = (offset_seconds % 3600) / 60;
    format!("{sign}{hours:02}:{minutes:02}")
}

#[cfg(feature = "sqlite")]
fn format_uuid(bytes: [u8; 16]) -> String {
    #[cfg(feature = "uuid")]
    {
        uuid::Uuid::from_bytes(bytes).hyphenated().to_string()
    }

    #[cfg(not(feature = "uuid"))]
    {
        let _ = bytes;
        unreachable!("uuid formatting requires the `uuid` feature")
    }
}

#[cfg(all(test, feature = "sqlite", feature = "postgres"))]
pub(crate) fn to_sqlite_values(values: &[Value]) -> Result<Vec<quex_driver::sqlite::Value>> {
    values
        .iter()
        .cloned()
        .map(|value| match value {
            Value::Null => Ok(quex_driver::sqlite::Value::Null),
            Value::I64(v) => Ok(quex_driver::sqlite::Value::I64(v)),
            Value::U64(v) => i64::try_from(v).map(quex_driver::sqlite::Value::I64).map_err(|_| {
                Error::Unsupported(
                    "u64 value exceeds SQLite signed integer range; pass a string explicitly instead"
                        .into(),
                )
            }),
            Value::F64(v) => Ok(quex_driver::sqlite::Value::F64(v)),
            Value::Date(v) => Ok(quex_driver::sqlite::Value::String(format_date(v))),
            Value::Time(v) => Ok(quex_driver::sqlite::Value::String(format_time(v))),
            Value::DateTime(v) => Ok(quex_driver::sqlite::Value::String(format_datetime(v))),
            Value::DateTimeTz(v) => Ok(quex_driver::sqlite::Value::String(format_datetime_tz(v))),
            Value::Uuid(v) => Ok(quex_driver::sqlite::Value::String(format_uuid(v))),
            Value::Bytes(v) => Ok(quex_driver::sqlite::Value::Bytes(v)),
            Value::String(v) => Ok(quex_driver::sqlite::Value::String(v)),
        })
        .collect()
}
