#![cfg_attr(
    not(any(feature = "mysql", feature = "postgres", feature = "sqlite")),
    allow(unreachable_code, unused_variables)
)]

#[cfg(feature = "postgres")]
use std::borrow::Cow;

use crate::{ConnectOptions, Driver, Error, Result, Rows, Statement};

pub(crate) enum ConnectionInner {
    #[cfg(feature = "mysql")]
    Mysql(quex_driver::mysql::Connection),
    #[cfg(feature = "postgres")]
    Postgres(quex_driver::postgres::Connection),
    #[cfg(feature = "sqlite")]
    Sqlite(quex_driver::sqlite::Connection),
    _Disabled,
}

pub(crate) struct ManagedConnection {
    pub(crate) driver: Driver,
    pub(crate) inner: ConnectionInner,
}

pub(crate) async fn query_inner<'a>(inner: &'a mut ConnectionInner, sql: &str) -> Result<Rows<'a>> {
    match inner {
        #[cfg(feature = "mysql")]
        ConnectionInner::Mysql(conn) => {
            let rows = conn.query(sql).await?;
            Ok(Rows::mysql(rows))
        }
        #[cfg(feature = "postgres")]
        ConnectionInner::Postgres(conn) => {
            let rows = conn.query(sql).await?;
            Ok(Rows::postgres(rows))
        }
        #[cfg(feature = "sqlite")]
        ConnectionInner::Sqlite(conn) => {
            let rows = conn.query(sql).await?;
            Ok(Rows::sqlite(rows))
        }
        ConnectionInner::_Disabled => unreachable!("disabled backend placeholder"),
    }
}

pub(crate) async fn prepare_inner<'a>(
    inner: &'a mut ConnectionInner,
    sql: &str,
) -> Result<Statement<'a>> {
    match inner {
        #[cfg(feature = "mysql")]
        ConnectionInner::Mysql(conn) => Ok(Statement::Mysql(conn.prepare_cached(sql).await?)),
        #[cfg(feature = "postgres")]
        ConnectionInner::Postgres(conn) => {
            let sql = rewrite_postgres_placeholders(sql);
            Ok(Statement::Postgres(conn.prepare_cached(&sql).await?))
        }
        #[cfg(feature = "sqlite")]
        ConnectionInner::Sqlite(conn) => Ok(Statement::Sqlite(conn.prepare_cached(sql).await?)),
        ConnectionInner::_Disabled => unreachable!("disabled backend placeholder"),
    }
}

#[cfg(feature = "postgres")]
pub(crate) fn rewrite_postgres_placeholders(sql: &str) -> Cow<'_, str> {
    let bytes = sql.as_bytes();
    let mut output: Option<String> = None;
    let mut last = 0;
    let mut i = 0;
    let mut param = 1;

    while i < bytes.len() {
        match bytes[i] {
            b'?' => {
                let output = output.get_or_insert_with(|| String::with_capacity(sql.len() + 4));
                output.push_str(&sql[last..i]);
                output.push('$');
                output.push_str(param.to_string().as_str());
                param += 1;
                i += 1;
                last = i;
            }
            b'\'' => i = skip_single_quoted(bytes, i),
            b'"' => i = skip_double_quoted(bytes, i),
            b'-' if bytes.get(i + 1) == Some(&b'-') => i = skip_line_comment(bytes, i + 2),
            b'/' if bytes.get(i + 1) == Some(&b'*') => i = skip_block_comment(bytes, i + 2),
            b'$' => {
                if let Some(delimiter_len) = dollar_quote_delimiter_len(bytes, i) {
                    i = skip_dollar_quoted(bytes, i, delimiter_len);
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }

    match output {
        Some(mut output) => {
            output.push_str(&sql[last..]);
            Cow::Owned(output)
        }
        None => Cow::Borrowed(sql),
    }
}

#[cfg(feature = "postgres")]
fn skip_single_quoted(bytes: &[u8], mut i: usize) -> usize {
    i += 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            i += 1;
            if bytes.get(i) == Some(&b'\'') {
                i += 1;
            } else {
                break;
            }
        } else {
            i += 1;
        }
    }
    i
}

#[cfg(feature = "postgres")]
fn skip_double_quoted(bytes: &[u8], mut i: usize) -> usize {
    i += 1;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            i += 1;
            if bytes.get(i) == Some(&b'"') {
                i += 1;
            } else {
                break;
            }
        } else {
            i += 1;
        }
    }
    i
}

#[cfg(feature = "postgres")]
fn skip_line_comment(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }
    i
}

#[cfg(feature = "postgres")]
fn skip_block_comment(bytes: &[u8], mut i: usize) -> usize {
    let mut depth = 1;
    while i + 1 < bytes.len() {
        match (bytes[i], bytes[i + 1]) {
            (b'/', b'*') => {
                depth += 1;
                i += 2;
            }
            (b'*', b'/') => {
                depth -= 1;
                i += 2;
                if depth == 0 {
                    break;
                }
            }
            _ => i += 1,
        }
    }
    bytes.len().min(i)
}

#[cfg(feature = "postgres")]
fn dollar_quote_delimiter_len(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start + 1;
    if bytes.get(i) == Some(&b'$') {
        return Some(2);
    }
    if !matches!(bytes.get(i), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_')) {
        return None;
    }
    i += 1;
    while matches!(
        bytes.get(i),
        Some(b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
    ) {
        i += 1;
    }
    if bytes.get(i) == Some(&b'$') {
        Some(i - start + 1)
    } else {
        None
    }
}

#[cfg(feature = "postgres")]
fn skip_dollar_quoted(bytes: &[u8], start: usize, delimiter_len: usize) -> usize {
    let delimiter = &bytes[start..start + delimiter_len];
    let mut i = start + delimiter_len;
    while i + delimiter_len <= bytes.len() {
        if &bytes[i..i + delimiter_len] == delimiter {
            return i + delimiter_len;
        }
        i += 1;
    }
    bytes.len()
}

pub(crate) async fn start_transaction_inner(inner: &mut ConnectionInner) -> Result<()> {
    match inner {
        #[cfg(feature = "mysql")]
        ConnectionInner::Mysql(conn) => {
            let _ = conn.query("start transaction").await?;
            Ok(())
        }
        #[cfg(feature = "postgres")]
        ConnectionInner::Postgres(conn) => {
            let _ = conn.query("begin").await?;
            Ok(())
        }
        #[cfg(feature = "sqlite")]
        ConnectionInner::Sqlite(conn) => conn
            .execute_batch("begin immediate")
            .await
            .map_err(Into::into),
        ConnectionInner::_Disabled => unreachable!("disabled backend placeholder"),
    }
}

pub(crate) async fn commit_inner(inner: &mut ConnectionInner) -> Result<()> {
    match inner {
        #[cfg(feature = "mysql")]
        ConnectionInner::Mysql(conn) => conn.commit().await.map_err(Into::into),
        #[cfg(feature = "postgres")]
        ConnectionInner::Postgres(conn) => conn.commit().await.map_err(Into::into),
        #[cfg(feature = "sqlite")]
        ConnectionInner::Sqlite(conn) => conn.execute_batch("commit").await.map_err(Into::into),
        ConnectionInner::_Disabled => unreachable!("disabled backend placeholder"),
    }
}

pub(crate) async fn rollback_inner(inner: &mut ConnectionInner) -> Result<()> {
    match inner {
        #[cfg(feature = "mysql")]
        ConnectionInner::Mysql(conn) => conn.rollback().await.map_err(Into::into),
        #[cfg(feature = "postgres")]
        ConnectionInner::Postgres(conn) => conn.rollback().await.map_err(Into::into),
        #[cfg(feature = "sqlite")]
        ConnectionInner::Sqlite(conn) => conn.execute_batch("rollback").await.map_err(Into::into),
        ConnectionInner::_Disabled => unreachable!("disabled backend placeholder"),
    }
}

pub(crate) async fn connect_managed(options: ConnectOptions) -> Result<ManagedConnection> {
    let driver = options
        .driver
        .ok_or_else(|| Error::invalid_url("missing driver"))?;

    let inner = match driver {
        Driver::Mysql => {
            #[cfg(feature = "mysql")]
            {
                ConnectionInner::Mysql(
                    quex_driver::mysql::Connection::connect(
                        quex_driver::mysql::ConnectOptions::new()
                            .host(options.host.unwrap_or_else(|| "127.0.0.1".into()))
                            .port(options.port.unwrap_or(3306) as u32)
                            .user(options.username.unwrap_or_else(|| "root".into()))
                            .password(options.password.unwrap_or_default())
                            .database(options.database.unwrap_or_default())
                            .unix_socket(options.unix_socket.unwrap_or_default()),
                    )
                    .await?,
                )
            }
            #[cfg(not(feature = "mysql"))]
            {
                return Err(Error::Unsupported(
                    "mysql support is not enabled; enable the `mysql` feature".into(),
                ));
            }
        }
        Driver::Pgsql => {
            #[cfg(feature = "postgres")]
            {
                ConnectionInner::Postgres(
                    quex_driver::postgres::Connection::connect(
                        quex_driver::postgres::ConnectOptions::new()
                            .host(options.host.unwrap_or_else(|| "127.0.0.1".into()))
                            .port(options.port.unwrap_or(5432))
                            .user(options.username.unwrap_or_else(|| "postgres".into()))
                            .password(options.password.unwrap_or_default())
                            .database(options.database.unwrap_or_else(|| "postgres".into())),
                    )
                    .await?,
                )
            }
            #[cfg(not(feature = "postgres"))]
            {
                return Err(Error::Unsupported(
                    "postgres support is not enabled; enable the `postgres` feature".into(),
                ));
            }
        }
        Driver::Sqlite => {
            #[cfg(feature = "sqlite")]
            {
                let mut connect = quex_driver::sqlite::ConnectOptions::new()
                    .read_only(options.read_only)
                    .create_if_missing(options.create_if_missing);
                if let Some(timeout) = options.busy_timeout {
                    connect = connect.busy_timeout(timeout);
                }
                connect = if options.in_memory {
                    connect.in_memory()
                } else if let Some(path) = options.path {
                    connect.path(path)
                } else {
                    connect.path("sqlite.db")
                };
                ConnectionInner::Sqlite(quex_driver::sqlite::Connection::connect(connect).await?)
            }
            #[cfg(not(feature = "sqlite"))]
            {
                return Err(Error::Unsupported(
                    "sqlite support is not enabled; enable the `sqlite` feature".into(),
                ));
            }
        }
    };

    Ok(ManagedConnection { driver, inner })
}
