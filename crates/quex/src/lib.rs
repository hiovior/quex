//! Async SQL access for mariadb, mysql, postgres, and sqlite.
//!
//! `quex` is a small facade over [`quex_driver`]. It gives you one API for:
//!
//! - opening one connection or a pool
//! - running SQL directly
//! - binding positional parameters
//! - decoding rows into your own types
//!
//! It stays close to SQL. This crate does not try to hide queries behind an
//! orm or query builder.
//!
//! The main entry points are:
//!
//! - [`Pool`] for many short-lived checked-out connections
//! - [`Connection`] for one connection
//! - [`query`] for one-off SQL
//! - [`prepare`] for a reusable prepared statement
//! - [`Hooks`] when a pool needs connection setup or validation
//! - [`Executor`] when you want generic code that works with any supported
//!   executor type
//! - [`Encode`] for binding your own parameter types
//! - [`FromRow`], [`FromRowRef`], and [`Decode`] for mapping rows into your own
//!   types
//!
//! `query` and `prepare` use `?` placeholders for every driver. When the driver
//! is postgres, `quex` rewrites them to `$1`, `$2`, and so on before sending
//! the SQL to libpq.
//!
//! A small sqlite example:
//!
//! ```no_run
//! use quex::{FromRow, Pool, Row, SqliteConnectOptions};
//!
//! struct User {
//!     id: i64,
//!     name: String,
//! }
//!
//! impl FromRow for User {
//!     fn from_row(row: &Row) -> quex::Result<Self> {
//!         Ok(Self {
//!             id: row.get("id")?,
//!             name: row.get("name")?,
//!         })
//!     }
//! }
//!
//! # async fn run() -> quex::Result<()> {
//! let pool = Pool::connect(SqliteConnectOptions::new().in_memory())?
//!     .max_size(4)
//!     .build()
//!     .await?;
//! let mut db = pool.acquire().await?;
//!
//! quex::query("create table users(id integer primary key, name text not null)")
//!     .execute(&mut db)
//!     .await?;
//!
//! quex::query("insert into users(name) values(?)")
//!     .bind("Ada")
//!     .execute(&mut db)
//!     .await?;
//!
//! let users = quex::query("select id, name from users order by id")
//!     .all::<User>(&mut db)
//!     .await?;
//! # let _ = users;
//! # Ok(())
//! # }
//! ```
//!
//! A few things matter when using the crate:
//!
//! - Borrowed rows only live until the stream advances. If you need to keep
//!   data longer, decode into owned types or collect owned rows.
//! - [`Query::one`] and [`Query::optional`] ignore extra rows.
//! - A [`PoolTransaction`] holds one checked-out connection until commit,
//!   rollback, or drop.
//! - [`Pool::with_hooks`] can run setup on fresh connections and validate a
//!   connection before it is handed out.
//! - [`Pool`] is the better default when many tasks need database access.
//!   [`Connection`] is the simpler single-connection option.
//!
//! Optional features add support for common value types:
//!
//! - `mysql` enables mysql and mariadb support
//! - `postgres` enables postgres support
//! - `sqlite` enables sqlite support
//! - `chrono` for chrono date and time types
//! - `time` for `time` crate types
//! - `uuid` for `uuid::Uuid`
//! - `json` for `serde_json::Value`
//!
//! Setup depends on the driver you use. `quex_driver` talks to libpq,
//! libmariadb, and sqlite3 through ffi, so those client libraries need to be
//! available for your target environment. No database backend is enabled by
//! default, so pick the ones you need in your `Cargo.toml`.

mod connection;
mod data;
mod error;
mod executor;
mod options;
mod pool;
mod rows;
mod statement;

#[cfg(test)]
mod tests;

pub use data::{
    Column, ColumnIndex, ColumnType, Date, DateTime, DateTimeTz, Decode, Decoder, Encode, Encoder,
    ExecResult, FromColumnRef, FromRow, FromRowRef, ParamValue, Row, Time, Value,
};
pub use error::{Error, Result};
pub use executor::{
    BoundStatement, Executor, ParamRef, ParamSource, Params, Prepare, PreparedStatement, Query,
    RowStream, prepare, query,
};
pub use options::{
    ConnectOptions, Driver, MysqlConnectOptions, PostgresConnectOptions, SqliteConnectOptions,
};
pub use pool::{
    AcquireDecision, Connection, Hooks, IntoConnectOptions, Pool, PoolBuilder, PoolStatement,
    PoolTransaction, PooledConnection, PooledStatement, PooledTransaction, Transaction,
};
pub use rows::{RowRef, Rows};
pub use statement::Statement;
