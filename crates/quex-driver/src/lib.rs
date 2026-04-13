//! Low-level async database drivers used by `quex`.
//!
//! This crate exposes driver-specific building blocks for mariadb, mysql,
//! postgres, and sqlite. Most applications should start with the `quex` crate,
//! which wraps these APIs behind one facade. Use `quex-driver` directly when you
//! need driver-specific control or want to avoid the facade layer.
//!
//! The mysql and postgres drivers use the native client libraries through ffi:
//! libmariadb for mysql/mariadb and libpq for postgres. The sqlite driver uses
//! sqlite3 from a worker thread.
//!
//! Add it directly only when you want the lower-level driver api:
//!
//! ```toml
//! quex-driver = "0.1"
//! ```
//!
//! Backend support is feature-gated. Enable only the modules you need:
//!
//! - `mysql` for mysql and mariadb
//! - `postgres` for postgres
//! - `sqlite` for sqlite
//!
//! No backend is enabled by default.
//!
//! Each enabled module exposes its own connect options, connection type, row
//! types, statement types, and values. The types are intentionally
//! driver-specific; if you want one shared api across all three backends, use
//! `quex`.
//!
//! ```no_run
//! # #[cfg(feature = "mysql")]
//! # async fn run() -> quex_driver::mysql::Result<()> {
//! let mut conn = quex_driver::mysql::Connection::connect(
//!     quex_driver::mysql::ConnectOptions::new()
//!         .host("127.0.0.1")
//!         .user("root")
//!         .database("app"),
//! )
//! .await?;
//!
//! let mut rows = conn.query("select 1").await?;
//! while let Some(row) = rows.next().await? {
//!     let value = row.get_i64(0)?;
//! }
//! # Ok(())
//! # }
//! ```

#![cfg_attr(not(unix), allow(unused))]

#[cfg(not(unix))]
compile_error!("quex-driver currently supports Unix-like platforms only");

#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;
