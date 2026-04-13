#![cfg_attr(not(feature = "sqlite"), allow(dead_code, unused_imports))]

#[cfg(feature = "postgres")]
use std::borrow::Cow;
use std::path::PathBuf;
#[cfg(feature = "sqlite")]
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[cfg(feature = "postgres")]
use crate::connection::rewrite_postgres_placeholders;
use crate::data::decode_value;
#[cfg(all(feature = "postgres", feature = "sqlite"))]
use crate::statement::to_postgres_values;
#[cfg(all(feature = "postgres", feature = "sqlite"))]
use crate::statement::to_sqlite_values;
use crate::*;
#[cfg(feature = "sqlite")]
use tokio::time::{Duration as TokioDuration, timeout};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Email(String);

impl Email {
    fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.contains('@') {
            Ok(Self(value))
        } else {
            Err(Error::Unsupported("invalid email".into()))
        }
    }
}

impl Encode for Email {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_str(self.0.as_str());
    }
}

impl Decode for Email {
    fn decode(value: &mut Decoder<'_>) -> Result<Self> {
        Self::parse(value.decode_str()?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FakeUuid(u128);

impl Encode for FakeUuid {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_string(format!("{:032x}", self.0));
    }
}

struct BadEmptyEncode;

impl Encode for BadEmptyEncode {
    fn encode(&self, _out: Encoder<'_>) {}
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct User {
    id: i64,
    email: Email,
    score: i64,
}

impl FromRow for User {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(Self {
            id: row.get("id")?,
            email: row.get("email")?,
            score: row.get("score")?,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
struct UserRef<'r> {
    id: i64,
    name: &'r str,
    email: &'r str,
}

impl<'r> FromRowRef<'r> for UserRef<'r> {
    fn from_row_ref(row: &'r RowRef<'r>) -> Result<Self> {
        Ok(Self {
            id: row.get("id")?,
            name: row.get("name")?,
            email: row.get("email")?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum ArenaBind {
    Str { start: usize, len: usize },
    Bytes { start: usize, len: usize },
}

struct ArenaParams {
    arena: Vec<u8>,
    binds: Vec<ArenaBind>,
}

impl ArenaParams {
    fn new() -> Self {
        Self {
            arena: Vec::new(),
            binds: Vec::new(),
        }
    }

    fn push_str(&mut self, value: &str) {
        let start = self.arena.len();
        self.arena.extend_from_slice(value.as_bytes());
        self.binds.push(ArenaBind::Str {
            start,
            len: value.len(),
        });
    }

    fn push_bytes(&mut self, value: &[u8]) {
        let start = self.arena.len();
        self.arena.extend_from_slice(value);
        self.binds.push(ArenaBind::Bytes {
            start,
            len: value.len(),
        });
    }
}

impl ParamSource for ArenaParams {
    fn len(&self) -> usize {
        self.binds.len()
    }

    fn value_at(&self, index: usize) -> ParamRef<'_> {
        match self.binds[index] {
            ArenaBind::Str { start, len } => {
                let value = std::str::from_utf8(&self.arena[start..start + len]).unwrap();
                ParamRef::Str(value)
            }
            ArenaBind::Bytes { start, len } => ParamRef::Bytes(&self.arena[start..start + len]),
        }
    }
}

async fn count_rows<E: Executor>(exec: &mut E) -> Result<i64> {
    let mut rows = query("select count(*) as count from t").fetch(exec).await?;
    let row = rows.next().await?.unwrap();
    row.get_i64("count")
}

#[test]
fn parses_mysql_url() {
    let options = ConnectOptions::from_url("mysql://root:secret@127.0.0.1:3306/test").unwrap();
    assert_eq!(options.driver, Some(Driver::Mysql));
    assert_eq!(options.host.as_deref(), Some("127.0.0.1"));
    assert_eq!(options.port, Some(3306));
    assert_eq!(options.username.as_deref(), Some("root"));
    assert_eq!(options.password.as_deref(), Some("secret"));
    assert_eq!(options.database.as_deref(), Some("test"));
}

#[test]
fn parses_sqlite_memory_url() {
    let options = ConnectOptions::from_url("sqlite::memory:").unwrap();
    assert_eq!(options.driver, Some(Driver::Sqlite));
    assert!(options.in_memory);
}

#[test]
fn connect_options_from_pgsql_url() {
    let options =
        ConnectOptions::from_url("postgres://postgres:postgres@127.0.0.1:5432/postgres").unwrap();
    assert_eq!(options.driver, Some(Driver::Pgsql));
    assert_eq!(options.host.as_deref(), Some("127.0.0.1"));
    assert_eq!(options.port, Some(5432));
    assert_eq!(options.database.as_deref(), Some("postgres"));
}

#[test]
fn typed_connect_options_do_not_require_dsn() {
    let options = ConnectOptions::new(Driver::Pgsql)
        .host("127.0.0.1")
        .port(5432)
        .database("postgres")
        .username("postgres")
        .password("postgres");

    assert_eq!(options.driver, Some(Driver::Pgsql));
    assert_eq!(options.host.as_deref(), Some("127.0.0.1"));
    assert_eq!(options.port, Some(5432));
    assert_eq!(options.database.as_deref(), Some("postgres"));
    assert_eq!(options.username.as_deref(), Some("postgres"));
}

#[test]
fn into_connect_options_accepts_urls_generic_and_typed_options() {
    let from_url = "sqlite::memory:".into_connect_options().unwrap();
    assert_eq!(from_url.driver, Some(Driver::Sqlite));
    assert!(from_url.in_memory);

    let generic = ConnectOptions::new(Driver::Pgsql)
        .host("127.0.0.1")
        .database("postgres")
        .into_connect_options()
        .unwrap();
    assert_eq!(generic.driver, Some(Driver::Pgsql));
    assert_eq!(generic.host.as_deref(), Some("127.0.0.1"));
    assert_eq!(generic.database.as_deref(), Some("postgres"));

    let sqlite = SqliteConnectOptions::new()
        .in_memory()
        .into_connect_options()
        .unwrap();
    assert_eq!(sqlite.driver, Some(Driver::Sqlite));
    assert!(sqlite.in_memory);

    let postgres = PostgresConnectOptions::new()
        .host("localhost")
        .port(5432)
        .username("postgres")
        .password("postgres")
        .database("postgres")
        .into_connect_options()
        .unwrap();
    assert_eq!(postgres.driver, Some(Driver::Pgsql));
    assert_eq!(postgres.host.as_deref(), Some("localhost"));
    assert_eq!(postgres.port, Some(5432));
    assert_eq!(postgres.username.as_deref(), Some("postgres"));
    assert_eq!(postgres.password.as_deref(), Some("postgres"));
    assert_eq!(postgres.database.as_deref(), Some("postgres"));

    let mysql = MysqlConnectOptions::new()
        .host("localhost")
        .port(3306)
        .username("root")
        .password("root")
        .database("mysql")
        .unix_socket("/tmp/mysql.sock")
        .into_connect_options()
        .unwrap();
    assert_eq!(mysql.driver, Some(Driver::Mysql));
    assert_eq!(mysql.host.as_deref(), Some("localhost"));
    assert_eq!(mysql.port, Some(3306));
    assert_eq!(mysql.username.as_deref(), Some("root"));
    assert_eq!(mysql.password.as_deref(), Some("root"));
    assert_eq!(mysql.database.as_deref(), Some("mysql"));
    assert_eq!(mysql.unix_socket.as_deref(), Some("/tmp/mysql.sock"));
}

#[test]
fn sqlite_url_parses_absolute_path() {
    let options = ConnectOptions::from_url("sqlite:///tmp/quex-example.db").unwrap();
    assert_eq!(options.driver, Some(Driver::Sqlite));
    assert_eq!(
        options.path.as_deref(),
        Some(PathBuf::from("/tmp/quex-example.db").as_path())
    );
}

#[cfg(all(feature = "postgres", feature = "sqlite"))]
#[test]
fn oversized_u64_is_rejected_for_postgres_and_sqlite() {
    let values = [Value::U64((i64::MAX as u64) + 1)];
    assert!(to_postgres_values(&values).is_err());
    assert!(to_sqlite_values(&values).is_err());
}

#[test]
fn params_bind_supports_scalars_and_custom_encoders() {
    let params = Params::new()
        .bind(7)
        .bind("Ada")
        .bind(Email::parse("ada@example.com").unwrap());

    assert!(matches!(params.value_at(0), ParamRef::I64(value) if *value == 7));
    assert!(matches!(params.value_at(1), ParamRef::Str("Ada")));
    assert!(matches!(
        params.value_at(2),
        ParamRef::Str("ada@example.com")
    ));
}

#[test]
#[should_panic(expected = "must write exactly one SQL parameter, but wrote none")]
fn encode_panics_when_it_writes_no_parameter() {
    let _ = Params::new().bind(BadEmptyEncode);
}

#[test]
fn params_store_custom_encoded_values() {
    let params = Params::new()
        .bind(Email::parse("ada@example.com").unwrap())
        .bind(FakeUuid(0xfeed_beef));

    assert!(matches!(
        params.value_at(0),
        ParamRef::Str("ada@example.com")
    ));
    assert!(matches!(
        params.value_at(1),
        ParamRef::Str("000000000000000000000000feedbeef")
    ));
}

#[test]
fn params_store_core_date_time_values() {
    let date = Date {
        year: 2024,
        month: 1,
        day: 2,
    };
    let time = Time {
        hour: 3,
        minute: 4,
        second: 5,
        microsecond: 123_456,
    };
    let datetime = DateTime { date, time };
    let datetime_tz = DateTimeTz {
        datetime,
        offset_seconds: 3600,
    };

    let params = Params::new()
        .bind(date)
        .bind(time)
        .bind(datetime)
        .bind(datetime_tz);

    assert!(matches!(params.value_at(0), ParamRef::Date(value) if *value == date));
    assert!(matches!(params.value_at(1), ParamRef::Time(value) if *value == time));
    assert!(matches!(params.value_at(2), ParamRef::DateTime(value) if *value == datetime));
    assert!(matches!(params.value_at(3), ParamRef::DateTimeTz(value) if *value == datetime_tz));
}

#[test]
fn core_decode_prefers_typed_owned_values() {
    let date = Date {
        year: 2024,
        month: 1,
        day: 2,
    };
    let time = Time {
        hour: 3,
        minute: 4,
        second: 5,
        microsecond: 123_456,
    };
    let datetime = DateTime { date, time };
    let datetime_tz = DateTimeTz {
        datetime,
        offset_seconds: 0,
    };

    let decoded_date: Date = decode_value(&Value::Date(date)).unwrap();
    let decoded_time: Time = decode_value(&Value::Time(time)).unwrap();
    let decoded_datetime: DateTime = decode_value(&Value::DateTime(datetime)).unwrap();
    let decoded_datetime_tz: DateTimeTz = decode_value(&Value::DateTimeTz(datetime_tz)).unwrap();

    assert_eq!(decoded_date, date);
    assert_eq!(decoded_time, time);
    assert_eq!(decoded_datetime, datetime);
    assert_eq!(decoded_datetime_tz, datetime_tz);
}

#[cfg(feature = "uuid")]
#[test]
fn uuid_decode_accepts_native_owned_uuid_value() {
    let uuid = [
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
        0x88,
    ];

    let decoded: uuid::Uuid = decode_value(&Value::Uuid(uuid)).unwrap();
    assert_eq!(decoded.as_bytes(), &uuid);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_date_time_values_round_trip_through_sqlite_text() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query("create table t(day text not null, at_time text not null, happened_at text not null, offset_at text not null);")
        .execute(&mut db)
        .await
        .unwrap();

    let day = Date {
        year: 2024,
        month: 1,
        day: 2,
    };
    let at_time = Time {
        hour: 3,
        minute: 4,
        second: 5,
        microsecond: 123_456,
    };
    let happened_at = DateTime {
        date: day,
        time: at_time,
    };
    let offset_at = DateTimeTz {
        datetime: happened_at,
        offset_seconds: 3600,
    };

    query("insert into t(day, at_time, happened_at, offset_at) values(?, ?, ?, ?)")
        .bind(day)
        .bind(at_time)
        .bind(happened_at)
        .bind(offset_at)
        .execute(&mut db)
        .await
        .unwrap();

    let scalar: DateTime = query("select happened_at from t")
        .one(&mut db)
        .await
        .unwrap();
    assert_eq!(scalar, happened_at);

    let row: (Date, Time, DateTime, DateTimeTz) =
        query("select day, at_time, happened_at, offset_at from t")
            .one(&mut db)
            .await
            .unwrap();
    assert_eq!(row, (day, at_time, happened_at, offset_at));
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_placeholders_are_rewritten_outside_literals_and_comments() {
    let rewritten = rewrite_postgres_placeholders(
        "select ? as a, '?' as literal, \"?\" as ident, ? as b -- ?\n/* ? */ and ?",
    );
    assert_eq!(
        rewritten,
        "select $1 as a, '?' as literal, \"?\" as ident, $2 as b -- ?\n/* ? */ and $3",
    );
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_placeholder_rewrite_skips_dollar_quoted_strings() {
    let rewritten =
        rewrite_postgres_placeholders("select $$?$$ as anon, $tag$?$tag$ as tagged, ? as value");
    assert_eq!(
        rewritten,
        "select $$?$$ as anon, $tag$?$tag$ as tagged, $1 as value",
    );
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_placeholder_rewrite_borrows_when_unchanged() {
    let rewritten = rewrite_postgres_placeholders("select 1 as value");
    assert!(matches!(rewritten, Cow::Borrowed(_)));
    assert_eq!(rewritten, "select 1 as value");
}

#[test]
fn owned_row_get_decodes_custom_type() {
    let row = Row {
        columns: vec![Column {
            name: "email".into(),
            nullable: false,
            kind: ColumnType::Sqlite(Some("text".into())),
        }],
        values: vec![Value::String("ada@example.com".into())],
    };

    let email: Email = row.get("email").unwrap();
    assert_eq!(email, Email::parse("ada@example.com").unwrap());
}

#[test]
fn tuple_from_row_supports_custom_decode_value_types() {
    let row = Row {
        columns: vec![
            Column {
                name: "email".into(),
                nullable: false,
                kind: ColumnType::Sqlite(Some("text".into())),
            },
            Column {
                name: "score".into(),
                nullable: false,
                kind: ColumnType::Sqlite(Some("integer".into())),
            },
        ],
        values: vec![Value::String("ada@example.com".into()), Value::I64(42)],
    };

    let value: (Email, i64) = row.decode().unwrap();
    assert_eq!(value, (Email::parse("ada@example.com").unwrap(), 42));
}

#[test]
fn owned_row_decode_uses_from_row() {
    let row = Row {
        columns: vec![
            Column {
                name: "id".into(),
                nullable: false,
                kind: ColumnType::Sqlite(Some("integer".into())),
            },
            Column {
                name: "email".into(),
                nullable: false,
                kind: ColumnType::Sqlite(Some("text".into())),
            },
            Column {
                name: "score".into(),
                nullable: false,
                kind: ColumnType::Sqlite(Some("integer".into())),
            },
        ],
        values: vec![
            Value::I64(7),
            Value::String("ada@example.com".into()),
            Value::I64(42),
        ],
    };

    let user: User = row.decode().unwrap();
    assert_eq!(
        user,
        User {
            id: 7,
            email: Email::parse("ada@example.com").unwrap(),
            score: 42,
        }
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quex_connect_sqlite_opens_one_connection() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    assert_eq!(db.driver(), Driver::Sqlite);
    query("create table t(id integer primary key);")
        .execute(&mut db)
        .await
        .unwrap();
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_lazily_creates_and_reuses_sqlite_connections() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .build()
        .await
        .unwrap();

    assert_eq!(pool.idle_count(), 0);

    {
        let mut conn = pool.acquire().await.unwrap();
        query("create table t(id integer primary key, name text not null);")
            .execute(&mut conn)
            .await
            .unwrap();
        query("insert into t(name) values ('Ada');")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    assert_eq!(pool.idle_count(), 1);

    let mut conn = pool.acquire().await.unwrap();
    let mut rows = conn.query("select count(*) as count from t").await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_i64("count").unwrap(), 1);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_discards_broken_connections_after_error() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .build()
        .await
        .unwrap();

    {
        let mut conn = pool.acquire().await.unwrap();
        query("create table t(id integer primary key, name text not null);")
            .execute(&mut conn)
            .await
            .unwrap();
        let _ = conn.query("select * from definitely_missing_table").await;
    }

    assert_eq!(pool.idle_count(), 0);

    let mut conn = pool.acquire().await.unwrap();
    let err = conn.query("select count(*) from t").await.err().unwrap();
    assert!(matches!(err, Error::Sqlite(_)));
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_honors_max_size_with_blocking_acquire() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .build()
        .await
        .unwrap();

    let conn = pool.acquire().await.unwrap();
    let second = timeout(TokioDuration::from_millis(50), pool.acquire()).await;
    assert!(second.is_err());
    drop(conn);

    let second = timeout(TokioDuration::from_secs(1), pool.acquire()).await;
    assert!(second.is_ok());
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_acquire_respects_configured_timeout() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .acquire_timeout(TokioDuration::from_millis(50))
        .build()
        .await
        .unwrap();

    let conn = pool.acquire().await.unwrap();
    let err = pool.acquire().await.err().unwrap();
    assert!(matches!(err, Error::PoolTimedOut));
    drop(conn);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_try_acquire_and_try_begin_fail_fast_when_exhausted() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .build()
        .await
        .unwrap();

    let conn = pool.try_acquire().await.unwrap();
    let err = match pool.try_acquire().await {
        Ok(_) => panic!("expected pool exhaustion"),
        Err(err) => err,
    };
    assert!(matches!(err, Error::PoolExhausted));
    let err = match pool.try_begin().await {
        Ok(_) => panic!("expected pool exhaustion"),
        Err(err) => err,
    };
    assert!(matches!(err, Error::PoolExhausted));
    drop(conn);

    let mut tx = pool.try_begin().await.unwrap();
    query("select 1;").execute(&mut tx).await.unwrap();
    tx.rollback().await.unwrap();
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_close_rejects_new_acquires_and_drops_idle_connections() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .build()
        .await
        .unwrap();

    {
        let mut conn = pool.acquire().await.unwrap();
        query("create table t(id integer primary key);")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    assert_eq!(pool.idle_count(), 1);
    pool.close();
    assert!(pool.is_closed());
    assert_eq!(pool.idle_count(), 0);

    let err = pool.acquire().await.err().unwrap();
    assert!(matches!(err, Error::PoolClosed));
    let err = pool.try_begin().await.err().unwrap();
    assert!(matches!(err, Error::PoolClosed));
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_build_can_warm_min_connections() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .min_connections(2)
        .max_size(3)
        .build()
        .await
        .unwrap();

    assert_eq!(pool.idle_count(), 2);
    assert_eq!(pool.available_permits(), 3);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_on_connect_runs_only_for_fresh_connections() {
    let calls = Arc::new(AtomicUsize::new(0));
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .build()
        .await
        .unwrap()
        .with_hooks(Hooks::new().on_connect({
            let calls = Arc::clone(&calls);
            async move |conn: &mut PooledConnection| {
                calls.fetch_add(1, Ordering::SeqCst);
                query("create table if not exists t(id integer primary key);")
                    .execute(conn)
                    .await?;
                Ok(())
            }
        }));

    {
        let mut conn = pool.acquire().await.unwrap();
        query("insert into t default values;")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    {
        let mut conn = pool.acquire().await.unwrap();
        let count: i64 = query("select count(*) from t").one(&mut conn).await.unwrap();
        assert_eq!(count, 1);
    }

    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_on_connect_runs_for_prewarmed_connections_on_first_checkout() {
    let calls = Arc::new(AtomicUsize::new(0));
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .min_connections(1)
        .max_size(1)
        .build()
        .await
        .unwrap()
        .with_hooks(Hooks::new().on_connect({
            let calls = Arc::clone(&calls);
            async move |conn: &mut PooledConnection| {
                calls.fetch_add(1, Ordering::SeqCst);
                query("create table if not exists t(id integer primary key);")
                    .execute(conn)
                    .await?;
                Ok(())
            }
        }));

    let mut conn = pool.acquire().await.unwrap();
    query("insert into t default values;")
        .execute(&mut conn)
        .await
        .unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_before_acquire_can_replace_rejected_idle_connection() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .build()
        .await
        .unwrap()
        .with_hooks(Hooks::new().before_acquire(async |conn: &mut PooledConnection| {
            let rows: i64 = query(
                "select count(*) from sqlite_master where type = 'table' and name = 't'",
            )
            .one(conn)
            .await?;
            if rows > 0 {
                Ok(AcquireDecision::Retry)
            } else {
                Ok(AcquireDecision::Accept)
            }
        }));

    {
        let mut conn = pool.acquire().await.unwrap();
        query("create table t(id integer primary key);")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    let mut conn = pool.acquire().await.unwrap();
    let rows: i64 = query("select count(*) from sqlite_master where type = 'table' and name = 't'")
        .one(&mut conn)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_before_acquire_error_discards_candidate() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .build()
        .await
        .unwrap()
        .with_hooks(Hooks::new().before_acquire(async |conn: &mut PooledConnection| {
            let rows: i64 = query(
                "select count(*) from sqlite_master where type = 'table' and name = 't'",
            )
            .one(conn)
            .await?;
            if rows > 0 {
                Err(Error::Unsupported("reject idle connection".into()))
            } else {
                Ok(AcquireDecision::Accept)
            }
        }));

    {
        let mut conn = pool.acquire().await.unwrap();
        query("create table t(id integer primary key);")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    let err = pool.acquire().await.err().unwrap();
    assert!(matches!(err, Error::Unsupported(message) if message == "reject idle connection"));
    assert_eq!(pool.idle_count(), 0);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_before_acquire_retry_respects_acquire_timeout() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .acquire_timeout(TokioDuration::from_millis(50))
        .build()
        .await
        .unwrap()
        .with_hooks(Hooks::new().before_acquire(async |_conn: &mut PooledConnection| {
            Ok(AcquireDecision::Retry)
        }));

    let err = pool.acquire().await.err().unwrap();
    assert!(matches!(err, Error::PoolTimedOut));
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_drops_idle_connections_after_idle_timeout() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .idle_timeout(TokioDuration::from_millis(20))
        .build()
        .await
        .unwrap();

    {
        let mut conn = pool.acquire().await.unwrap();
        query("create table t(id integer primary key);")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    assert_eq!(pool.idle_count(), 1);
    tokio::time::sleep(TokioDuration::from_millis(30)).await;

    let mut conn = pool.acquire().await.unwrap();
    let err = conn.query("select count(*) from t").await.err().unwrap();
    assert!(matches!(err, Error::Sqlite(_)));
    assert_eq!(pool.idle_count(), 0);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_rotates_connections_after_max_lifetime() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .max_lifetime(TokioDuration::from_millis(20))
        .build()
        .await
        .unwrap();

    {
        let mut conn = pool.acquire().await.unwrap();
        query("create table t(id integer primary key);")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    tokio::time::sleep(TokioDuration::from_millis(30)).await;

    {
        let mut conn = pool.acquire().await.unwrap();
        let err = conn.query("select count(*) from t").await.err().unwrap();
        assert!(matches!(err, Error::Sqlite(_)));
    }

    assert_eq!(pool.idle_count(), 0);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_transaction_commits_and_reuses_connection() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .build()
        .await
        .unwrap();

    {
        let mut conn = pool.acquire().await.unwrap();
        query("create table t(id integer primary key, score integer not null);")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    {
        let mut tx = pool.begin().await.unwrap();
        query("insert into t(score) values (7);")
            .execute(&mut tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let mut conn = pool.acquire().await.unwrap();
    let mut rows = conn.query("select score from t").await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_i64(0).unwrap(), 7);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_builders_work_with_plain_connection_and_transaction() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query("create table t(id integer primary key, score integer not null);")
        .execute(&mut db)
        .await
        .unwrap();

    {
        let mut tx = db.begin().await.unwrap();
        let mut stmt = prepare("insert into t(score) values(?)")
            .run(&mut tx)
            .await
            .unwrap();
        stmt.bind(7).execute().await.unwrap();
        stmt.bind(9).execute().await.unwrap();
        tx.commit().await.unwrap();
    }

    assert_eq!(count_rows(&mut db).await.unwrap(), 2);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_builders_work_with_pool_and_pooled_transaction() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(1)
        .build()
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    query("create table t(id integer primary key, score integer not null);")
        .execute(&mut conn)
        .await
        .unwrap();

    {
        let mut tx = conn.begin().await.unwrap();
        let mut stmt = prepare("insert into t(score) values(?)")
            .run(&mut tx)
            .await
            .unwrap();
        stmt.bind(11).execute().await.unwrap();
        stmt.bind(13).execute().await.unwrap();
        tx.commit().await.unwrap();
    }

    assert_eq!(count_rows(&mut conn).await.unwrap(), 2);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_bind_exec_reuses_cached_statement_path() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query("create table t(id integer primary key, score integer not null);")
        .execute(&mut db)
        .await
        .unwrap();

    query("insert into t(score) values(?)")
        .bind(17)
        .execute(&mut db)
        .await
        .unwrap();
    query("insert into t(score) values(?)")
        .bind(19)
        .execute(&mut db)
        .await
        .unwrap();

    struct Count {
        count: i64,
    }

    impl FromRow for Count {
        fn from_row(row: &Row) -> Result<Self> {
            Ok(Self {
                count: row.get("count")?,
            })
        }
    }

    let score: Count = query("select score as count from t where score = ?")
        .bind(19)
        .one(&mut db)
        .await
        .unwrap();
    assert_eq!(score.count, 19);

    let mut rows = query("select score from t where score > ? order by score")
        .bind(10)
        .fetch(&mut db)
        .await
        .unwrap();
    let first = rows.next().await.unwrap().unwrap();
    assert_eq!(first.get_i64(0).unwrap(), 17);
    let second = rows.next().await.unwrap().unwrap();
    assert_eq!(second.get_i64(0).unwrap(), 19);
    assert!(rows.next().await.unwrap().is_none());
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalar_from_row_decodes_one_column_queries() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query(
        "create table t(id integer primary key, active integer not null, score integer not null);",
    )
    .execute(&mut db)
    .await
    .unwrap();

    for (active, score) in [(true, 17), (false, 19), (true, 23)] {
        query("insert into t(active, score) values(?, ?)")
            .bind(active)
            .bind(score)
            .execute(&mut db)
            .await
            .unwrap();
    }

    let count: i64 = query("select count(*) from t").one(&mut db).await.unwrap();
    assert_eq!(count, 3);

    let active: bool = query("select active from t where score = ?")
        .bind(17)
        .one(&mut db)
        .await
        .unwrap();
    assert!(active);

    let inactive: bool = query("select active from t where score = ?")
        .bind(19)
        .one(&mut db)
        .await
        .unwrap();
    assert!(!inactive);

    let missing: Option<i64> = query("select score from t where score = ?")
        .bind(99)
        .optional(&mut db)
        .await
        .unwrap();
    assert_eq!(missing, None);

    let first: i64 = query("select score from t order by score")
        .one(&mut db)
        .await
        .unwrap();
    assert_eq!(first, 17);

    let missing_one = query("select score from t where score = ?")
        .bind(99)
        .one::<i64>(&mut db)
        .await
        .unwrap_err();
    assert!(
        missing_one
            .to_string()
            .contains("expected at least one row")
    );

    let scores: Vec<i64> = query("select score from t order by score")
        .all(&mut db)
        .await
        .unwrap();
    assert_eq!(scores, vec![17, 19, 23]);

    let err = query("select score, active from t limit 1")
        .one::<i64>(&mut db)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("expected one column for scalar row")
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_helpers_accept_shared_pool_references() {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())
        .unwrap()
        .max_size(4)
        .build()
        .await
        .unwrap();

    query("create table t(id integer primary key, score integer not null);")
        .execute(&pool)
        .await
        .unwrap();

    for score in [17, 19, 23] {
        query("insert into t(score) values(?)")
            .bind(score)
            .execute(&pool)
            .await
            .unwrap();
    }

    let first: i64 = query("select score from t order by score")
        .one(&pool)
        .await
        .unwrap();
    assert_eq!(first, 17);

    let missing: Option<i64> = query("select score from t where score = ?")
        .bind(99)
        .optional(&pool)
        .await
        .unwrap();
    assert_eq!(missing, None);

    let scores: Vec<i64> = query("select score from t order by score")
        .all(&pool)
        .await
        .unwrap();
    assert_eq!(scores, vec![17, 19, 23]);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tuple_from_row_decodes_scalar_columns() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query("create table t(id integer primary key, name text not null, active integer not null, score integer not null);")
        .execute(&mut db)
        .await
        .unwrap();

    for (name, active, score) in [("Ada", true, 37), ("Grace", false, 42), ("Alan", true, 31)] {
        query("insert into t(name, active, score) values(?, ?, ?)")
            .bind(name)
            .bind(active)
            .bind(score)
            .execute(&mut db)
            .await
            .unwrap();
    }

    let ada: (String, bool, i64) = query("select name, active, score from t where name = ?")
        .bind("Ada")
        .one(&mut db)
        .await
        .unwrap();
    assert_eq!(ada, ("Ada".into(), true, 37));

    let missing: Option<(String, i64)> = query("select name, score from t where name = ?")
        .bind("Missing")
        .optional(&mut db)
        .await
        .unwrap();
    assert_eq!(missing, None);

    let rows: Vec<(String, bool)> = query("select name, active from t order by id")
        .all(&mut db)
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ("Ada".into(), true),
            ("Grace".into(), false),
            ("Alan".into(), true),
        ]
    );

    let single: (i64,) = query("select score from t where name = ?")
        .bind("Grace")
        .one(&mut db)
        .await
        .unwrap();
    assert_eq!(single, (42,));

    let err = query("select name, active, score from t limit 1")
        .one::<(String, bool)>(&mut db)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("expected 2 columns for tuple row"));
}

#[cfg(feature = "chrono")]
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chrono_feature_decodes_date_time_values() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query("create table t(day text not null, at_time text not null, happened_at text not null, offset_at text not null);")
        .execute(&mut db)
        .await
        .unwrap();

    let day = chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let at_time = chrono::NaiveTime::from_hms_micro_opt(3, 4, 5, 123_456).unwrap();
    let happened_at = day.and_time(at_time);
    let offset_at = chrono::DateTime::<chrono::FixedOffset>::from_naive_utc_and_offset(
        happened_at,
        chrono::FixedOffset::east_opt(3600).unwrap(),
    );

    query("insert into t(day, at_time, happened_at, offset_at) values(?, ?, ?, ?)")
        .bind(day)
        .bind(at_time)
        .bind(happened_at)
        .bind(offset_at)
        .execute(&mut db)
        .await
        .unwrap();

    let scalar: chrono::NaiveDateTime = query("select happened_at from t")
        .one(&mut db)
        .await
        .unwrap();
    assert_eq!(scalar, happened_at);

    let row: (
        chrono::NaiveDate,
        chrono::NaiveTime,
        chrono::NaiveDateTime,
        chrono::DateTime<chrono::FixedOffset>,
    ) = query("select day, at_time, happened_at, offset_at from t")
        .one(&mut db)
        .await
        .unwrap();
    assert_eq!(row, (day, at_time, happened_at, offset_at));
}

#[cfg(feature = "time")]
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn time_feature_decodes_date_time_values() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query("create table t(day text not null, at_time text not null, happened_at text not null, offset_at text not null);")
        .execute(&mut db)
        .await
        .unwrap();

    let day = time::Date::from_calendar_date(2024, time::Month::January, 2).unwrap();
    let at_time = time::Time::from_hms_micro(3, 4, 5, 123_456).unwrap();
    let happened_at = time::PrimitiveDateTime::new(day, at_time);
    let offset_at = happened_at.assume_utc();

    query("insert into t(day, at_time, happened_at, offset_at) values(?, ?, ?, ?)")
        .bind(day)
        .bind(at_time)
        .bind(happened_at)
        .bind(offset_at)
        .execute(&mut db)
        .await
        .unwrap();

    let scalar: time::PrimitiveDateTime = query("select happened_at from t")
        .one(&mut db)
        .await
        .unwrap();
    assert_eq!(scalar, happened_at);

    let row: (
        time::Date,
        time::Time,
        time::PrimitiveDateTime,
        time::OffsetDateTime,
    ) = query("select day, at_time, happened_at, offset_at from t")
        .one(&mut db)
        .await
        .unwrap();
    assert_eq!(row, (day, at_time, happened_at, offset_at));
}

#[cfg(feature = "json")]
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_feature_round_trips_json_values() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query("create table t(payload text not null);")
        .execute(&mut db)
        .await
        .unwrap();

    let payload = serde_json::json!({
        "name": "Ada",
        "score": 42,
        "active": true,
        "tags": ["math", "logic"]
    });

    query("insert into t(payload) values(?)")
        .bind(payload.clone())
        .execute(&mut db)
        .await
        .unwrap();

    let scalar: serde_json::Value = query("select payload from t").one(&mut db).await.unwrap();
    assert_eq!(scalar, payload);

    let row: (serde_json::Value,) = query("select payload from t").one(&mut db).await.unwrap();
    assert_eq!(row, (payload,));
}

#[cfg(feature = "uuid")]
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uuid_feature_round_trips_uuid_values() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query("create table t(id text not null);")
        .execute(&mut db)
        .await
        .unwrap();

    let id = uuid::Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();

    query("insert into t(id) values(?)")
        .bind(id)
        .execute(&mut db)
        .await
        .unwrap();

    let scalar: uuid::Uuid = query("select id from t").one(&mut db).await.unwrap();
    assert_eq!(scalar, id);

    let row: (uuid::Uuid,) = query("select id from t").one(&mut db).await.unwrap();
    assert_eq!(row, (id,));
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_execute_runs_sql_without_rows() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    let schema = query("create table t(id integer primary key, score integer not null);")
        .execute(&mut db)
        .await
        .unwrap();
    assert_eq!(schema.rows_affected, 0);

    let inserted = query("insert into t(score) values (31);")
        .execute(&mut db)
        .await
        .unwrap();
    assert_eq!(inserted.rows_affected, 1);
    assert_eq!(inserted.last_insert_id, Some(1));

    let updated = query("update t set score = 41 where id = 1;")
        .execute(&mut db)
        .await
        .unwrap();
    assert_eq!(updated.rows_affected, 1);

    let mut tx = db.begin().await.unwrap();
    query("insert into t(score) values (37);")
        .execute(&mut tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(count_rows(&mut db).await.unwrap(), 2);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_execute_without_binds_returns_exec_result() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query("create table t(id integer primary key, score integer not null);")
        .execute(&mut db)
        .await
        .unwrap();
    let inserted = query("insert into t(score) values (41);")
        .execute(&mut db)
        .await
        .unwrap();

    assert_eq!(inserted.rows_affected, 1);
    assert_eq!(count_rows(&mut db).await.unwrap(), 1);
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_fetch_streams_bound_prepared_rows() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query("create table t(id integer primary key, score integer not null);")
        .execute(&mut db)
        .await
        .unwrap();
    for score in [3, 5, 8] {
        query("insert into t(score) values(?)")
            .bind(score)
            .execute(&mut db)
            .await
            .unwrap();
    }

    let mut rows = query("select score from t where score >= ? order by score")
        .bind(5)
        .fetch(&mut db)
        .await
        .unwrap();
    let first = rows.next().await.unwrap().unwrap();
    assert_eq!(first.get_i64(0).unwrap(), 5);
    let second = rows.next().await.unwrap().unwrap();
    assert_eq!(second.get_i64(0).unwrap(), 8);
    assert!(rows.next().await.unwrap().is_none());
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn arena_param_source_executes_without_owned_param_values() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query("create table t(name text not null, data blob not null);")
        .execute(&mut db)
        .await
        .unwrap();

    let mut arena = ArenaParams::new();
    arena.push_str("Ada");
    arena.push_bytes(&[0, 1, 2, 255]);

    let mut stmt = db
        .prepare("insert into t(name, data) values(?, ?)")
        .await
        .unwrap();
    stmt.exec_source(&arena).await.unwrap();

    query("insert into t(name, data) values(?, ?)")
        .bind("Grace")
        .bind(vec![3, 5, 8])
        .execute(&mut db)
        .await
        .unwrap();

    let rows = query("select name, data from t order by name")
        .all::<(String, Vec<u8>)>(&mut db)
        .await
        .unwrap();

    assert_eq!(
        rows,
        vec![
            ("Ada".to_owned(), vec![0, 1, 2, 255]),
            ("Grace".to_owned(), vec![3, 5, 8]),
        ]
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_decode_ref_and_owned_decoding_work() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query("create table users(id integer primary key, name text not null, email text not null, score integer not null);")
        .execute(&mut db)
        .await
        .unwrap();

    let ada = Email::parse("ada@example.com").unwrap();
    let mut insert = prepare("insert into users(name, email, score) values(?, ?, ?)")
        .run(&mut db)
        .await
        .unwrap();
    insert
        .bind("Ada")
        .bind(&ada)
        .bind(37)
        .execute()
        .await
        .unwrap();

    let mut rows = query("select id, name, email, score from users")
        .fetch(&mut db)
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();

    let id: i64 = row.get("id").unwrap();
    let name: &str = row.get("name").unwrap();
    let email: &str = row.get("email").unwrap();
    assert_eq!((id, name, email), (1, "Ada", "ada@example.com"));

    let user_ref: UserRef<'_> = row.decode_ref().unwrap();
    assert_eq!(
        user_ref,
        UserRef {
            id: 1,
            name: "Ada",
            email: "ada@example.com",
        }
    );

    let user: User = row.decode().unwrap();
    assert_eq!(
        user,
        User {
            id: 1,
            email: Email::parse("ada@example.com").unwrap(),
            score: 37,
        }
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rows_collect_decoded_work() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query("create table users(id integer primary key, name text not null, email text not null, score integer not null);")
        .execute(&mut db)
        .await
        .unwrap();

    let mut insert = prepare("insert into users(name, email, score) values(?, ?, ?)")
        .run(&mut db)
        .await
        .unwrap();
    insert
        .bind("Ada")
        .bind(Email::parse("ada@example.com").unwrap())
        .bind(37)
        .execute()
        .await
        .unwrap();
    insert
        .bind("Grace")
        .bind(Email::parse("grace@example.com").unwrap())
        .bind(42)
        .execute()
        .await
        .unwrap();

    let users: Vec<User> = query("select id, email, score from users order by id")
        .fetch(&mut db)
        .await
        .unwrap()
        .collect_decoded()
        .await
        .unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0].id, 1);
    assert_eq!(users[1].email, Email::parse("grace@example.com").unwrap());
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_execute_returns_metadata_and_helpers_are_ergonomic() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    let schema = query(
        "create table users(id integer primary key, name text not null, email text not null, score integer not null);",
    )
    .execute(&mut db)
    .await
    .unwrap();
    assert_eq!(schema.rows_affected, 0);

    let first = query("insert into users(name, email, score) values(?, ?, ?)")
        .bind("Ada")
        .bind(Email::parse("ada@example.com").unwrap())
        .bind(37)
        .execute(&mut db)
        .await
        .unwrap();
    assert_eq!(first.rows_affected, 1);
    assert_eq!(first.last_insert_id, Some(1));

    let second = query("insert into users(name, email, score) values(?, ?, ?)")
        .bind("Grace")
        .bind(Email::parse("grace@example.com").unwrap())
        .bind(42)
        .execute(&mut db)
        .await
        .unwrap();
    assert_eq!(second.rows_affected, 1);
    assert_eq!(second.last_insert_id, Some(2));

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct UserWithName {
        id: i64,
        name: String,
        email: Email,
        score: i64,
    }

    impl FromRow for UserWithName {
        fn from_row(row: &Row) -> Result<Self> {
            Ok(Self {
                id: row.get("id")?,
                name: row.get("name")?,
                email: row.get("email")?,
                score: row.get("score")?,
            })
        }
    }

    let top: User = query("select id, email, score from users where score = ?")
        .bind(42)
        .one(&mut db)
        .await
        .unwrap();
    assert_eq!(top.id, 2);

    let missing: Option<User> = query("select id, email, score from users where score = ?")
        .bind(99)
        .optional(&mut db)
        .await
        .unwrap();
    assert!(missing.is_none());

    let all: Vec<UserWithName> = query("select id, name, email, score from users order by id")
        .all(&mut db)
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].name, "Ada");
    assert_eq!(all[1].name, "Grace");

    let high_scores: Vec<UserWithName> =
        query("select id, name, email, score from users where score > ? order by id")
            .bind(40)
            .all(&mut db)
            .await
            .unwrap();
    assert_eq!(high_scores.len(), 1);
    assert_eq!(high_scores[0].name, "Grace");
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_one_optional_all_helpers_work() {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory())
        .await
        .unwrap();

    query(
        "create table users(id integer primary key, name text not null, email text not null, score integer not null);",
    )
    .execute(&mut db)
    .await
    .unwrap();

    let mut insert = prepare("insert into users(name, email, score) values(?, ?, ?)")
        .run(&mut db)
        .await
        .unwrap();
    let first = insert
        .bind("Ada")
        .bind(Email::parse("ada@example.com").unwrap())
        .bind(37)
        .exec()
        .await
        .unwrap();
    assert_eq!(first.rows_affected, 1);
    let second = insert
        .bind("Grace")
        .bind(Email::parse("grace@example.com").unwrap())
        .bind(42)
        .exec()
        .await
        .unwrap();
    assert_eq!(second.rows_affected, 1);

    let mut lookup = prepare("select id, email, score from users where score = ?")
        .run(&mut db)
        .await
        .unwrap();
    let user: User = lookup.bind(42).one().await.unwrap();
    assert_eq!(user.id, 2);

    let none: Option<User> = lookup.bind(99).optional().await.unwrap();
    assert!(none.is_none());

    let all: Vec<User> = lookup.bind(42).all().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].email, Email::parse("grace@example.com").unwrap());
}
