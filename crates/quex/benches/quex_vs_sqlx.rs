use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures_util::TryStreamExt;
use quex::{
    MysqlConnectOptions, PostgresConnectOptions, Quex, SqliteConnectOptions, query as quex_query,
};
use sqlx::mysql::{MySqlConnectOptions, MySqlConnection, MySqlPool, MySqlPoolOptions};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::sqlite::{
    SqliteConnectOptions as SqlxSqliteConnectOptions, SqlitePool, SqlitePoolOptions,
};
use sqlx::{Connection as _, Executor as _};
use tokio::runtime::Runtime;

const MYSQL_HOST: &str = "127.0.0.1";
const MYSQL_PORT: u16 = 3306;
const MYSQL_USER: &str = "root";
const MYSQL_PASSWORD: &str = "root";
const MYSQL_DATABASE: &str = "quex_driver_perf";

const POSTGRES_HOST: &str = "127.0.0.1";
const POSTGRES_PORT: u16 = 5432;
const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "postgres";
const POSTGRES_DATABASE: &str = "postgres";

const SQLITE_DB_PATH: &str = "/tmp/quex-sqlite-bench.db";
const SCAN_ROW_COUNT: usize = 2_000;

macro_rules! bench_async {
    ($group:expr, $runtime:expr, $name:expr, $driver:expr, $body:block) => {
        $group.bench_function(BenchmarkId::new($name, $driver), |b| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                $runtime.block_on(async {
                    for _ in 0..iters {
                        $body
                    }
                });
                start.elapsed()
            })
        });
    };
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    sqlite_benchmarks(c, &runtime);
    mysql_benchmarks(c, &runtime);
    postgres_benchmarks(c, &runtime);
}

fn sqlite_benchmarks(c: &mut Criterion, runtime: &Runtime) {
    runtime
        .block_on(prepare_sqlite_schema())
        .expect("prepare sqlite schema");

    let mut group = c.benchmark_group("quex_vs_sqlx_sqlite");
    group.sample_size(20);

    let mut quex_point = runtime
        .block_on(quex_sqlite_connection())
        .expect("quex sqlite connect");
    let quex_point_sql = "select id, name, score from perf_items where id = 42";
    bench_async!(group, runtime, "point_lookup", "quex", {
        quex_point_lookup(&mut quex_point, quex_point_sql).await;
    });

    let sqlx_point = runtime
        .block_on(sqlx_sqlite_pool())
        .expect("sqlx sqlite connect");
    bench_async!(group, runtime, "point_lookup", "sqlx", {
        sqlite_sqlx_point_lookup(&sqlx_point).await;
    });

    let mut quex_bound = runtime
        .block_on(quex_sqlite_connection())
        .expect("quex sqlite connect");
    let quex_bound_sql = "select id, name, score from perf_items where id = ?";
    bench_async!(group, runtime, "cached_bound_point", "quex", {
        quex_bound_point_lookup(&mut quex_bound, quex_bound_sql).await;
    });

    let sqlx_bound = runtime
        .block_on(sqlx_sqlite_pool())
        .expect("sqlx sqlite connect");
    bench_async!(group, runtime, "bound_point", "sqlx", {
        sqlite_sqlx_bound_point_lookup(&sqlx_bound).await;
    });

    let mut quex_scalar = runtime
        .block_on(quex_sqlite_connection())
        .expect("quex sqlite connect");
    let quex_scalar_sql = "select count(*) from perf_items";
    bench_async!(group, runtime, "scalar_count", "quex", {
        quex_scalar_count(&mut quex_scalar, quex_scalar_sql).await;
    });

    let sqlx_scalar = runtime
        .block_on(sqlx_sqlite_pool())
        .expect("sqlx sqlite connect");
    bench_async!(group, runtime, "scalar_count", "sqlx", {
        sqlite_sqlx_scalar_count(&sqlx_scalar).await;
    });

    let mut quex_all = runtime
        .block_on(quex_sqlite_connection())
        .expect("quex sqlite connect");
    let quex_scan_sql = "select id, score from perf_items order by id";
    bench_async!(group, runtime, "all_scan", "quex", {
        quex_all_scan(&mut quex_all, quex_scan_sql).await;
    });

    let sqlx_all = runtime
        .block_on(sqlx_sqlite_pool())
        .expect("sqlx sqlite connect");
    bench_async!(group, runtime, "all_scan", "sqlx", {
        sqlite_sqlx_all_scan(&sqlx_all).await;
    });

    let mut quex_stream = runtime
        .block_on(quex_sqlite_connection())
        .expect("quex sqlite connect");
    bench_async!(group, runtime, "stream_scan", "quex", {
        quex_stream_scan(&mut quex_stream, quex_scan_sql).await;
    });

    let sqlx_stream = runtime
        .block_on(sqlx_sqlite_pool())
        .expect("sqlx sqlite connect");
    bench_async!(group, runtime, "stream_scan", "sqlx", {
        sqlite_sqlx_stream_scan(&sqlx_stream).await;
    });

    let mut quex_insert = runtime
        .block_on(quex_sqlite_connection())
        .expect("quex sqlite connect");
    runtime
        .block_on(quex_query("delete from perf_insert_sink").execute(&mut quex_insert))
        .expect("clear sqlite quex sink");
    let mut quex_index: i64 = 0;
    let quex_insert_sql = "insert into perf_insert_sink(name, score) values(?, ?)";
    bench_async!(group, runtime, "cached_insert", "quex", {
        quex_insert_once(&mut quex_insert, quex_insert_sql, &mut quex_index).await;
    });

    let sqlx_insert = runtime
        .block_on(sqlx_sqlite_pool())
        .expect("sqlx sqlite connect");
    runtime
        .block_on(sqlx_insert.execute("delete from perf_insert_sink"))
        .expect("clear sqlite sqlx sink");
    let mut sqlx_index: i64 = 0;
    bench_async!(group, runtime, "insert", "sqlx", {
        sqlite_sqlx_insert_once(&sqlx_insert, &mut sqlx_index).await;
    });

    group.finish();
}

fn mysql_benchmarks(c: &mut Criterion, runtime: &Runtime) {
    runtime
        .block_on(prepare_mysql_schema())
        .expect("prepare mysql schema");

    let mut group = c.benchmark_group("quex_vs_sqlx_mysql");
    group.sample_size(20);

    let mut quex_point = runtime
        .block_on(quex_mysql_connection())
        .expect("quex mysql connect");
    let quex_point_sql = "select id, name, score from perf_items where id = 42";
    bench_async!(group, runtime, "point_lookup", "quex", {
        quex_point_lookup(&mut quex_point, quex_point_sql).await;
    });

    let sqlx_point = runtime
        .block_on(sqlx_mysql_pool())
        .expect("sqlx mysql connect");
    bench_async!(group, runtime, "point_lookup", "sqlx", {
        mysql_sqlx_point_lookup(&sqlx_point).await;
    });

    let mut quex_bound = runtime
        .block_on(quex_mysql_connection())
        .expect("quex mysql connect");
    let quex_bound_sql = "select id, name, score from perf_items where id = ?";
    bench_async!(group, runtime, "cached_bound_point", "quex", {
        quex_bound_point_lookup(&mut quex_bound, quex_bound_sql).await;
    });

    let sqlx_bound = runtime
        .block_on(sqlx_mysql_pool())
        .expect("sqlx mysql connect");
    bench_async!(group, runtime, "bound_point", "sqlx", {
        mysql_sqlx_bound_point_lookup(&sqlx_bound).await;
    });

    let mut quex_scalar = runtime
        .block_on(quex_mysql_connection())
        .expect("quex mysql connect");
    let quex_scalar_sql = "select count(*) from perf_items";
    bench_async!(group, runtime, "scalar_count", "quex", {
        quex_scalar_count(&mut quex_scalar, quex_scalar_sql).await;
    });

    let sqlx_scalar = runtime
        .block_on(sqlx_mysql_pool())
        .expect("sqlx mysql connect");
    bench_async!(group, runtime, "scalar_count", "sqlx", {
        mysql_sqlx_scalar_count(&sqlx_scalar).await;
    });

    let mut quex_all = runtime
        .block_on(quex_mysql_connection())
        .expect("quex mysql connect");
    let quex_scan_sql = "select id, score from perf_items order by id";
    bench_async!(group, runtime, "all_scan", "quex", {
        quex_all_scan(&mut quex_all, quex_scan_sql).await;
    });

    let sqlx_all = runtime
        .block_on(sqlx_mysql_pool())
        .expect("sqlx mysql connect");
    bench_async!(group, runtime, "all_scan", "sqlx", {
        mysql_sqlx_all_scan(&sqlx_all).await;
    });

    let mut quex_stream = runtime
        .block_on(quex_mysql_connection())
        .expect("quex mysql connect");
    bench_async!(group, runtime, "stream_scan", "quex", {
        quex_stream_scan(&mut quex_stream, quex_scan_sql).await;
    });

    let sqlx_stream = runtime
        .block_on(sqlx_mysql_pool())
        .expect("sqlx mysql connect");
    bench_async!(group, runtime, "stream_scan", "sqlx", {
        mysql_sqlx_stream_scan(&sqlx_stream).await;
    });

    let mut quex_insert = runtime
        .block_on(quex_mysql_connection())
        .expect("quex mysql connect");
    runtime
        .block_on(quex_query("truncate table perf_insert_sink").execute(&mut quex_insert))
        .expect("clear mysql quex sink");
    let mut quex_index: i64 = 0;
    let quex_insert_sql = "insert into perf_insert_sink(name, score) values(?, ?)";
    bench_async!(group, runtime, "cached_insert", "quex", {
        quex_insert_once(&mut quex_insert, quex_insert_sql, &mut quex_index).await;
    });

    let sqlx_insert = runtime
        .block_on(sqlx_mysql_pool())
        .expect("sqlx mysql connect");
    runtime
        .block_on(sqlx_insert.execute("truncate table perf_insert_sink"))
        .expect("clear mysql sqlx sink");
    let mut sqlx_index: i64 = 0;
    bench_async!(group, runtime, "insert", "sqlx", {
        mysql_sqlx_insert_once(&sqlx_insert, &mut sqlx_index).await;
    });

    group.finish();
}

fn postgres_benchmarks(c: &mut Criterion, runtime: &Runtime) {
    runtime
        .block_on(prepare_postgres_schema())
        .expect("prepare postgres schema");

    let mut group = c.benchmark_group("quex_vs_sqlx_postgres");
    group.sample_size(20);

    let mut quex_point = runtime
        .block_on(quex_postgres_connection())
        .expect("quex postgres connect");
    let quex_point_sql = "select id, name, score from perf_items_pg where id = 42";
    bench_async!(group, runtime, "point_lookup", "quex", {
        quex_point_lookup(&mut quex_point, quex_point_sql).await;
    });

    let sqlx_point = runtime
        .block_on(sqlx_postgres_pool())
        .expect("sqlx postgres connect");
    bench_async!(group, runtime, "point_lookup", "sqlx", {
        postgres_sqlx_point_lookup(&sqlx_point).await;
    });

    let mut quex_bound = runtime
        .block_on(quex_postgres_connection())
        .expect("quex postgres connect");
    let quex_bound_sql = "select id, name, score from perf_items_pg where id = ?";
    bench_async!(group, runtime, "cached_bound_point", "quex", {
        quex_bound_point_lookup(&mut quex_bound, quex_bound_sql).await;
    });

    let sqlx_bound = runtime
        .block_on(sqlx_postgres_pool())
        .expect("sqlx postgres connect");
    bench_async!(group, runtime, "bound_point", "sqlx", {
        postgres_sqlx_bound_point_lookup(&sqlx_bound).await;
    });

    let mut quex_scalar = runtime
        .block_on(quex_postgres_connection())
        .expect("quex postgres connect");
    let quex_scalar_sql = "select count(*) from perf_items_pg";
    bench_async!(group, runtime, "scalar_count", "quex", {
        quex_scalar_count(&mut quex_scalar, quex_scalar_sql).await;
    });

    let sqlx_scalar = runtime
        .block_on(sqlx_postgres_pool())
        .expect("sqlx postgres connect");
    bench_async!(group, runtime, "scalar_count", "sqlx", {
        postgres_sqlx_scalar_count(&sqlx_scalar).await;
    });

    let mut quex_all = runtime
        .block_on(quex_postgres_connection())
        .expect("quex postgres connect");
    let quex_scan_sql = "select id, score from perf_items_pg order by id";
    bench_async!(group, runtime, "all_scan", "quex", {
        quex_all_scan(&mut quex_all, quex_scan_sql).await;
    });

    let sqlx_all = runtime
        .block_on(sqlx_postgres_pool())
        .expect("sqlx postgres connect");
    bench_async!(group, runtime, "all_scan", "sqlx", {
        postgres_sqlx_all_scan(&sqlx_all).await;
    });

    let mut quex_stream = runtime
        .block_on(quex_postgres_connection())
        .expect("quex postgres connect");
    bench_async!(group, runtime, "stream_scan", "quex", {
        quex_stream_scan(&mut quex_stream, quex_scan_sql).await;
    });

    let sqlx_stream = runtime
        .block_on(sqlx_postgres_pool())
        .expect("sqlx postgres connect");
    bench_async!(group, runtime, "stream_scan", "sqlx", {
        postgres_sqlx_stream_scan(&sqlx_stream).await;
    });

    let mut quex_insert = runtime
        .block_on(quex_postgres_connection())
        .expect("quex postgres connect");
    runtime
        .block_on(quex_query("truncate table perf_insert_sink_pg").execute(&mut quex_insert))
        .expect("clear postgres quex sink");
    let mut quex_index: i64 = 0;
    let quex_insert_sql = "insert into perf_insert_sink_pg(name, score) values(?, ?)";
    bench_async!(group, runtime, "cached_insert", "quex", {
        quex_insert_once(&mut quex_insert, quex_insert_sql, &mut quex_index).await;
    });

    let sqlx_insert = runtime
        .block_on(sqlx_postgres_pool())
        .expect("sqlx postgres connect");
    runtime
        .block_on(sqlx_insert.execute("truncate table perf_insert_sink_pg"))
        .expect("clear postgres sqlx sink");
    let mut sqlx_index: i64 = 0;
    bench_async!(group, runtime, "insert", "sqlx", {
        postgres_sqlx_insert_once(&sqlx_insert, &mut sqlx_index).await;
    });

    group.finish();
}

async fn quex_point_lookup(conn: &mut Quex, sql: &str) {
    let row: (i64, String, i64) = quex_query(sql).one(conn).await.expect("quex point query");
    assert_eq!(row, (42, "name-42".into(), 294));
}

async fn quex_bound_point_lookup(conn: &mut Quex, sql: &str) {
    let row: (i64, String, i64) = quex_query(sql)
        .bind(42)
        .one(conn)
        .await
        .expect("quex bound point query");
    assert_eq!(row, (42, "name-42".into(), 294));
}

async fn quex_scalar_count(conn: &mut Quex, sql: &str) {
    let count: i64 = quex_query(sql).one(conn).await.expect("quex scalar count");
    assert_eq!(count, SCAN_ROW_COUNT as i64);
}

async fn quex_all_scan(conn: &mut Quex, sql: &str) {
    let rows: Vec<(i64, i64)> = quex_query(sql).all(conn).await.expect("quex all scan");
    assert_eq!(rows.len(), SCAN_ROW_COUNT);
    assert_eq!(sum_pairs(rows), expected_scan_sum());
}

async fn quex_stream_scan(conn: &mut Quex, sql: &str) {
    let mut rows = quex_query(sql).fetch(conn).await.expect("quex stream scan");
    let mut row_count = 0;
    let mut sum = 0;
    while let Some(row) = rows.next().await.expect("quex stream row") {
        sum += row.get::<i64>(0).expect("id");
        sum += row.get::<i64>(1).expect("score");
        row_count += 1;
    }
    assert_eq!(row_count, SCAN_ROW_COUNT);
    assert_eq!(sum, expected_scan_sum());
}

async fn quex_insert_once(conn: &mut Quex, sql: &str, index: &mut i64) {
    let name = format!("insert-{index}");
    quex_query(sql)
        .bind(name)
        .bind(*index * 11)
        .execute(conn)
        .await
        .expect("quex insert");
    *index += 1;
}

async fn sqlite_sqlx_point_lookup(pool: &SqlitePool) {
    let row = sqlx::query("select id, name, score from perf_items where id = 42")
        .fetch_one(pool)
        .await
        .expect("sqlx sqlite point query");
    assert_point_row(row);
}

async fn mysql_sqlx_point_lookup(pool: &MySqlPool) {
    let row = sqlx::query("select id, name, score from perf_items where id = 42")
        .fetch_one(pool)
        .await
        .expect("sqlx mysql point query");
    assert_point_row(row);
}

async fn postgres_sqlx_point_lookup(pool: &PgPool) {
    let row = sqlx::query("select id, name, score from perf_items_pg where id = 42")
        .fetch_one(pool)
        .await
        .expect("sqlx postgres point query");
    assert_point_row(row);
}

async fn sqlite_sqlx_bound_point_lookup(pool: &SqlitePool) {
    let row = sqlx::query("select id, name, score from perf_items where id = ?")
        .bind(42)
        .fetch_one(pool)
        .await
        .expect("sqlx sqlite bound query");
    assert_point_row(row);
}

async fn mysql_sqlx_bound_point_lookup(pool: &MySqlPool) {
    let row = sqlx::query("select id, name, score from perf_items where id = ?")
        .bind(42)
        .fetch_one(pool)
        .await
        .expect("sqlx mysql bound query");
    assert_point_row(row);
}

async fn postgres_sqlx_bound_point_lookup(pool: &PgPool) {
    let row = sqlx::query("select id, name, score from perf_items_pg where id = $1")
        .bind(42)
        .fetch_one(pool)
        .await
        .expect("sqlx postgres bound query");
    assert_point_row(row);
}

async fn sqlite_sqlx_scalar_count(pool: &SqlitePool) {
    let count: i64 = sqlx::query_scalar("select count(*) from perf_items")
        .fetch_one(pool)
        .await
        .expect("sqlx sqlite count");
    assert_eq!(count, SCAN_ROW_COUNT as i64);
}

async fn mysql_sqlx_scalar_count(pool: &MySqlPool) {
    let count: i64 = sqlx::query_scalar("select count(*) from perf_items")
        .fetch_one(pool)
        .await
        .expect("sqlx mysql count");
    assert_eq!(count, SCAN_ROW_COUNT as i64);
}

async fn postgres_sqlx_scalar_count(pool: &PgPool) {
    let count: i64 = sqlx::query_scalar("select count(*) from perf_items_pg")
        .fetch_one(pool)
        .await
        .expect("sqlx postgres count");
    assert_eq!(count, SCAN_ROW_COUNT as i64);
}

async fn sqlite_sqlx_all_scan(pool: &SqlitePool) {
    let rows = sqlx::query("select id, score from perf_items order by id")
        .fetch_all(pool)
        .await
        .expect("sqlx sqlite all scan");
    assert_scan_rows(&rows);
}

async fn mysql_sqlx_all_scan(pool: &MySqlPool) {
    let rows = sqlx::query("select id, score from perf_items order by id")
        .fetch_all(pool)
        .await
        .expect("sqlx mysql all scan");
    assert_scan_rows(&rows);
}

async fn postgres_sqlx_all_scan(pool: &PgPool) {
    let rows = sqlx::query("select id, score from perf_items_pg order by id")
        .fetch_all(pool)
        .await
        .expect("sqlx postgres all scan");
    assert_scan_rows(&rows);
}

async fn sqlite_sqlx_stream_scan(pool: &SqlitePool) {
    let mut rows = sqlx::query("select id, score from perf_items order by id").fetch(pool);
    assert_stream_rows(&mut rows).await;
}

async fn mysql_sqlx_stream_scan(pool: &MySqlPool) {
    let mut rows = sqlx::query("select id, score from perf_items order by id").fetch(pool);
    assert_stream_rows(&mut rows).await;
}

async fn postgres_sqlx_stream_scan(pool: &PgPool) {
    let mut rows = sqlx::query("select id, score from perf_items_pg order by id").fetch(pool);
    assert_stream_rows(&mut rows).await;
}

async fn sqlite_sqlx_insert_once(pool: &SqlitePool, index: &mut i64) {
    let name = format!("insert-{index}");
    sqlx::query("insert into perf_insert_sink(name, score) values(?, ?)")
        .bind(name)
        .bind(*index * 11)
        .execute(pool)
        .await
        .expect("sqlx sqlite insert");
    *index += 1;
}

async fn mysql_sqlx_insert_once(pool: &MySqlPool, index: &mut i64) {
    let name = format!("insert-{index}");
    sqlx::query("insert into perf_insert_sink(name, score) values(?, ?)")
        .bind(name)
        .bind(*index * 11)
        .execute(pool)
        .await
        .expect("sqlx mysql insert");
    *index += 1;
}

async fn postgres_sqlx_insert_once(pool: &PgPool, index: &mut i64) {
    let name = format!("insert-{index}");
    sqlx::query("insert into perf_insert_sink_pg(name, score) values($1, $2)")
        .bind(name)
        .bind(*index * 11)
        .execute(pool)
        .await
        .expect("sqlx postgres insert");
    *index += 1;
}

async fn prepare_sqlite_schema() -> Result<(), Box<dyn std::error::Error>> {
    ensure_parent_dir(SQLITE_DB_PATH)?;
    let _ = fs::remove_file(SQLITE_DB_PATH);

    let pool = sqlx_sqlite_pool().await?;
    pool.execute("pragma journal_mode = wal").await?;
    pool.execute("pragma synchronous = normal").await?;
    pool.execute("pragma temp_store = memory").await?;
    sqlite_create_tables(&pool).await?;
    sqlite_seed_items(&pool).await?;
    Ok(())
}

async fn prepare_mysql_schema() -> Result<(), Box<dyn std::error::Error>> {
    let mut admin = sqlx_mysql_admin_connection().await?;
    admin
        .execute(format!("create database if not exists `{MYSQL_DATABASE}`").as_str())
        .await?;
    drop(admin);

    let pool = sqlx_mysql_pool().await?;
    mysql_create_tables(&pool).await?;
    mysql_seed_items(&pool).await?;
    Ok(())
}

async fn prepare_postgres_schema() -> Result<(), Box<dyn std::error::Error>> {
    let pool = sqlx_postgres_pool().await?;
    postgres_create_tables(&pool).await?;
    postgres_seed_items(&pool).await?;
    Ok(())
}

async fn sqlite_create_tables(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    pool.execute(
        "create table if not exists perf_items (
            id integer primary key,
            name text not null,
            score integer not null
        )",
    )
    .await?;
    pool.execute(
        "create table if not exists perf_insert_sink (
            id integer primary key autoincrement,
            name text not null,
            score integer not null
        )",
    )
    .await?;
    pool.execute("delete from perf_items").await?;
    pool.execute("delete from perf_insert_sink").await?;
    Ok(())
}

async fn mysql_create_tables(pool: &MySqlPool) -> Result<(), sqlx::Error> {
    pool.execute(
        "create table if not exists perf_items (
            id bigint primary key,
            name varchar(64) not null,
            score bigint not null
        )",
    )
    .await?;
    pool.execute(
        "create table if not exists perf_insert_sink (
            id bigint auto_increment primary key,
            name varchar(64) not null,
            score bigint not null
        )",
    )
    .await?;
    pool.execute("truncate table perf_items").await?;
    pool.execute("truncate table perf_insert_sink").await?;
    Ok(())
}

async fn postgres_create_tables(pool: &PgPool) -> Result<(), sqlx::Error> {
    pool.execute(
        "create table if not exists perf_items_pg (
            id bigint primary key,
            name text not null,
            score bigint not null
        )",
    )
    .await?;
    pool.execute(
        "create table if not exists perf_insert_sink_pg (
            id bigserial primary key,
            name text not null,
            score bigint not null
        )",
    )
    .await?;
    pool.execute("truncate table perf_items_pg").await?;
    pool.execute("truncate table perf_insert_sink_pg").await?;
    Ok(())
}

async fn sqlite_seed_items(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    for chunk_start in (0..SCAN_ROW_COUNT).step_by(250) {
        let statement = seed_statement("perf_items", chunk_start);
        pool.execute(statement.as_str()).await?;
    }
    Ok(())
}

async fn mysql_seed_items(pool: &MySqlPool) -> Result<(), sqlx::Error> {
    for chunk_start in (0..SCAN_ROW_COUNT).step_by(250) {
        let statement = seed_statement("perf_items", chunk_start);
        pool.execute(statement.as_str()).await?;
    }
    Ok(())
}

async fn postgres_seed_items(pool: &PgPool) -> Result<(), sqlx::Error> {
    for chunk_start in (0..SCAN_ROW_COUNT).step_by(250) {
        let statement = seed_statement("perf_items_pg", chunk_start);
        pool.execute(statement.as_str()).await?;
    }
    Ok(())
}

fn seed_statement(table: &str, chunk_start: usize) -> String {
    let chunk_end = (chunk_start + 250).min(SCAN_ROW_COUNT);
    let mut statement = format!("insert into {table} (id, name, score) values ");
    for id in chunk_start..chunk_end {
        if id != chunk_start {
            statement.push(',');
        }
        let score = (id as i64) * 7;
        statement.push_str(&format!("({},'name-{}',{})", id as i64, id, score));
    }
    statement
}

fn assert_point_row<R>(row: R)
where
    R: sqlx::Row,
    for<'r> i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    assert_eq!(row.try_get::<i64, _>(0).expect("id"), 42);
    assert_eq!(row.try_get::<String, _>(1).expect("name"), "name-42");
    assert_eq!(row.try_get::<i64, _>(2).expect("score"), 294);
}

fn assert_scan_rows<R>(rows: &[R])
where
    R: sqlx::Row,
    for<'r> i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    let mut sum = 0;
    for row in rows {
        sum += row.try_get::<i64, _>(0).expect("id");
        sum += row.try_get::<i64, _>(1).expect("score");
    }
    assert_eq!(rows.len(), SCAN_ROW_COUNT);
    assert_eq!(sum, expected_scan_sum());
}

async fn assert_stream_rows<S, R>(rows: &mut S)
where
    S: futures_util::Stream<Item = Result<R, sqlx::Error>> + Unpin,
    R: sqlx::Row,
    for<'r> i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    let mut row_count = 0;
    let mut sum = 0;
    while let Some(row) = rows.try_next().await.expect("sqlx stream row") {
        sum += row.try_get::<i64, _>(0).expect("id");
        sum += row.try_get::<i64, _>(1).expect("score");
        row_count += 1;
    }
    assert_eq!(row_count, SCAN_ROW_COUNT);
    assert_eq!(sum, expected_scan_sum());
}

fn sum_pairs(rows: Vec<(i64, i64)>) -> i64 {
    rows.into_iter().map(|(id, score)| id + score).sum()
}

fn expected_scan_sum() -> i64 {
    let id_sum = ((SCAN_ROW_COUNT - 1) * SCAN_ROW_COUNT / 2) as i64;
    id_sum * 8
}

async fn quex_mysql_connection() -> quex::Result<Quex> {
    Quex::connect(
        MysqlConnectOptions::new()
            .host(MYSQL_HOST)
            .port(MYSQL_PORT.into())
            .username(MYSQL_USER)
            .password(MYSQL_PASSWORD)
            .database(MYSQL_DATABASE),
    )
    .await
}

async fn quex_postgres_connection() -> quex::Result<Quex> {
    Quex::connect(
        PostgresConnectOptions::new()
            .host(POSTGRES_HOST)
            .port(POSTGRES_PORT)
            .username(POSTGRES_USER)
            .password(POSTGRES_PASSWORD)
            .database(POSTGRES_DATABASE),
    )
    .await
}

async fn quex_sqlite_connection() -> quex::Result<Quex> {
    Quex::connect(
        SqliteConnectOptions::new()
            .path(SQLITE_DB_PATH)
            .busy_timeout(Duration::from_secs(5)),
    )
    .await
}

async fn sqlx_mysql_admin_connection() -> Result<MySqlConnection, sqlx::Error> {
    let options = MySqlConnectOptions::new()
        .host(MYSQL_HOST)
        .port(MYSQL_PORT)
        .username(MYSQL_USER)
        .password(MYSQL_PASSWORD);
    MySqlConnection::connect_with(&options).await
}

async fn sqlx_mysql_pool() -> Result<MySqlPool, sqlx::Error> {
    let options = MySqlConnectOptions::new()
        .host(MYSQL_HOST)
        .port(MYSQL_PORT)
        .username(MYSQL_USER)
        .password(MYSQL_PASSWORD)
        .database(MYSQL_DATABASE);
    MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
}

async fn sqlx_postgres_pool() -> Result<PgPool, sqlx::Error> {
    let options = PgConnectOptions::new()
        .host(POSTGRES_HOST)
        .port(POSTGRES_PORT)
        .username(POSTGRES_USER)
        .password(POSTGRES_PASSWORD)
        .database(POSTGRES_DATABASE);
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
}

async fn sqlx_sqlite_pool() -> Result<SqlitePool, sqlx::Error> {
    let options = SqlxSqliteConnectOptions::new()
        .filename(SQLITE_DB_PATH)
        .create_if_missing(true);
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
}

fn ensure_parent_dir(path: impl AsRef<Path>) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = PathBuf::from(path.as_ref()).parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
