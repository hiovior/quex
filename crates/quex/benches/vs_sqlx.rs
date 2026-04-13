use std::time::Instant;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures_util::TryStreamExt;
use quex_driver::mysql::{ConnectOptions, Connection, Value};
use sqlx::mysql::{MySqlConnectOptions, MySqlConnection};
use sqlx::{Connection as _, Executor as _, Row as _};
use tokio::runtime::Runtime;

const HOST: &str = "127.0.0.1";
const PORT: u16 = 3306;
const USER: &str = "root";
const PASSWORD: &str = "root";
const DATABASE: &str = "quex_driver_perf";
const SCAN_ROW_COUNT: usize = 2_000;

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    runtime.block_on(prepare_schema()).expect("prepare schema");

    let mut group = c.benchmark_group("mysql_vs_sqlx");
    group.sample_size(20);

    let mut quex_driver_point = runtime
        .block_on(quex_driver_connection())
        .expect("quex_driver connect");
    group.bench_function(BenchmarkId::new("point_lookup", "quex_driver"), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            runtime.block_on(async {
                for _ in 0..iters {
                    quex_driver_point_lookup_once(&mut quex_driver_point).await;
                }
            });
            start.elapsed()
        })
    });

    let mut sqlx_point = runtime
        .block_on(sqlx_benchmark_connection())
        .expect("sqlx connect");
    group.bench_function(BenchmarkId::new("point_lookup", "sqlx"), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            runtime.block_on(async {
                for _ in 0..iters {
                    sqlx_point_lookup_once(&mut sqlx_point).await;
                }
            });
            start.elapsed()
        })
    });

    let mut quex_driver_prepared_point_conn = runtime
        .block_on(quex_driver_connection())
        .expect("quex_driver connect");
    let mut quex_driver_prepared_point = runtime
        .block_on(
            quex_driver_prepared_point_conn
                .prepare("select id, name, score from perf_items where id = ?"),
        )
        .expect("prepare quex_driver select");
    group.bench_function(
        BenchmarkId::new("prepared_point_lookup", "quex_driver"),
        |b| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                runtime.block_on(async {
                    for _ in 0..iters {
                        quex_driver_prepared_point_lookup_once(&mut quex_driver_prepared_point)
                            .await;
                    }
                });
                start.elapsed()
            })
        },
    );

    let mut sqlx_prepared_point = runtime
        .block_on(sqlx_benchmark_connection())
        .expect("sqlx connect");
    group.bench_function(BenchmarkId::new("prepared_point_lookup", "sqlx"), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            runtime.block_on(async {
                for _ in 0..iters {
                    sqlx_prepared_point_lookup_once(&mut sqlx_prepared_point).await;
                }
            });
            start.elapsed()
        })
    });

    let mut quex_driver_scan = runtime
        .block_on(quex_driver_connection())
        .expect("quex_driver connect");
    group.bench_function(BenchmarkId::new("scan", "quex_driver"), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            runtime.block_on(async {
                for _ in 0..iters {
                    quex_driver_scan_once(&mut quex_driver_scan).await;
                }
            });
            start.elapsed()
        })
    });

    let mut sqlx_scan = runtime
        .block_on(sqlx_benchmark_connection())
        .expect("sqlx connect");
    group.bench_function(BenchmarkId::new("scan", "sqlx"), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            runtime.block_on(async {
                for _ in 0..iters {
                    sqlx_scan_once(&mut sqlx_scan).await;
                }
            });
            start.elapsed()
        })
    });

    let mut quex_driver_insert = runtime
        .block_on(quex_driver_connection())
        .expect("quex_driver connect");
    runtime
        .block_on(async {
            quex_driver_insert
                .query("truncate table perf_insert_sink")
                .await
        })
        .expect("truncate sink");
    let mut quex_driver_stmt = runtime
        .block_on(
            quex_driver_insert.prepare("insert into perf_insert_sink(name, score) values(?, ?)"),
        )
        .expect("prepare quex_driver stmt");
    let mut quex_driver_insert_index = 0;
    group.bench_function(BenchmarkId::new("prepared_insert", "quex_driver"), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            runtime.block_on(async {
                for _ in 0..iters {
                    quex_driver_insert_once(&mut quex_driver_stmt, &mut quex_driver_insert_index)
                        .await;
                }
            });
            start.elapsed()
        })
    });
    drop(quex_driver_stmt);

    let mut sqlx_insert = runtime
        .block_on(sqlx_benchmark_connection())
        .expect("sqlx connect");
    runtime
        .block_on(async { sqlx_insert.execute("truncate table perf_insert_sink").await })
        .expect("truncate sink");
    let mut sqlx_insert_index = 0;
    group.bench_function(BenchmarkId::new("prepared_insert", "sqlx"), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            runtime.block_on(async {
                for _ in 0..iters {
                    sqlx_insert_once(&mut sqlx_insert, &mut sqlx_insert_index).await;
                }
            });
            start.elapsed()
        })
    });

    group.finish();
}

async fn prepare_schema() -> Result<(), Box<dyn std::error::Error>> {
    let mut admin = sqlx_admin_connection().await?;
    admin
        .execute(format!("create database if not exists `{DATABASE}`").as_str())
        .await?;
    drop(admin);

    let mut conn = sqlx_benchmark_connection().await?;
    conn.execute(
        r#"
        create table if not exists perf_items (
            id bigint primary key,
            name varchar(64) not null,
            score bigint not null
        )
        "#,
    )
    .await?;
    conn.execute(
        r#"
        create table if not exists perf_insert_sink (
            id bigint auto_increment primary key,
            name varchar(64) not null,
            score bigint not null
        )
        "#,
    )
    .await?;
    conn.execute("truncate table perf_items").await?;
    conn.execute("truncate table perf_insert_sink").await?;

    for chunk_start in (0..SCAN_ROW_COUNT).step_by(250) {
        let chunk_end = (chunk_start + 250).min(SCAN_ROW_COUNT);
        let mut statement = String::from("insert into perf_items (id, name, score) values ");
        for id in chunk_start..chunk_end {
            if id != chunk_start {
                statement.push(',');
            }
            let score = (id as i64) * 7;
            statement.push_str(&format!("({},'name-{}',{})", id as i64, id, score));
        }
        conn.execute(statement.as_str()).await?;
    }

    Ok(())
}

async fn quex_driver_point_lookup_once(conn: &mut Connection) {
    let mut rows = conn
        .query("select id, name, score from perf_items where id = 42")
        .await
        .expect("quex_driver point query");
    let row = rows.next().await.expect("fetch point row").expect("row");
    assert_eq!(row.get_i64(0).expect("id"), 42);
    assert_eq!(row.get_str(1).expect("name"), "name-42");
    assert_eq!(row.get_i64(2).expect("score"), 294);
}

async fn sqlx_point_lookup_once(conn: &mut MySqlConnection) {
    let row = sqlx::query("select id, name, score from perf_items where id = 42")
        .fetch_one(conn)
        .await
        .expect("sqlx point query");
    assert_eq!(row.try_get::<i64, _>(0).expect("id"), 42);
    assert_eq!(row.try_get::<String, _>(1).expect("name"), "name-42");
    assert_eq!(row.try_get::<i64, _>(2).expect("score"), 294);
}

async fn quex_driver_prepared_point_lookup_once(stmt: &mut quex_driver::mysql::Statement<'_>) {
    let mut rows = stmt
        .execute(&[Value::I64(42)])
        .await
        .expect("quex_driver prepared point query");
    let row = rows.next().await.expect("fetch point row").expect("row");
    assert_eq!(row.get_i64(0).expect("id"), 42);
    assert_eq!(row.get_str(1).expect("name"), "name-42");
    assert_eq!(row.get_i64(2).expect("score"), 294);
}

async fn sqlx_prepared_point_lookup_once(conn: &mut MySqlConnection) {
    let row = sqlx::query("select id, name, score from perf_items where id = ?")
        .bind(i64::from(42))
        .fetch_one(conn)
        .await
        .expect("sqlx prepared point query");
    assert_eq!(row.try_get::<i64, _>(0).expect("id"), 42);
    assert_eq!(row.try_get::<String, _>(1).expect("name"), "name-42");
    assert_eq!(row.try_get::<i64, _>(2).expect("score"), 294);
}

async fn quex_driver_scan_once(conn: &mut Connection) {
    let mut rows = conn
        .query("select id, score from perf_items order by id")
        .await
        .expect("quex_driver scan query");
    let mut row_count = 0;
    let mut sum = 0;
    while let Some(row) = rows.next().await.expect("fetch scan row") {
        sum += row.get_i64(0).expect("id");
        sum += row.get_i64(1).expect("score");
        row_count += 1;
    }
    assert_eq!(row_count, SCAN_ROW_COUNT);
    assert_eq!(sum, expected_scan_sum());
}

async fn sqlx_scan_once(conn: &mut MySqlConnection) {
    let mut rows = sqlx::query("select id, score from perf_items order by id").fetch(conn);
    let mut row_count = 0;
    let mut sum = 0;
    while let Some(row) = rows.try_next().await.expect("fetch scan row") {
        sum += row.try_get::<i64, _>(0).expect("id");
        sum += row.try_get::<i64, _>(1).expect("score");
        row_count += 1;
    }
    assert_eq!(row_count, SCAN_ROW_COUNT);
    assert_eq!(sum, expected_scan_sum());
}

async fn quex_driver_insert_once(stmt: &mut quex_driver::mysql::Statement<'_>, index: &mut i64) {
    let current = *index;
    *index += 1;
    stmt.execute(&[
        Value::String(format!("insert-{current}")),
        Value::I64(current * 5),
    ])
    .await
    .expect("quex_driver insert");
}

async fn sqlx_insert_once(conn: &mut MySqlConnection, index: &mut i64) {
    let current = *index;
    *index += 1;
    sqlx::query("insert into perf_insert_sink(name, score) values(?, ?)")
        .bind(format!("insert-{current}"))
        .bind(current * 5)
        .execute(conn)
        .await
        .expect("sqlx insert");
}

fn expected_scan_sum() -> i64 {
    let ids = (SCAN_ROW_COUNT as i64 - 1) * (SCAN_ROW_COUNT as i64) / 2;
    let scores = 7 * ids;
    ids + scores
}

async fn quex_driver_connection() -> Result<Connection, Box<dyn std::error::Error>> {
    Ok(Connection::connect(
        ConnectOptions::new()
            .host(HOST)
            .port(PORT as u32)
            .user(USER)
            .password(PASSWORD)
            .database(DATABASE),
    )
    .await?)
}

async fn sqlx_admin_connection() -> Result<MySqlConnection, Box<dyn std::error::Error>> {
    let options = MySqlConnectOptions::new()
        .host(HOST)
        .port(PORT)
        .username(USER)
        .password(PASSWORD);
    Ok(MySqlConnection::connect_with(&options).await?)
}

async fn sqlx_benchmark_connection() -> Result<MySqlConnection, Box<dyn std::error::Error>> {
    let options = MySqlConnectOptions::new()
        .host(HOST)
        .port(PORT)
        .username(USER)
        .password(PASSWORD)
        .database(DATABASE);
    Ok(MySqlConnection::connect_with(&options).await?)
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
