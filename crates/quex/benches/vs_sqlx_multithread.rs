use std::time::Instant;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures_util::TryStreamExt;
use quex_driver::mysql::{ConnectOptions, Connection, Value};
use sqlx::mysql::{MySqlConnectOptions, MySqlConnection};
use sqlx::{Connection as _, Executor as _, Row as _};
use tokio::runtime::Builder;

const HOST: &str = "127.0.0.1";
const PORT: u16 = 3306;
const USER: &str = "root";
const PASSWORD: &str = "root";
const DATABASE: &str = "quex_driver_perf";
const SCAN_ROW_COUNT: usize = 2_000;
const CONCURRENCY: usize = 4;

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = Builder::new_multi_thread()
        .worker_threads(CONCURRENCY)
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(prepare_schema()).expect("prepare schema");

    let mut group = c.benchmark_group("mysql_vs_sqlx_multithread");
    group.sample_size(20);

    group.bench_function(
        BenchmarkId::new("point_lookup_parallel", "quex_driver"),
        |b| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                runtime.block_on(async {
                    quex_driver_parallel_point_lookup(iters as usize)
                        .await
                        .expect("quex_driver point benchmark");
                });
                start.elapsed()
            })
        },
    );

    group.bench_function(BenchmarkId::new("point_lookup_parallel", "sqlx"), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            runtime.block_on(async {
                sqlx_parallel_point_lookup(iters as usize)
                    .await
                    .expect("sqlx point benchmark");
            });
            start.elapsed()
        })
    });

    group.bench_function(BenchmarkId::new("scan_parallel", "quex_driver"), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            runtime.block_on(async {
                quex_driver_parallel_scan(iters as usize)
                    .await
                    .expect("quex_driver scan benchmark");
            });
            start.elapsed()
        })
    });

    group.bench_function(BenchmarkId::new("scan_parallel", "sqlx"), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            runtime.block_on(async {
                sqlx_parallel_scan(iters as usize)
                    .await
                    .expect("sqlx scan benchmark");
            });
            start.elapsed()
        })
    });

    group.bench_function(
        BenchmarkId::new("prepared_insert_parallel", "quex_driver"),
        |b| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                runtime.block_on(async {
                    quex_driver_parallel_insert(iters as usize)
                        .await
                        .expect("quex_driver insert benchmark");
                });
                start.elapsed()
            })
        },
    );

    group.bench_function(BenchmarkId::new("prepared_insert_parallel", "sqlx"), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            runtime.block_on(async {
                sqlx_parallel_insert(iters as usize)
                    .await
                    .expect("sqlx insert benchmark");
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

async fn quex_driver_parallel_point_lookup(iters: usize) -> Result<(), Box<dyn std::error::Error>> {
    let per_task = per_task_iters(iters);
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        handles.push(tokio::spawn(async move {
            let mut conn = quex_driver_connection().await.expect("quex_driver connect");
            for _ in 0..per_task {
                quex_driver_point_lookup_once(&mut conn).await;
            }
        }));
    }
    for handle in handles {
        handle.await?;
    }
    Ok(())
}

async fn sqlx_parallel_point_lookup(iters: usize) -> Result<(), Box<dyn std::error::Error>> {
    let per_task = per_task_iters(iters);
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        handles.push(tokio::spawn(async move {
            let mut conn = sqlx_benchmark_connection().await.expect("sqlx connect");
            for _ in 0..per_task {
                sqlx_point_lookup_once(&mut conn).await;
            }
        }));
    }
    for handle in handles {
        handle.await?;
    }
    Ok(())
}

async fn quex_driver_parallel_scan(iters: usize) -> Result<(), Box<dyn std::error::Error>> {
    let per_task = per_task_iters(iters);
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        handles.push(tokio::spawn(async move {
            let mut conn = quex_driver_connection().await.expect("quex_driver connect");
            for _ in 0..per_task {
                quex_driver_scan_once(&mut conn).await;
            }
        }));
    }
    for handle in handles {
        handle.await?;
    }
    Ok(())
}

async fn sqlx_parallel_scan(iters: usize) -> Result<(), Box<dyn std::error::Error>> {
    let per_task = per_task_iters(iters);
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        handles.push(tokio::spawn(async move {
            let mut conn = sqlx_benchmark_connection().await.expect("sqlx connect");
            for _ in 0..per_task {
                sqlx_scan_once(&mut conn).await;
            }
        }));
    }
    for handle in handles {
        handle.await?;
    }
    Ok(())
}

async fn quex_driver_parallel_insert(iters: usize) -> Result<(), Box<dyn std::error::Error>> {
    {
        let mut conn = quex_driver_connection().await?;
        conn.query("truncate table perf_insert_sink").await?;
    }

    let per_task = per_task_iters(iters);
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for task_id in 0..CONCURRENCY {
        handles.push(tokio::spawn(async move {
            let mut conn = quex_driver_connection().await.expect("quex_driver connect");
            let mut stmt = conn
                .prepare("insert into perf_insert_sink(name, score) values(?, ?)")
                .await
                .expect("prepare stmt");
            let mut index = task_id as i64 * per_task as i64;
            for _ in 0..per_task {
                quex_driver_insert_once(&mut stmt, &mut index).await;
            }
        }));
    }
    for handle in handles {
        handle.await?;
    }
    Ok(())
}

async fn sqlx_parallel_insert(iters: usize) -> Result<(), Box<dyn std::error::Error>> {
    {
        let mut conn = sqlx_benchmark_connection().await?;
        conn.execute("truncate table perf_insert_sink").await?;
    }

    let per_task = per_task_iters(iters);
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for task_id in 0..CONCURRENCY {
        handles.push(tokio::spawn(async move {
            let mut conn = sqlx_benchmark_connection().await.expect("sqlx connect");
            let mut index = task_id as i64 * per_task as i64;
            for _ in 0..per_task {
                sqlx_insert_once(&mut conn, &mut index).await;
            }
        }));
    }
    for handle in handles {
        handle.await?;
    }
    Ok(())
}

fn per_task_iters(iters: usize) -> usize {
    iters.max(CONCURRENCY).div_ceil(CONCURRENCY)
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
