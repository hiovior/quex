use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
struct Measurement {
    iterations: usize,
    elapsed: Duration,
}

impl Measurement {
    fn avg_us(self) -> f64 {
        self.elapsed.as_secs_f64() * 1_000_000.0 / self.iterations as f64
    }
}

const WARMUP_ITERATIONS: usize = 10;
const POINT_LOOKUP_ITERATIONS: usize = 200;
const SCAN_ITERATIONS: usize = 50;
const SCAN_ROW_COUNT: usize = 2_000;
const INSERT_ITERATIONS: usize = 200;

fn print_header() {
    println!();
    println!("Workload                  Driver      Total ms   Avg us/op   Relative");
}

fn print_line(workload: &str, driver: &str, current: Measurement, baseline: Measurement) {
    let relative = current.elapsed.as_secs_f64() / baseline.elapsed.as_secs_f64();
    println!(
        "{:<24} {:<10} {:>9.3} {:>11.1} {:>9.2}x",
        workload,
        driver,
        current.elapsed.as_secs_f64() * 1_000.0,
        current.avg_us(),
        relative,
    );
}

fn expected_scan_sum() -> i64 {
    let ids = (SCAN_ROW_COUNT as i64 - 1) * (SCAN_ROW_COUNT as i64) / 2;
    let scores = 7 * ids;
    ids + scores
}

#[cfg(feature = "mysql")]
mod mysql {
    use super::*;
    use futures_util::TryStreamExt;
    use quex::{MysqlConnectOptions, Pool, query};
    use sqlx::mysql::{MySqlConnectOptions, MySqlConnection, MySqlPool, MySqlPoolOptions};
    use sqlx::{Connection as _, Executor as _, Row as _};

    const HOST: &str = "127.0.0.1";
    const PORT: u16 = 3306;
    const USER: &str = "root";
    const PASSWORD: &str = "root";
    const DATABASE: &str = "quex_driver_perf";

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "performance comparison against a live local MariaDB instance"]
    async fn compare_quex_to_sqlx_mysql() -> Result<(), Box<dyn std::error::Error>> {
        prepare_schema().await?;

        let quex_point = benchmark_quex_point_lookup().await?;
        let sqlx_point = benchmark_sqlx_point_lookup().await?;
        let quex_scan = benchmark_quex_scan().await?;
        let sqlx_scan = benchmark_sqlx_scan().await?;
        let quex_insert = benchmark_quex_prepared_insert().await?;
        let sqlx_insert = benchmark_sqlx_prepared_insert().await?;

        print_header();
        print_line("point lookup", "quex", quex_point, quex_point);
        print_line("point lookup", "sqlx", sqlx_point, quex_point);
        print_line("row scan", "quex", quex_scan, quex_scan);
        print_line("row scan", "sqlx", sqlx_scan, quex_scan);
        print_line("prepared insert", "quex", quex_insert, quex_insert);
        print_line("prepared insert", "sqlx", sqlx_insert, quex_insert);
        println!();
        println!("Database: mysql://{USER}:***@{HOST}:{PORT}/{DATABASE}");
        println!("Scan rows per iteration: {SCAN_ROW_COUNT}");

        Ok(())
    }

    async fn prepare_schema() -> Result<(), Box<dyn std::error::Error>> {
        let mut admin = sqlx_admin_connection().await?;
        admin
            .execute(format!("create database if not exists `{DATABASE}`").as_str())
            .await?;
        drop(admin);

        let pool = sqlx_benchmark_pool().await?;
        pool.execute(
            r#"
            create table if not exists perf_items (
                id bigint primary key,
                name varchar(64) not null,
                score bigint not null
            )
            "#,
        )
        .await?;
        pool.execute(
            r#"
            create table if not exists perf_insert_sink (
                id bigint auto_increment primary key,
                name varchar(64) not null,
                score bigint not null
            )
            "#,
        )
        .await?;
        pool.execute("truncate table perf_items").await?;
        pool.execute("truncate table perf_insert_sink").await?;

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
            pool.execute(statement.as_str()).await?;
        }

        Ok(())
    }

    async fn benchmark_quex_point_lookup() -> Result<Measurement, Box<dyn std::error::Error>> {
        let mut pool = quex_benchmark_pool().await?;
        for _ in 0..WARMUP_ITERATIONS {
            let mut rows = query("select id, name, score from perf_items where id = 42")
                .fetch(&mut pool)
                .await?;
            let row = rows.next().await?.expect("missing warmup row");
            assert_eq!(row.get_i64(0)?, 42);
            assert_eq!(row.get_str(1)?, "name-42");
            assert_eq!(row.get_i64(2)?, 294);
        }

        let start = Instant::now();
        for _ in 0..POINT_LOOKUP_ITERATIONS {
            let mut rows = query("select id, name, score from perf_items where id = 42")
                .fetch(&mut pool)
                .await?;
            let row = rows.next().await?.expect("missing point-lookup row");
            assert_eq!(row.get_i64(0)?, 42);
            assert_eq!(row.get_str(1)?, "name-42");
            assert_eq!(row.get_i64(2)?, 294);
            assert!(rows.next().await?.is_none());
        }

        Ok(Measurement {
            iterations: POINT_LOOKUP_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_sqlx_point_lookup() -> Result<Measurement, Box<dyn std::error::Error>> {
        let pool = sqlx_benchmark_pool().await?;
        for _ in 0..WARMUP_ITERATIONS {
            let row = sqlx::query("select id, name, score from perf_items where id = 42")
                .fetch_one(&pool)
                .await?;
            assert_eq!(row.try_get::<i64, _>(0)?, 42);
            assert_eq!(row.try_get::<String, _>(1)?, "name-42");
            assert_eq!(row.try_get::<i64, _>(2)?, 294);
        }

        let start = Instant::now();
        for _ in 0..POINT_LOOKUP_ITERATIONS {
            let row = sqlx::query("select id, name, score from perf_items where id = 42")
                .fetch_one(&pool)
                .await?;
            assert_eq!(row.try_get::<i64, _>(0)?, 42);
            assert_eq!(row.try_get::<String, _>(1)?, "name-42");
            assert_eq!(row.try_get::<i64, _>(2)?, 294);
        }

        Ok(Measurement {
            iterations: POINT_LOOKUP_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_quex_scan() -> Result<Measurement, Box<dyn std::error::Error>> {
        let mut pool = quex_benchmark_pool().await?;
        for _ in 0..WARMUP_ITERATIONS {
            let sum = quex_scan_once(&mut pool).await?;
            assert_eq!(sum, expected_scan_sum());
        }

        let start = Instant::now();
        for _ in 0..SCAN_ITERATIONS {
            let sum = quex_scan_once(&mut pool).await?;
            assert_eq!(sum, expected_scan_sum());
        }

        Ok(Measurement {
            iterations: SCAN_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_sqlx_scan() -> Result<Measurement, Box<dyn std::error::Error>> {
        let pool = sqlx_benchmark_pool().await?;
        for _ in 0..WARMUP_ITERATIONS {
            let sum = sqlx_scan_once(&pool).await?;
            assert_eq!(sum, expected_scan_sum());
        }

        let start = Instant::now();
        for _ in 0..SCAN_ITERATIONS {
            let sum = sqlx_scan_once(&pool).await?;
            assert_eq!(sum, expected_scan_sum());
        }

        Ok(Measurement {
            iterations: SCAN_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_quex_prepared_insert() -> Result<Measurement, Box<dyn std::error::Error>> {
        let mut pool = quex_benchmark_pool().await?;
        query("truncate table perf_insert_sink")
            .execute(&mut pool)
            .await?;

        for index in 0..WARMUP_ITERATIONS {
            query("insert into perf_insert_sink(name, score) values(?, ?)")
                .bind(format!("warmup-{index}"))
                .bind(index as i64)
                .execute(&mut pool)
                .await?;
        }

        query("truncate table perf_insert_sink")
            .execute(&mut pool)
            .await?;

        let start = Instant::now();
        for index in 0..INSERT_ITERATIONS {
            query("insert into perf_insert_sink(name, score) values(?, ?)")
                .bind(format!("insert-{index}"))
                .bind((index as i64) * 5)
                .execute(&mut pool)
                .await?;
        }

        let inserted = quex_count(&mut pool, "select count(*) from perf_insert_sink").await?;
        assert_eq!(inserted, INSERT_ITERATIONS as i64);

        Ok(Measurement {
            iterations: INSERT_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_sqlx_prepared_insert() -> Result<Measurement, Box<dyn std::error::Error>> {
        let pool = sqlx_benchmark_pool().await?;
        pool.execute("truncate table perf_insert_sink").await?;

        for index in 0..WARMUP_ITERATIONS {
            sqlx::query("insert into perf_insert_sink(name, score) values(?, ?)")
                .bind(format!("warmup-{index}"))
                .bind(index as i64)
                .execute(&pool)
                .await?;
        }

        pool.execute("truncate table perf_insert_sink").await?;

        let start = Instant::now();
        for index in 0..INSERT_ITERATIONS {
            sqlx::query("insert into perf_insert_sink(name, score) values(?, ?)")
                .bind(format!("insert-{index}"))
                .bind((index as i64) * 5)
                .execute(&pool)
                .await?;
        }

        let inserted: i64 = sqlx::query_scalar("select count(*) from perf_insert_sink")
            .fetch_one(&pool)
            .await?;
        assert_eq!(inserted, INSERT_ITERATIONS as i64);

        Ok(Measurement {
            iterations: INSERT_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn quex_scan_once(pool: &mut Pool) -> Result<i64, Box<dyn std::error::Error>> {
        let mut rows = query("select id, score from perf_items order by id")
            .fetch(pool)
            .await?;
        let mut sum = 0;
        let mut row_count = 0;
        while let Some(row) = rows.next().await? {
            sum += row.get_i64(0)?;
            sum += row.get_i64(1)?;
            row_count += 1;
        }
        assert_eq!(row_count, SCAN_ROW_COUNT);
        Ok(sum)
    }

    async fn sqlx_scan_once(pool: &MySqlPool) -> Result<i64, Box<dyn std::error::Error>> {
        let mut sum = 0;
        let mut row_count = 0;
        let mut rows = sqlx::query("select id, score from perf_items order by id").fetch(pool);
        while let Some(row) = rows.try_next().await? {
            sum += row.try_get::<i64, _>(0)?;
            sum += row.try_get::<i64, _>(1)?;
            row_count += 1;
        }
        assert_eq!(row_count, SCAN_ROW_COUNT);
        Ok(sum)
    }

    async fn quex_count(pool: &mut Pool, sql: &str) -> Result<i64, Box<dyn std::error::Error>> {
        let mut rows = query(sql).fetch(pool).await?;
        let row = rows.next().await?.expect("count query returned no row");
        Ok(row.get_i64(0)?)
    }

    async fn quex_benchmark_pool() -> Result<Pool, Box<dyn std::error::Error>> {
        Ok(Pool::connect(
            MysqlConnectOptions::new()
                .host(HOST)
                .port(PORT as u32)
                .username(USER)
                .password(PASSWORD)
                .database(DATABASE),
        )?
        .max_size(1)
        .build()
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

    async fn sqlx_benchmark_pool() -> Result<MySqlPool, Box<dyn std::error::Error>> {
        let options = MySqlConnectOptions::new()
            .host(HOST)
            .port(PORT)
            .username(USER)
            .password(PASSWORD)
            .database(DATABASE);
        Ok(MySqlPoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?)
    }
}

#[cfg(feature = "postgres")]
mod postgres {
    use super::*;
    use futures_util::TryStreamExt;
    use quex::{Pool, PostgresConnectOptions, query};
    use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
    use sqlx::{Executor as _, Row as _};

    const HOST: &str = "127.0.0.1";
    const PORT: u16 = 5432;
    const USER: &str = "postgres";
    const PASSWORD: &str = "postgres";
    const DATABASE: &str = "postgres";

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "performance comparison against a live local postgres instance"]
    async fn compare_quex_to_sqlx_postgres() -> Result<(), Box<dyn std::error::Error>> {
        prepare_schema().await?;

        let quex_point = benchmark_quex_point_lookup().await?;
        let sqlx_point = benchmark_sqlx_point_lookup().await?;
        let quex_scan = benchmark_quex_scan().await?;
        let sqlx_scan = benchmark_sqlx_scan().await?;
        let quex_insert = benchmark_quex_prepared_insert().await?;
        let sqlx_insert = benchmark_sqlx_prepared_insert().await?;

        print_header();
        print_line("point lookup", "quex", quex_point, quex_point);
        print_line("point lookup", "sqlx", sqlx_point, quex_point);
        print_line("row scan", "quex", quex_scan, quex_scan);
        print_line("row scan", "sqlx", sqlx_scan, quex_scan);
        print_line("prepared insert", "quex", quex_insert, quex_insert);
        print_line("prepared insert", "sqlx", sqlx_insert, quex_insert);
        println!();
        println!("Database: postgres://{USER}:***@{HOST}:{PORT}/{DATABASE}");
        println!("Scan rows per iteration: {SCAN_ROW_COUNT}");

        Ok(())
    }

    async fn prepare_schema() -> Result<(), Box<dyn std::error::Error>> {
        let pool = sqlx_benchmark_pool().await?;
        pool.execute(
            r#"
            create table if not exists perf_items_pg (
                id bigint primary key,
                name text not null,
                score bigint not null
            )
            "#,
        )
        .await?;
        pool.execute(
            r#"
            create table if not exists perf_insert_sink_pg (
                id bigserial primary key,
                name text not null,
                score bigint not null
            )
            "#,
        )
        .await?;
        pool.execute("truncate table perf_items_pg").await?;
        pool.execute("truncate table perf_insert_sink_pg").await?;

        for chunk_start in (0..SCAN_ROW_COUNT).step_by(250) {
            let chunk_end = (chunk_start + 250).min(SCAN_ROW_COUNT);
            let mut statement =
                String::from("insert into perf_items_pg (id, name, score) values ");
            for id in chunk_start..chunk_end {
                if id != chunk_start {
                    statement.push(',');
                }
                let score = (id as i64) * 7;
                statement.push_str(&format!("({},'name-{}',{})", id as i64, id, score));
            }
            pool.execute(statement.as_str()).await?;
        }

        Ok(())
    }

    async fn benchmark_quex_point_lookup() -> Result<Measurement, Box<dyn std::error::Error>> {
        let mut pool = quex_benchmark_pool().await?;
        for _ in 0..WARMUP_ITERATIONS {
            let mut rows = query("select id, name, score from perf_items_pg where id = 42")
                .fetch(&mut pool)
                .await?;
            let row = rows.next().await?.expect("missing warmup row");
            assert_eq!(row.get_i64(0)?, 42);
            assert_eq!(row.get_str(1)?, "name-42");
            assert_eq!(row.get_i64(2)?, 294);
        }

        let start = Instant::now();
        for _ in 0..POINT_LOOKUP_ITERATIONS {
            let mut rows = query("select id, name, score from perf_items_pg where id = 42")
                .fetch(&mut pool)
                .await?;
            let row = rows.next().await?.expect("missing point-lookup row");
            assert_eq!(row.get_i64(0)?, 42);
            assert_eq!(row.get_str(1)?, "name-42");
            assert_eq!(row.get_i64(2)?, 294);
            assert!(rows.next().await?.is_none());
        }

        Ok(Measurement {
            iterations: POINT_LOOKUP_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_sqlx_point_lookup() -> Result<Measurement, Box<dyn std::error::Error>> {
        let pool = sqlx_benchmark_pool().await?;
        for _ in 0..WARMUP_ITERATIONS {
            let row = sqlx::query("select id, name, score from perf_items_pg where id = 42")
                .fetch_one(&pool)
                .await?;
            assert_eq!(row.try_get::<i64, _>(0)?, 42);
            assert_eq!(row.try_get::<String, _>(1)?, "name-42");
            assert_eq!(row.try_get::<i64, _>(2)?, 294);
        }

        let start = Instant::now();
        for _ in 0..POINT_LOOKUP_ITERATIONS {
            let row = sqlx::query("select id, name, score from perf_items_pg where id = 42")
                .fetch_one(&pool)
                .await?;
            assert_eq!(row.try_get::<i64, _>(0)?, 42);
            assert_eq!(row.try_get::<String, _>(1)?, "name-42");
            assert_eq!(row.try_get::<i64, _>(2)?, 294);
        }

        Ok(Measurement {
            iterations: POINT_LOOKUP_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_quex_scan() -> Result<Measurement, Box<dyn std::error::Error>> {
        let mut pool = quex_benchmark_pool().await?;
        for _ in 0..WARMUP_ITERATIONS {
            let sum = quex_scan_once(&mut pool).await?;
            assert_eq!(sum, expected_scan_sum());
        }

        let start = Instant::now();
        for _ in 0..SCAN_ITERATIONS {
            let sum = quex_scan_once(&mut pool).await?;
            assert_eq!(sum, expected_scan_sum());
        }

        Ok(Measurement {
            iterations: SCAN_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_sqlx_scan() -> Result<Measurement, Box<dyn std::error::Error>> {
        let pool = sqlx_benchmark_pool().await?;
        for _ in 0..WARMUP_ITERATIONS {
            let sum = sqlx_scan_once(&pool).await?;
            assert_eq!(sum, expected_scan_sum());
        }

        let start = Instant::now();
        for _ in 0..SCAN_ITERATIONS {
            let sum = sqlx_scan_once(&pool).await?;
            assert_eq!(sum, expected_scan_sum());
        }

        Ok(Measurement {
            iterations: SCAN_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_quex_prepared_insert() -> Result<Measurement, Box<dyn std::error::Error>> {
        let mut pool = quex_benchmark_pool().await?;
        query("truncate table perf_insert_sink_pg")
            .execute(&mut pool)
            .await?;

        for index in 0..WARMUP_ITERATIONS {
            query("insert into perf_insert_sink_pg(name, score) values(?, ?)")
                .bind(format!("warmup-{index}"))
                .bind(index as i64)
                .execute(&mut pool)
                .await?;
        }

        query("truncate table perf_insert_sink_pg")
            .execute(&mut pool)
            .await?;

        let start = Instant::now();
        for index in 0..INSERT_ITERATIONS {
            query("insert into perf_insert_sink_pg(name, score) values(?, ?)")
                .bind(format!("insert-{index}"))
                .bind((index as i64) * 5)
                .execute(&mut pool)
                .await?;
        }

        let inserted = quex_count(&mut pool, "select count(*) from perf_insert_sink_pg").await?;
        assert_eq!(inserted, INSERT_ITERATIONS as i64);

        Ok(Measurement {
            iterations: INSERT_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_sqlx_prepared_insert() -> Result<Measurement, Box<dyn std::error::Error>> {
        let pool = sqlx_benchmark_pool().await?;
        pool.execute("truncate table perf_insert_sink_pg").await?;

        for index in 0..WARMUP_ITERATIONS {
            sqlx::query("insert into perf_insert_sink_pg(name, score) values($1, $2)")
                .bind(format!("warmup-{index}"))
                .bind(index as i64)
                .execute(&pool)
                .await?;
        }

        pool.execute("truncate table perf_insert_sink_pg").await?;

        let start = Instant::now();
        for index in 0..INSERT_ITERATIONS {
            sqlx::query("insert into perf_insert_sink_pg(name, score) values($1, $2)")
                .bind(format!("insert-{index}"))
                .bind((index as i64) * 5)
                .execute(&pool)
                .await?;
        }

        let inserted: i64 = sqlx::query_scalar("select count(*) from perf_insert_sink_pg")
            .fetch_one(&pool)
            .await?;
        assert_eq!(inserted, INSERT_ITERATIONS as i64);

        Ok(Measurement {
            iterations: INSERT_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn quex_scan_once(pool: &mut Pool) -> Result<i64, Box<dyn std::error::Error>> {
        let mut rows = query("select id, score from perf_items_pg order by id")
            .fetch(pool)
            .await?;
        let mut sum = 0;
        let mut row_count = 0;
        while let Some(row) = rows.next().await? {
            sum += row.get_i64(0)?;
            sum += row.get_i64(1)?;
            row_count += 1;
        }
        assert_eq!(row_count, SCAN_ROW_COUNT);
        Ok(sum)
    }

    async fn sqlx_scan_once(pool: &PgPool) -> Result<i64, Box<dyn std::error::Error>> {
        let mut sum = 0;
        let mut row_count = 0;
        let mut rows = sqlx::query("select id, score from perf_items_pg order by id").fetch(pool);
        while let Some(row) = rows.try_next().await? {
            sum += row.try_get::<i64, _>(0)?;
            sum += row.try_get::<i64, _>(1)?;
            row_count += 1;
        }
        assert_eq!(row_count, SCAN_ROW_COUNT);
        Ok(sum)
    }

    async fn quex_count(pool: &mut Pool, sql: &str) -> Result<i64, Box<dyn std::error::Error>> {
        let mut rows = query(sql).fetch(pool).await?;
        let row = rows.next().await?.expect("count query returned no row");
        Ok(row.get_i64(0)?)
    }

    async fn quex_benchmark_pool() -> Result<Pool, Box<dyn std::error::Error>> {
        Ok(Pool::connect(
            PostgresConnectOptions::new()
                .host(HOST)
                .port(PORT)
                .username(USER)
                .password(PASSWORD)
                .database(DATABASE),
        )?
        .max_size(1)
        .build()
        .await?)
    }

    async fn sqlx_benchmark_pool() -> Result<PgPool, Box<dyn std::error::Error>> {
        let options = PgConnectOptions::new()
            .host(HOST)
            .port(PORT)
            .username(USER)
            .password(PASSWORD)
            .database(DATABASE);
        Ok(PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?)
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    use futures_util::TryStreamExt;
    use quex::{Pool, SqliteConnectOptions, query};
    use sqlx::sqlite::{SqliteConnectOptions as SqlxSqliteConnectOptions, SqlitePool, SqlitePoolOptions};
    use sqlx::{ConnectOptions as _, Executor as _, Row as _};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::Duration as StdDuration;

    const DB_PATH: &str = "/tmp/quex-sqlite-perf.db";

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "performance comparison against a local sqlite database file"]
    async fn compare_quex_to_sqlx_sqlite() -> Result<(), Box<dyn std::error::Error>> {
        prepare_schema().await?;

        let quex_point = benchmark_quex_point_lookup().await?;
        let sqlx_point = benchmark_sqlx_point_lookup().await?;
        let quex_scan = benchmark_quex_scan().await?;
        let sqlx_scan = benchmark_sqlx_scan().await?;
        let quex_insert = benchmark_quex_prepared_insert().await?;
        let sqlx_insert = benchmark_sqlx_prepared_insert().await?;

        print_header();
        print_line("point lookup", "quex", quex_point, quex_point);
        print_line("point lookup", "sqlx", sqlx_point, quex_point);
        print_line("row scan", "quex", quex_scan, quex_scan);
        print_line("row scan", "sqlx", sqlx_scan, quex_scan);
        print_line("prepared insert", "quex", quex_insert, quex_insert);
        print_line("prepared insert", "sqlx", sqlx_insert, quex_insert);
        println!();
        println!("Database: sqlite://{DB_PATH}");
        println!("Scan rows per iteration: {SCAN_ROW_COUNT}");

        Ok(())
    }

    async fn prepare_schema() -> Result<(), Box<dyn std::error::Error>> {
        ensure_parent_dir(DB_PATH)?;
        let _ = fs::remove_file(DB_PATH);

        let pool = sqlx_benchmark_pool().await?;
        pool.execute("pragma journal_mode = wal").await?;
        pool.execute("pragma synchronous = normal").await?;
        pool.execute("pragma temp_store = memory").await?;
        pool.execute(
            r#"
            create table if not exists perf_items (
                id integer primary key,
                name text not null,
                score integer not null
            )
            "#,
        )
        .await?;
        pool.execute(
            r#"
            create table if not exists perf_insert_sink (
                id integer primary key autoincrement,
                name text not null,
                score integer not null
            )
            "#,
        )
        .await?;
        pool.execute("delete from perf_items").await?;
        pool.execute("delete from perf_insert_sink").await?;

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
            pool.execute(statement.as_str()).await?;
        }

        Ok(())
    }

    async fn benchmark_quex_point_lookup() -> Result<Measurement, Box<dyn std::error::Error>> {
        let mut pool = quex_benchmark_pool().await?;
        for _ in 0..WARMUP_ITERATIONS {
            let mut rows = query("select id, name, score from perf_items where id = 42")
                .fetch(&mut pool)
                .await?;
            let row = rows.next().await?.expect("missing warmup row");
            assert_eq!(row.get_i64(0)?, 42);
            assert_eq!(row.get_str(1)?, "name-42");
            assert_eq!(row.get_i64(2)?, 294);
        }

        let start = Instant::now();
        for _ in 0..POINT_LOOKUP_ITERATIONS {
            let mut rows = query("select id, name, score from perf_items where id = 42")
                .fetch(&mut pool)
                .await?;
            let row = rows.next().await?.expect("missing point-lookup row");
            assert_eq!(row.get_i64(0)?, 42);
            assert_eq!(row.get_str(1)?, "name-42");
            assert_eq!(row.get_i64(2)?, 294);
            assert!(rows.next().await?.is_none());
        }

        Ok(Measurement {
            iterations: POINT_LOOKUP_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_sqlx_point_lookup() -> Result<Measurement, Box<dyn std::error::Error>> {
        let pool = sqlx_benchmark_pool().await?;
        for _ in 0..WARMUP_ITERATIONS {
            let row = sqlx::query("select id, name, score from perf_items where id = 42")
                .fetch_one(&pool)
                .await?;
            assert_eq!(row.try_get::<i64, _>(0)?, 42);
            assert_eq!(row.try_get::<String, _>(1)?, "name-42");
            assert_eq!(row.try_get::<i64, _>(2)?, 294);
        }

        let start = Instant::now();
        for _ in 0..POINT_LOOKUP_ITERATIONS {
            let row = sqlx::query("select id, name, score from perf_items where id = 42")
                .fetch_one(&pool)
                .await?;
            assert_eq!(row.try_get::<i64, _>(0)?, 42);
            assert_eq!(row.try_get::<String, _>(1)?, "name-42");
            assert_eq!(row.try_get::<i64, _>(2)?, 294);
        }

        Ok(Measurement {
            iterations: POINT_LOOKUP_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_quex_scan() -> Result<Measurement, Box<dyn std::error::Error>> {
        let mut pool = quex_benchmark_pool().await?;
        for _ in 0..WARMUP_ITERATIONS {
            let sum = quex_scan_once(&mut pool).await?;
            assert_eq!(sum, expected_scan_sum());
        }

        let start = Instant::now();
        for _ in 0..SCAN_ITERATIONS {
            let sum = quex_scan_once(&mut pool).await?;
            assert_eq!(sum, expected_scan_sum());
        }

        Ok(Measurement {
            iterations: SCAN_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_sqlx_scan() -> Result<Measurement, Box<dyn std::error::Error>> {
        let pool = sqlx_benchmark_pool().await?;
        for _ in 0..WARMUP_ITERATIONS {
            let sum = sqlx_scan_once(&pool).await?;
            assert_eq!(sum, expected_scan_sum());
        }

        let start = Instant::now();
        for _ in 0..SCAN_ITERATIONS {
            let sum = sqlx_scan_once(&pool).await?;
            assert_eq!(sum, expected_scan_sum());
        }

        Ok(Measurement {
            iterations: SCAN_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_quex_prepared_insert() -> Result<Measurement, Box<dyn std::error::Error>> {
        let mut pool = quex_benchmark_pool().await?;
        query("delete from perf_insert_sink")
            .execute(&mut pool)
            .await?;

        for index in 0..WARMUP_ITERATIONS {
            query("insert into perf_insert_sink(name, score) values(?, ?)")
                .bind(format!("warmup-{index}"))
                .bind(index as i64)
                .execute(&mut pool)
                .await?;
        }

        query("delete from perf_insert_sink")
            .execute(&mut pool)
            .await?;

        let start = Instant::now();
        for index in 0..INSERT_ITERATIONS {
            query("insert into perf_insert_sink(name, score) values(?, ?)")
                .bind(format!("insert-{index}"))
                .bind((index as i64) * 5)
                .execute(&mut pool)
                .await?;
        }

        let inserted = quex_count(&mut pool, "select count(*) from perf_insert_sink").await?;
        assert_eq!(inserted, INSERT_ITERATIONS as i64);

        Ok(Measurement {
            iterations: INSERT_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn benchmark_sqlx_prepared_insert() -> Result<Measurement, Box<dyn std::error::Error>> {
        let pool = sqlx_benchmark_pool().await?;
        pool.execute("delete from perf_insert_sink").await?;

        for index in 0..WARMUP_ITERATIONS {
            sqlx::query("insert into perf_insert_sink(name, score) values(?, ?)")
                .bind(format!("warmup-{index}"))
                .bind(index as i64)
                .execute(&pool)
                .await?;
        }

        pool.execute("delete from perf_insert_sink").await?;

        let start = Instant::now();
        for index in 0..INSERT_ITERATIONS {
            sqlx::query("insert into perf_insert_sink(name, score) values(?, ?)")
                .bind(format!("insert-{index}"))
                .bind((index as i64) * 5)
                .execute(&pool)
                .await?;
        }

        let inserted: i64 = sqlx::query_scalar("select count(*) from perf_insert_sink")
            .fetch_one(&pool)
            .await?;
        assert_eq!(inserted, INSERT_ITERATIONS as i64);

        Ok(Measurement {
            iterations: INSERT_ITERATIONS,
            elapsed: start.elapsed(),
        })
    }

    async fn quex_scan_once(pool: &mut Pool) -> Result<i64, Box<dyn std::error::Error>> {
        let mut rows = query("select id, score from perf_items order by id")
            .fetch(pool)
            .await?;
        let mut sum = 0;
        let mut row_count = 0;
        while let Some(row) = rows.next().await? {
            sum += row.get_i64(0)?;
            sum += row.get_i64(1)?;
            row_count += 1;
        }
        assert_eq!(row_count, SCAN_ROW_COUNT);
        Ok(sum)
    }

    async fn sqlx_scan_once(pool: &SqlitePool) -> Result<i64, Box<dyn std::error::Error>> {
        let mut sum = 0;
        let mut row_count = 0;
        let mut rows = sqlx::query("select id, score from perf_items order by id").fetch(pool);
        while let Some(row) = rows.try_next().await? {
            sum += row.try_get::<i64, _>(0)?;
            sum += row.try_get::<i64, _>(1)?;
            row_count += 1;
        }
        assert_eq!(row_count, SCAN_ROW_COUNT);
        Ok(sum)
    }

    async fn quex_count(pool: &mut Pool, sql: &str) -> Result<i64, Box<dyn std::error::Error>> {
        let mut rows = query(sql).fetch(pool).await?;
        let row = rows.next().await?.expect("count query returned no row");
        Ok(row.get_i64(0)?)
    }

    async fn quex_benchmark_pool() -> Result<Pool, Box<dyn std::error::Error>> {
        Ok(Pool::connect(
            SqliteConnectOptions::new()
                .path(DB_PATH)
                .create_if_missing(true)
                .busy_timeout(StdDuration::from_secs(5)),
        )?
        .max_size(1)
        .build()
        .await?)
    }

    async fn sqlx_benchmark_pool() -> Result<SqlitePool, Box<dyn std::error::Error>> {
        let options = SqlxSqliteConnectOptions::new()
            .filename(DB_PATH)
            .create_if_missing(true);
        Ok(SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options.disable_statement_logging())
            .await?)
    }

    fn ensure_parent_dir(path: impl AsRef<Path>) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = PathBuf::from(path.as_ref()).parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(())
    }
}
