use quex_driver::postgres::{ConnectOptions, Connection, Value};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), quex_driver::postgres::Error> {
    let mut conn = Connection::connect(
        ConnectOptions::new()
            .host("127.0.0.1")
            .port(5432)
            .user("postgres")
            .password("postgres")
            .database("postgres"),
    )
    .await?;

    let mut rows = conn
        .query("select 1::bigint as id, 'Ada'::text as name")
        .await?;
    while let Some(row) = rows.next().await? {
        println!("id={} name={}", row.get_i64("id")?, row.get_str("name")?);
    }

    let mut stmt = conn
        .prepare_cached("select $1::bigint as id, $2::text as name")
        .await?;
    let mut rows = stmt
        .execute(&[Value::I64(42), Value::String("Grace".into())])
        .await?;
    let row = rows.next().await?.unwrap();
    println!("id={} name={}", row.get_i64(0)?, row.get_str(1)?);

    Ok(())
}
