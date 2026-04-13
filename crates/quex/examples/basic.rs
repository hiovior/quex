use quex_driver::mysql::{ConnectOptions, Connection, Error, Value};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let opts = ConnectOptions::new()
        .host("127.0.0.1")
        .port(3306)
        .user("root")
        .password("root");

    let mut conn = Connection::connect(opts).await?;

    let mut rows = conn.query("select id, name from users order by id").await?;
    while let Some(row) = rows.next().await? {
        println!("{} {}", row.get_i64(0)?, row.get_str("name")?);
    }

    let mut stmt = conn
        .prepare("insert into users(name, age) values(?, ?)")
        .await?;
    stmt.execute(&[Value::String("Ada".into()), Value::I64(37)])
        .await?;
    drop(stmt);

    let mut tx = conn.begin().await?;
    tx.connection()
        .query("update users set active = 1 where active = 0")
        .await?;
    tx.commit().await?;

    Ok(())
}
