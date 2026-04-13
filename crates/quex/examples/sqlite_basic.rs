use quex_driver::sqlite::{ConnectOptions, Connection, Value};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), quex_driver::sqlite::Error> {
    let mut conn = Connection::connect(ConnectOptions::new().in_memory()).await?;

    conn.execute_batch(
        "create table users(id integer primary key, name text not null, score integer not null);",
    )
    .await?;

    {
        let mut insert = conn
            .prepare_cached("insert into users(name, score) values(?, ?)")
            .await?;
        insert
            .execute(&[Value::String("Ada".into()), Value::I64(37)])
            .await?;
    }

    let mut rows = conn
        .query("select id, name, score from users order by id")
        .await?;

    while let Some(row) = rows.next().await? {
        println!(
            "id={} name={} score={}",
            row.get_i64(0)?,
            row.get_str("name")?,
            row.get_i64(2)?,
        );
    }

    Ok(())
}
