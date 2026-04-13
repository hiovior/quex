# quex

Query execution engine for mysql, postgres and sqlite.

It's a small rust crate for running sql without picking a different api for
each database.

Under the hood it talks to the database libraries through ffi:

- `libpq` for postgres
- `libmariadb` for mysql / mariadb
- `sqlite3` for sqlite

So the driver layer stays close to the native clients. the goal is to keep it
as performant as possible without making the public api awkward.

It is not an orm, and it does not try to hide sql. you write the query, bind the
values, and read the rows back into your own types.

The main crate is `quex`.

`quex-driver` is the lower level crate underneath it. most people probably want
to use `quex` directly.

```rust
use quex::{FromRow, Pool, Row, SqliteConnectOptions, query};

struct User {
    id: i64,
    name: String,
}

impl FromRow for User {
    fn from_row(row: &Row) -> quex::Result<Self> {
        Ok(Self {
            id: row.get("id")?,
            name: row.get("name")?,
        })
    }
}

async fn run() -> quex::Result<()> {
    let pool = Pool::connect(SqliteConnectOptions::new().in_memory())?
        .max_size(4)
        .build()?;
    let mut db = pool.acquire().await?;

    quex::query("create table users(id integer primary key, name text not null)")
        .execute(&mut db)
        .await?;

    quex::query("insert into users(name) values(?)")
        .bind("ada")
        .execute(&mut db)
        .await?;

    let users = quex::query("select id, name from users")
        .all::<User>(&mut db)
        .await?;

    Ok(())
}
```

Parameters use `?`. When the connection is postgres, `quex` rewrites those to `$1`, `$2`, and so on
before sending the query.

The goal is to be very close to the database layer:

- plain sql
- async connections
- prepared statements
- row decoding
- mysql, postgres and sqlite behind feature flags
