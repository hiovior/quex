use quex::{
    Connection, Error, ExecResult, Executor, FromRow, ParamRef, ParamSource, PreparedStatement,
    Row, RowStream, SqliteConnectOptions, query,
};

#[derive(Debug)]
struct User {
    id: i64,
    name: String,
}

impl FromRow for User {
    fn from_row(row: &Row) -> Result<Self, Error> {
        Ok(Self {
            id: row.get("id")?,
            name: row.get("name")?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum ArenaParam {
    Str { start: usize, len: usize },
    Bytes { start: usize, len: usize },
}

#[derive(Debug, Clone)]
struct Query {
    sql: &'static str,
    params: Vec<ArenaParam>,
    arena: Vec<u8>,
}

impl Query {
    fn select_users_by_name() -> Self {
        Self {
            sql: "select id, name from users where name = ? order by id",
            params: Vec::new(),
            arena: Vec::new(),
        }
    }

    fn insert_user_name() -> Self {
        Self {
            sql: "insert into users(name) values(?)",
            params: Vec::new(),
            arena: Vec::new(),
        }
    }

    fn bind_str(mut self, value: &str) -> Self {
        let start = self.arena.len();
        self.arena.extend_from_slice(value.as_bytes());
        self.params.push(ArenaParam::Str {
            start,
            len: value.len(),
        });
        self
    }

    #[allow(dead_code)]
    fn bind_bytes(mut self, value: &[u8]) -> Self {
        let start = self.arena.len();
        self.arena.extend_from_slice(value);
        self.params.push(ArenaParam::Bytes {
            start,
            len: value.len(),
        });
        self
    }

    async fn fetch<'db, E>(&self, db: &'db mut E) -> Result<E::Rows<'db>, Error>
    where
        E: Executor,
    {
        if self.params.is_empty() {
            db.query(self.sql).await
        } else {
            db.query_prepared_source(self.sql, self).await
        }
    }

    async fn one<T>(&self, db: &mut impl Executor) -> Result<T, Error>
    where
        T: FromRow,
    {
        self.fetch(db)
            .await?
            .next()
            .await?
            .map(|row| row.decode())
            .transpose()?
            .ok_or_else(|| Error::Unsupported("expected at least one row, got zero".into()))
    }

    async fn optional<T>(&self, db: &mut impl Executor) -> Result<Option<T>, Error>
    where
        T: FromRow,
    {
        let mut rows = self.fetch(db).await?;
        let first = rows.next().await?.map(|row| row.decode()).transpose()?;
        if first.is_none() {
            return Ok(None);
        }
        Ok(first)
    }

    async fn all<T>(&self, db: &mut impl Executor) -> Result<Vec<T>, Error>
    where
        T: FromRow,
    {
        let mut rows = self.fetch(db).await?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(row.decode()?);
        }
        Ok(out)
    }

    async fn execute(&self, db: &mut impl Executor) -> Result<ExecResult, Error> {
        let mut stmt = db.prepare(self.sql).await?;
        stmt.exec_source(self).await
    }
}

impl ParamSource for Query {
    fn len(&self) -> usize {
        self.params.len()
    }

    fn value_at(&self, index: usize) -> ParamRef<'_> {
        match self.params[index] {
            ArenaParam::Str { start, len } => {
                let value = std::str::from_utf8(&self.arena[start..start + len])
                    .expect("query arena stored invalid utf-8");
                ParamRef::Str(value)
            }
            ArenaParam::Bytes { start, len } => ParamRef::Bytes(&self.arena[start..start + len]),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory()).await?;

    query(
        "create table users(
            id integer primary key,
            name text not null
        )",
    )
    .execute(&mut db)
    .await?;

    query("insert into users(name) values('Ada'), ('Alan'), ('Ada')")
        .execute(&mut db)
        .await?;
    Query::insert_user_name()
        .bind_str("Grace")
        .execute(&mut db)
        .await?;

    let users = Query::select_users_by_name()
        .bind_str("Ada")
        .all::<User>(&mut db)
        .await?;

    for user in users {
        println!("id={} name={}", user.id, user.name);
    }

    let alan = Query::select_users_by_name()
        .bind_str("Alan")
        .one::<User>(&mut db)
        .await?;
    println!("one={} {}", alan.id, alan.name);

    let missing = Query::select_users_by_name()
        .bind_str("Marie")
        .optional::<User>(&mut db)
        .await?;
    println!("optional={}", missing.is_some());

    Ok(())
}
