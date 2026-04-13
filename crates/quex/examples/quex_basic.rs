use quex::{Connection, Decode, Decoder, Encode, Encoder, SqliteConnectOptions, query};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Email(String);

impl Email {
    fn parse(value: impl Into<String>) -> Result<Self, quex::Error> {
        let value = value.into();
        if value.contains('@') {
            Ok(Self(value))
        } else {
            Err(quex::Error::Unsupported("invalid email".into()))
        }
    }
}

impl Encode for Email {
    fn encode(&self, out: Encoder<'_>) {
        out.encode_str(self.0.as_str());
    }
}

impl Decode for Email {
    fn decode(value: &mut Decoder<'_>) -> Result<Self, quex::Error> {
        Self::parse(value.decode_str()?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct User {
    id: i64,
    name: String,
    email: Email,
    score: i64,
}

impl quex::FromRow for User {
    fn from_row(row: &quex::Row) -> Result<Self, quex::Error> {
        Ok(Self {
            id: row.get("id")?,
            name: row.get("name")?,
            email: row.get("email")?,
            score: row.get("score")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NameOnly {
    name: String,
}

impl quex::FromRow for NameOnly {
    fn from_row(row: &quex::Row) -> Result<Self, quex::Error> {
        Ok(Self {
            name: row.get("name")?,
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), quex::Error> {
    let mut db = Connection::connect(SqliteConnectOptions::new().in_memory()).await?;

    let schema = query("create table users(id integer primary key, name text not null, email text not null, score integer not null);")
        .execute(&mut db)
        .await?;
    println!("schema rows_affected={}", schema.rows_affected);

    let ada = Email::parse("ada@example.com")?;
    let grace = Email::parse("grace@example.com")?;
    let mut tx = db.begin().await?;
    let first = query("insert into users(name, email, score) values(?, ?, ?)")
        .bind("Ada")
        .bind(&ada)
        .bind(i64::from(37))
        .execute(&mut tx)
        .await?;
    let second = query("insert into users(name, email, score) values(?, ?, ?)")
        .bind("Grace")
        .bind(&grace)
        .bind(i64::from(42))
        .execute(&mut tx)
        .await?;
    println!(
        "insert last_ids: {:?}, {:?}",
        first.last_insert_id, second.last_insert_id
    );
    tx.commit().await?;

    let users: Vec<User> = query("select id, name, email, score from users order by id")
        .all(&mut db)
        .await?;

    for user in users {
        println!(
            "id={} name={} email={} score={}",
            user.id, user.name, user.email.0, user.score
        );
    }

    let top: NameOnly = query("select name from users where score = ?")
        .bind(i64::from(42))
        .one(&mut db)
        .await?;
    println!("top={}", top.name);

    Ok(())
}
