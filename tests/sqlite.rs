use futures_util::StreamExt;
use sqlx::{query, Column, ConnectOptions, Connection, Executor, Row};
use sqlx_odbc::{ODBCConnectOptions, ODBCConnection};

async fn test_connection() -> ODBCConnection {
    let connect_options = ODBCConnectOptions {
        // FIXME: This only works on macos right now
        connection_string: "Driver=/opt/homebrew/lib/libsqlite3odbc.dylib;Database=:memory:;"
            .to_string(),
    };
    connect_options.connect().await.unwrap()
}

#[tokio::test]
async fn connect() {
    let _ = test_connection().await;
}

#[tokio::test]
async fn simple_select() {
    let mut conn = test_connection().await;
    let res = query("select 42 as test_column")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let columns = res.columns();
    assert_eq!(
        Vec::from(["test_column"]),
        columns
            .into_iter()
            .map(|c| { c.name() })
            .collect::<Vec<_>>()
    );
    let val: i64 = res.get(0);
    assert_eq!(42, val)
}

#[tokio::test]
async fn select_with_arg() {
    let mut conn = test_connection().await;
    let res = query("select ?+1 as test_column")
        .bind(42)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let columns = res.columns();
    assert_eq!(
        Vec::from(["test_column"]),
        columns
            .into_iter()
            .map(|c| { c.name() })
            .collect::<Vec<_>>()
    );
    let val: i64 = res.get(0);
    assert_eq!(43, val)
}

async fn test_query_roundtrip<T>(v: &T)
where
    T: std::fmt::Debug
        + for<'r> sqlx::Type<sqlx_odbc::ODBC>
        + for<'r> sqlx::Decode<'r, sqlx_odbc::ODBC>
        + for<'r> sqlx::Encode<'r, sqlx_odbc::ODBC>
        + Send
        + Clone
        + std::cmp::PartialEq,
{
    let mut conn = test_connection().await;
    let res = query("select ? as test_column")
        .bind(v.clone())
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let columns = res.columns();
    assert_eq!(
        Vec::from(["test_column"]),
        columns
            .into_iter()
            .map(|c| { c.name() })
            .collect::<Vec<_>>()
    );
    let val: T = res.get(0);
    assert_eq!(v.clone(), val.clone())
}

async fn test_for_type<T>(v: T)
where
    T: std::fmt::Debug
        + for<'r> sqlx::Type<sqlx_odbc::ODBC>
        + for<'r> sqlx::Decode<'r, sqlx_odbc::ODBC>
        + for<'r> sqlx::Encode<'r, sqlx_odbc::ODBC>
        + Send
        + Clone
        + std::cmp::PartialEq,
{
    test_query_roundtrip(&v).await;
    test_query_roundtrip(&None::<T>).await;
}

#[tokio::test]
async fn roundtrip_i32() {
    test_for_type(42 as i32).await
}

#[tokio::test]
async fn roundtrip_i64() {
    test_for_type(42 as i64).await
}

#[tokio::test]
async fn roundtrip_f64() {
    test_for_type(42.12 as f64).await
}

#[tokio::test]
async fn roundtrip_str() {
    test_for_type("YAY!".to_string()).await
}

#[tokio::test]
async fn roundtrip_binary() {
    test_for_type(Vec::from([
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    ]))
    .await
}

#[tokio::test]
async fn describe() {
    let mut conn = test_connection().await;
    let res = conn
        .describe("select 1, ?+1 as num, 'Hello' || ?")
        .await
        .unwrap();
    assert_eq!(
        res.columns.iter().map(|c| c.name()).collect::<Vec<&str>>(),
        vec!("1", "num", "'Hello' || ?")
    )
}

#[tokio::test]
async fn fetch_many() {
    let mut conn = test_connection().await;
    let res = query(
        "with cte(col1, col2) as
            (values (1,'one'), (2, 'two'))
            select * from cte",
    )
    .bind(42)
    .fetch_many(&mut conn);
    let (res1, tail) = res.into_future().await;
    let res1 = res1.unwrap().unwrap().right().unwrap();
    let columns = res1.columns();
    assert_eq!(
        Vec::from(["col1", "col2"]),
        columns
            .into_iter()
            .map(|c| { c.name() })
            .collect::<Vec<_>>()
    );
    let v1: i64 = res1.get(0);
    let v2: String = res1.get(1);
    assert_eq!((1, "one".to_owned()), (v1, v2));

    let (res2, tail) = tail.into_future().await;
    let res2 = res2.unwrap().unwrap().right().unwrap();
    let columns = res1.columns();
    assert_eq!(
        Vec::from(["col1", "col2"]),
        columns
            .into_iter()
            .map(|c| { c.name() })
            .collect::<Vec<_>>()
    );
    let v1: i64 = res2.get(0);
    let v2: String = res2.get(1);
    assert_eq!((2, "two".to_owned()), (v1, v2));

    let (res_empty, _tail) = tail.into_future().await;
    match res_empty {
        None => {}
        Some(_) => panic!("Found result where there should be none"),
    }
}

#[tokio::test]
async fn transaction_rollback() {
    let mut conn = test_connection().await;
    conn.execute("CREATE TABLE test(x INTEGER NOT NULL)");
    let mut transaction = conn.begin().await.unwrap();
    transaction.execute("INSERT INTO test(x) VALUES (42)");
    let res = transaction.fetch_one("SELECT * FROM test").await.unwrap();
    assert_eq!(res.get::<i64, usize>(0), 42);
    transaction.rollback().await.unwrap();
    let res = conn.fetch_optional("SELECT * from test").await.unwrap();
    match res {
        None => {}
        Some(_) => panic!("Found result where there should be none"),
    }
}

#[tokio::test]
async fn transaction_commit() {
    let mut conn = test_connection().await;
    conn.execute("CREATE TABLE test(x INTEGER NOT NULL)");
    let mut transaction = conn.begin().await.unwrap();
    transaction.execute("INSERT INTO test(x) VALUES (42)");
    transaction.commit().await.unwrap();
    let res = conn.fetch_one("SELECT * from test").await.unwrap();
    assert_eq!(res.get::<i64, usize>(0), 42);
}
