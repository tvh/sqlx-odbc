use sqlx::{query, Column, ConnectOptions, Executor, Row};
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
    let val: i64 = res.try_get(0).unwrap();
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
    let val: i64 = res.try_get(0).unwrap();
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
    let val: T = res.try_get(0).unwrap();
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
