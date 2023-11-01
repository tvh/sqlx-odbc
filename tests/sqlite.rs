use sqlx::{query, ConnectOptions, Row};
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
    let res = query("select 1").fetch_one(&mut conn).await.unwrap();
    let columns = res.columns();
    let valRef: i64 = res.try_get(0).unwrap();
}
