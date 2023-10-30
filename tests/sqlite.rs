use sqlx::ConnectOptions;
use sqlx_odbc::{ODBCConnectOptions, ODBCConnection};

async fn test_connection() -> ODBCConnection {
    let connect_options = ODBCConnectOptions {
        connection_string: "Driver=SQLITE3;Database=:memory:;".to_string(),
    };
    connect_options.connect().await.unwrap()
}

#[tokio::test]
async fn connect() {
    let _ = test_connection().await;
}
