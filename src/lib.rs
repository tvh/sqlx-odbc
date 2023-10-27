use odbc_api::{DataType, Nullability};
use sqlx::{Acquire, Column, Database, Executor, Row};
use sqlx_core::{bytes::Bytes, *};

#[derive(Debug)]
pub struct ODBC;

#[derive(Debug)]
pub struct ODBCConnection<'a>(odbc_api::Connection<'a>);

pub struct ODBCRow<'a>(odbc_api::CursorRow<'a>);

pub struct ODBCArguments;

pub struct ODBCTransactionManager;

#[derive(Debug)]
pub struct ODBCColumn {
    pub(crate) ordinal: usize,
    pub(crate) name: String,
    pub(crate) type_info: DataType,
}

pub struct ODBCQueryResult {
    pub(crate) rows_affected: u64,
}

impl Column for ODBCColumn {
    type Database = ODBC;

    fn ordinal(&self) -> usize {
        self.ordinal
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn type_info(&self) -> &<Self::Database as Database>::TypeInfo {
        &self.type_info
    }
}

#[derive(Clone)]
pub struct ODBCValue {
    pub(crate) value: Option<Bytes>,
    pub(crate) type_info: DataType,
}

impl Database for ODBC {
    type Connection<'a> = ODBCConnection<'a>;

    type TransactionManager = ODBCTransactionManager;

    type Row<'a> = ODBCRow<'a>;

    type QueryResult = ODBCQueryResult;

    type Column = ODBCColumn;

    type TypeInfo = DataType;

    type Value = ODBCValue;

    const NAME: &'static str = "odbc";

    const URL_SCHEMES: &'static [&'static str] = &[];
}

impl<'c> Acquire<'c> for &'c mut ODBCConnection<'c> {
    type Database = ODBC;

    type Connection = &'c mut <ODBC as Database>::Connection;

    fn acquire(
        self,
    ) -> futures_core::future::BoxFuture<'c, std::result::Result<Self::Connection, Error>> {
    }

    fn begin(
        self,
    ) -> futures_core::future::BoxFuture<
        'c,
        std::result::Result<transaction::Transaction<'c, Self::Database>, Error>,
    > {
    }
}

impl Row for ODBCRow<'a> {
    type Database = ODBC;

    fn columns(&self) -> &[<Self::Database as Database>::Column] {}

    fn try_get_raw<I>(
        &self,
        index: I,
    ) -> std::result::Result<<Self::Database as database::HasValueRef<'_>>::ValueRef, Error>
    where
        I: column::ColumnIndex<Self>,
    {
    }
}

impl<'c> Executor<'c> for &'c mut ODBCConnection<'c> {
    type Database = ODBC;

    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> futures_core::stream::BoxStream<
        'e,
        std::result::Result<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
            Error,
        >,
    >
    where
        'c: 'e,
        E: executor::Execute<'q, Self::Database>,
    {
        todo!()
    }

    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> futures_core::future::BoxFuture<
        'e,
        std::result::Result<Option<<Self::Database as Database>::Row>, Error>,
    >
    where
        'c: 'e,
        E: executor::Execute<'q, Self::Database>,
    {
        todo!()
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> futures_core::future::BoxFuture<
        'e,
        std::result::Result<<Self::Database as database::HasStatement<'q>>::Statement, Error>,
    >
    where
        'c: 'e,
    {
        todo!()
    }
}
