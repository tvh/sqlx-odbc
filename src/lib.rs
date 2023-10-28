use std::{borrow::Cow, sync::Arc};

use odbc_api::{parameter::InputParameter, DataType};
use sqlx::{Acquire, Arguments, Column, Database, Executor, Row, Statement};
use sqlx_core::{
    bytes::Bytes,
    database::{HasArguments, HasStatement},
    ext::ustr::UStr,
    *,
};

#[derive(Debug)]
pub struct ODBC;

#[derive(Debug)]
pub struct ODBCConnection<'a>(odbc_api::Connection<'a>);

pub struct ODBCRow<'a>(odbc_api::CursorRow<'a>);

// FIXME: This needs to go away
unsafe impl<'a> Sync for ODBCRow<'a> {}
unsafe impl<'a> Send for ODBCRow<'a> {}

#[derive(Default)]
pub struct ODBCArguments<'q> {
    pub(crate) values: Vec<Box<dyn InputParameter + Send + 'q>>,
}

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

impl<'a> Row for ODBCRow<'a> {
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

#[derive(Debug, Clone)]
pub struct ODBCStatement<'q> {
    pub(crate) sql: Cow<'q, str>,
    pub(crate) parameters: usize,
    pub(crate) columns: Arc<Vec<ODBCColumn>>,
    pub(crate) column_names: Arc<HashMap<UStr, usize>>,
}

impl<'q> Statement<'q> for ODBCStatement<'q> {
    type Database = ODBC;

    fn to_owned(&self) -> ODBCStatement<'static> {
        ODBCStatement::<'static> {
            sql: Cow::Owned(self.sql.clone().into_owned()),
            parameters: self.parameters,
            columns: Arc::clone(&self.columns),
            column_names: Arc::clone(&self.column_names),
        }
    }

    fn sql(&self) -> &str {
        &self.sql
    }

    fn parameters(&self) -> Option<Either<&[DataType], usize>> {
        Some(Either::Right(self.parameters))
    }

    fn columns(&self) -> &[ODBCColumn] {
        &self.columns
    }

    impl_statement_query!(ODBCArguments);
}

impl<'q> HasStatement<'q> for ODBC {
    type Database = ODBC;

    type Statement = ODBCStatement<'q>;
}

impl<'q> Arguments<'q> for ODBCArguments<'q> {
    type Database = ODBC;

    fn reserve(&mut self, additional: usize, size: usize) {
        // TODO: implement this
    }

    fn add<T>(&mut self, value: T)
    where
        T: 'q + Send + encode::Encode<'q, Self::Database> + types::Type<Self::Database>,
    {
        value.encode(&mut self.values)
    }
}

impl<'q> HasArguments<'q> for ODBC {
    type Database = ODBC;

    type Arguments = ODBCArguments<'q>;

    type ArgumentBuffer = Vec<Box<dyn InputParameter + Send + 'q>>;
}
