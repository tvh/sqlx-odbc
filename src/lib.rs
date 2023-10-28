use std::{borrow::Cow, fmt::Display, sync::Arc};

use futures_core::future::BoxFuture;
use odbc_api::{parameter::InputParameter, DataType};
use sqlx::{
    Acquire, Arguments, Column, Database, Describe, Executor, Row, Statement, TransactionManager,
    TypeInfo, ValueRef,
};
use sqlx_core::{
    any::AnyTransactionManager,
    database::{HasArguments, HasStatement, HasValueRef},
    ext::ustr::UStr,
    *,
};

#[derive(Debug)]
pub struct ODBC;

#[derive(Debug)]
pub struct ODBCConnection<'a>(odbc_api::Connection<'a>);

pub struct ODBCRow(Arc<odbc_api::CursorRow<'static>>);

// FIXME: This needs to go away
unsafe impl Sync for ODBCRow {}
unsafe impl Send for ODBCRow {}

#[derive(Default)]
pub struct ODBCArguments<'q> {
    pub(crate) values: Vec<Box<dyn InputParameter + Send + 'q>>,
}

pub struct ODBCTransactionManager;

impl TransactionManager for ODBCTransactionManager {
    type Database = ODBC;

    fn begin(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> BoxFuture<'_, std::result::Result<(), Error>> {
        todo!()
    }

    fn commit(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> BoxFuture<'_, std::result::Result<(), Error>> {
        todo!()
    }

    fn rollback(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> BoxFuture<'_, std::result::Result<(), Error>> {
        todo!()
    }

    fn start_rollback(conn: &mut <Self::Database as Database>::Connection) {
        todo!()
    }
}

#[derive(Debug)]
pub struct ODBCColumn {
    pub(crate) ordinal: usize,
    pub(crate) name: String,
    pub(crate) type_info: DataType,
}

#[derive(Default)]
pub struct ODBCQueryResult {
    pub(crate) rows_affected: u64,
}

impl Extend<ODBCQueryResult> for ODBCQueryResult {
    fn extend<T: IntoIterator<Item = ODBCQueryResult>>(&mut self, iter: T) {
        for elem in iter {
            self.rows_affected += elem.rows_affected;
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ODBCTypeInfo(DataType);

impl Display for ODBCTypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(self.name())
    }
}

impl TypeInfo for ODBCTypeInfo {
    fn is_null(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        "FIXME_TYPE_NAME"
    }
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
        &ODBCTypeInfo(self.type_info)
    }
}

impl Database for ODBC {
    type Connection<'a> = ODBCConnection<'a>;

    type TransactionManager = ODBCTransactionManager;

    type Row = ODBCRow;

    type QueryResult = ODBCQueryResult;

    type Column = ODBCColumn;

    type TypeInfo = ODBCTypeInfo;

    type Value = ODBCValue;

    const NAME: &'static str = "odbc";

    const URL_SCHEMES: &'static [&'static str] = &[];
}

enum ODBCValue {}

enum ODBCValueData<'r> {
    Value(&'r ODBCValue),
}

pub struct ODBCValueRef<'r>(ODBCValueData<'r>);

impl<'r> ValueRef<'r> for ODBCValueRef<'r> {
    type Database = ODBC;

    fn to_owned(&self) -> <Self::Database as Database>::Value {
        match self.0 {
            ODBCValueData::Value(v) => *v.clone(),
        }
    }

    fn type_info(&self) -> Cow<'_, <Self::Database as Database>::TypeInfo> {
        match self.0 {
            ODBCValueData::Value(v) => v.data_type(),
        }
    }

    fn is_null(&self) -> bool {
        match self.0 {
            ODBCValueData::Value(v) => match *v {},
        }
    }
}

impl<'r> HasValueRef<'r> for ODBC {
    type Database = ODBC;
    type ValueRef = ODBCValueRef<'r>;
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

impl Row for ODBCRow {
    type Database = ODBC;

    fn columns(&self) -> &[<Self::Database as Database>::Column] {}

    fn try_get_raw<I>(&self, index: I) -> std::result::Result<ODBCValueRef<'_>, Error>
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

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, Error>>
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
        value.encode(&mut self.values);
    }
}

impl<'q> HasArguments<'q> for ODBC {
    type Database = ODBC;

    type Arguments = ODBCArguments<'q>;

    type ArgumentBuffer = Vec<Box<dyn InputParameter + Send + 'q>>;
}
