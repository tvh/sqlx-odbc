use std::{
    borrow::Cow,
    fmt::{Debug, Display},
    mem::transmute,
    str::FromStr,
    sync::Arc,
};

use futures_core::future::BoxFuture;
use log::LevelFilter;
use odbc_api::{
    handles::StatementImpl, parameter::InputParameter, ColumnDescription, ConnectionOptions,
    Cursor, CursorImpl, CursorRow, DataType, Environment, ResultSetMetadata,
};
use once_cell::sync::Lazy;
use sqlx::{
    Arguments, Column, ConnectOptions, Connection, Database, Decode, Describe, Executor, Row,
    Statement, Transaction, TransactionManager, Type, TypeInfo, Value, ValueRef,
};
use sqlx_core::{
    database::{HasArguments, HasStatement, HasValueRef},
    error::BoxDynError,
    ext::ustr::UStr,
    *,
};

static ENV: Lazy<Environment> = Lazy::new(|| Environment::new().unwrap());

#[derive(Debug)]
pub struct ODBC;

pub struct ODBCConnection(odbc_api::Connection<'static>);

impl Debug for ODBCConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ODBCConnection").finish()
    }
}

// FIXME: This needs to go away
unsafe impl Sync for ODBCConnection {}
unsafe impl Send for ODBCConnection {}

impl Connection for ODBCConnection {
    type Database = ODBC;

    type Options = ODBCConnectOptions;

    fn close(self) -> BoxFuture<'static, std::result::Result<(), Error>> {
        // TODO: Implement this for better error handling. For now it works because of 'Drop'
        Box::pin(async { Ok(()) })
    }

    fn ping(&mut self) -> BoxFuture<'_, std::result::Result<(), Error>> {
        // TODO
        Box::pin(async { Ok(()) })
    }

    fn begin(
        &mut self,
    ) -> BoxFuture<'_, std::result::Result<transaction::Transaction<'_, Self::Database>, Error>>
    where
        Self: Sized,
    {
        Transaction::begin(self)
    }

    fn shrink_buffers(&mut self) {
        // TODO
    }

    fn close_hard(self) -> BoxFuture<'static, Result<(), Error>> {
        // TODO: Implement this. For now it works because of 'Drop'
        Box::pin(async { Ok(()) })
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }

    fn should_flush(&self) -> bool {
        // FIXME
        false
    }
}

#[derive(Clone, Debug)]
pub struct ODBCConnectOptions {
    pub connection_string: String,
}

impl FromStr for ODBCConnectOptions {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Error> {
        Err(Error::Configuration("TODO".into()))
    }
}

impl ConnectOptions for ODBCConnectOptions {
    type Connection = ODBCConnection;

    fn from_url(url: &Url) -> std::result::Result<Self, Error> {
        todo!()
    }

    fn connect(&self) -> BoxFuture<'_, std::result::Result<Self::Connection, Error>>
    where
        Self::Connection: Sized,
    {
        Box::pin(async {
            match ENV.connect_with_connection_string(
                &self.connection_string,
                ConnectionOptions {
                    // FIXME: Make this configurable
                    login_timeout_sec: Some(5),
                },
            ) {
                Ok(conn) => Ok(ODBCConnection(conn)),
                Err(e) => Err(Error::AnyDriverError(Box::new(e))),
            }
        })
    }

    fn log_statements(self, level: LevelFilter) -> Self {
        // TODO
        self
    }

    fn log_slow_statements(self, level: LevelFilter, duration: std::time::Duration) -> Self {
        // TODO
        self
    }
}

pub struct ODBCRow {
    colums: Vec<ODBCColumn>,
    row: std::cell::RefCell<odbc_api::CursorRow<'static>>,
    // NOTE: Here so that they are not dropped
    _cursor: Arc<CursorImpl<StatementImpl<'static>>>,
}

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

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ODBCColumn {
    pub(crate) ordinal: usize,
    pub(crate) name: String,
    pub(crate) type_info: ODBCTypeInfo,
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

#[derive(Clone, PartialEq, Eq, Debug)]
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
        &self.type_info
    }
}

impl Database for ODBC {
    type Connection = ODBCConnection;

    type TransactionManager = ODBCTransactionManager;

    type Row = ODBCRow;

    type QueryResult = ODBCQueryResult;

    type Column = ODBCColumn;

    type TypeInfo = ODBCTypeInfo;

    type Value = ODBCValue;

    const NAME: &'static str = "odbc";

    const URL_SCHEMES: &'static [&'static str] = &[];
}

#[derive(Copy, Clone)]
pub enum ODBCValue {
    I64(i64),
}

impl Value for ODBCValue {
    type Database = ODBC;

    fn as_ref(&self) -> <Self::Database as HasValueRef<'_>>::ValueRef {
        ODBCValueRef(ODBCValueData::ValueRef(self))
    }

    fn type_info(&self) -> Cow<'_, <Self::Database as Database>::TypeInfo> {
        match *self {
            Self::I64(_) => Cow::Owned(ODBCTypeInfo(DataType::BigInt)),
        }
    }

    fn is_null(&self) -> bool {
        match *self {
            Self::I64(_) => false,
        }
    }
}

enum ODBCValueData<'r> {
    ValueRef(&'r ODBCValue),
    Value(ODBCValue),
}

pub struct ODBCValueRef<'r>(ODBCValueData<'r>);

impl<'r> ValueRef<'r> for ODBCValueRef<'r> {
    type Database = ODBC;

    fn to_owned(&self) -> <Self::Database as Database>::Value {
        match self.0 {
            ODBCValueData::Value(v) => v.clone(),
            ODBCValueData::ValueRef(v) => v.clone(),
        }
    }

    fn type_info(&self) -> Cow<'_, <Self::Database as Database>::TypeInfo> {
        match self.0 {
            ODBCValueData::Value(v) => {
                let res = v.type_info().into_owned();
                Cow::Owned(res)
            }
            ODBCValueData::ValueRef(v) => v.type_info(),
        }
    }

    fn is_null(&self) -> bool {
        match self.0 {
            ODBCValueData::ValueRef(v) => match *v {
                ODBCValue::I64(_) => false,
            },
            ODBCValueData::Value(v) => match v {
                ODBCValue::I64(_) => false,
            },
        }
    }
}

impl<'r> HasValueRef<'r> for ODBC {
    type Database = ODBC;
    type ValueRef = ODBCValueRef<'r>;
}

impl Row for ODBCRow {
    type Database = ODBC;

    fn columns(&self) -> &[<Self::Database as Database>::Column] {
        &self.colums
    }

    fn try_get_raw<I>(&self, index: I) -> std::result::Result<ODBCValueRef<'_>, Error>
    where
        I: column::ColumnIndex<Self>,
    {
        let index = index.index(self)?;
        // TODO: Type dependant dispatch
        let mut res: i64 = 0;
        match self
            .row
            .borrow_mut()
            .get_data((index + 1).try_into().unwrap(), &mut res)
        {
            Ok(()) => Ok(ODBCValueRef(ODBCValueData::Value(ODBCValue::I64(res)))),
            Err(_) => todo!(),
        }
    }
}

impl ODBCConnection {
    fn fetch_optional_internal<'q, E: 'q>(
        &self,
        query: E,
    ) -> std::result::Result<Option<<ODBC as Database>::Row>, odbc_api::Error>
    where
        E: executor::Execute<'q, ODBC>,
    {
        let sql = query.sql().to_string();
        // FIXME: async
        let conn: &odbc_api::Connection<'static> = &self.0;
        let mut statement = conn.prepare(&sql)?;
        let num_cols = statement.num_result_cols()?;
        let mut colums: Vec<ODBCColumn> = Vec::with_capacity(num_cols.try_into().unwrap());
        for i in 0..num_cols {
            let mut col_desc: ColumnDescription = Default::default();
            statement.describe_col((i + 1).try_into().unwrap(), &mut col_desc)?;
            colums.push(ODBCColumn {
                ordinal: i.try_into().unwrap(),
                name: col_desc.name_to_string().unwrap(),
                type_info: ODBCTypeInfo(col_desc.data_type),
            })
        }
        match conn.execute(
            &sql,
            // FIXME: Parameters
            (),
        ) {
            Ok(Some(mut cursor)) => {
                let mut cursor: CursorImpl<StatementImpl<'static>> = unsafe { transmute(cursor) };
                match cursor.next_row() {
                    Ok(None) => Ok(None),
                    Ok(Some(row)) => {
                        let row: CursorRow<'static> = unsafe {
                            std::mem::transmute::<CursorRow<'_>, CursorRow<'static>>(row)
                        };
                        Ok(Some(ODBCRow {
                            row: std::cell::RefCell::new(row),
                            colums,
                            _cursor: Arc::new(cursor),
                        }))
                    }
                    Err(e) => todo!(),
                }
            }
            Ok(None) => Ok(None),
            Err(e) => todo!(),
        }
    }
}

impl<'c> Executor<'c> for &'c mut ODBCConnection {
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
        let sql = query.sql().to_string();
        Box::pin(async {
            match self.fetch_optional_internal(query) {
                Ok(res) => Ok(res),
                Err(_) => todo!(),
            }
        })
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

    fn parameters(&self) -> Option<Either<&[ODBCTypeInfo], usize>> {
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

impl_into_arguments_for_arguments!(ODBCArguments<'q>);
impl_acquire!(ODBC, ODBCConnection);
impl_column_index_for_row!(ODBCRow);
impl_column_index_for_statement!(ODBCStatement);
impl_encode_for_option!(ODBC);

impl Type<ODBC> for i64 {
    fn type_info() -> ODBCTypeInfo {
        ODBCTypeInfo(DataType::BigInt)
    }

    fn compatible(ty: &ODBCTypeInfo) -> bool {
        matches!(ty.0, DataType::BigInt)
    }
}

impl<'r> Decode<'r, ODBC> for i64 {
    fn decode(value: ODBCValueRef<'r>) -> Result<Self, BoxDynError> {
        match value.0 {
            ODBCValueData::Value(ODBCValue::I64(i)) => Ok(i),
            ODBCValueData::ValueRef(ODBCValue::I64(i)) => Ok(i.to_owned()),
        }
    }
}
