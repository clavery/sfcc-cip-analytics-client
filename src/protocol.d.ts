import * as $protobuf from "protobufjs";
import Long = require("long");
/** Properties of a ConnectionProperties. */
export interface IConnectionProperties {

    /** ConnectionProperties isDirty */
    isDirty?: (boolean|null);

    /** ConnectionProperties autoCommit */
    autoCommit?: (boolean|null);

    /** ConnectionProperties hasAutoCommit */
    hasAutoCommit?: (boolean|null);

    /** ConnectionProperties readOnly */
    readOnly?: (boolean|null);

    /** ConnectionProperties hasReadOnly */
    hasReadOnly?: (boolean|null);

    /** ConnectionProperties transactionIsolation */
    transactionIsolation?: (number|null);

    /** ConnectionProperties catalog */
    catalog?: (string|null);

    /** ConnectionProperties schema */
    schema?: (string|null);
}

/** Properties of a StatementHandle. */
export interface IStatementHandle {

    /** StatementHandle connectionId */
    connectionId?: (string|null);

    /** StatementHandle id */
    id?: (number|null);

    /** StatementHandle signature */
    signature?: (ISignature|null);
}

/** Properties of a Signature. */
export interface ISignature {

    /** Signature columns */
    columns?: (IColumnMetaData[]|null);

    /** Signature sql */
    sql?: (string|null);

    /** Signature parameters */
    parameters?: (IAvaticaParameter[]|null);

    /** Signature cursorFactory */
    cursorFactory?: (ICursorFactory|null);

    /** Signature statementType */
    statementType?: (StatementType|null);
}

/** StatementType enum. */
export enum StatementType {
    SELECT = 0,
    INSERT = 1,
    UPDATE = 2,
    DELETE = 3,
    UPSERT = 4,
    MERGE = 5,
    OTHER_DML = 6,
    CREATE = 7,
    DROP = 8,
    ALTER = 9,
    OTHER_DDL = 10,
    CALL = 11
}

/** Properties of a ColumnMetaData. */
export interface IColumnMetaData {

    /** ColumnMetaData ordinal */
    ordinal?: (number|null);

    /** ColumnMetaData autoIncrement */
    autoIncrement?: (boolean|null);

    /** ColumnMetaData caseSensitive */
    caseSensitive?: (boolean|null);

    /** ColumnMetaData searchable */
    searchable?: (boolean|null);

    /** ColumnMetaData currency */
    currency?: (boolean|null);

    /** ColumnMetaData nullable */
    nullable?: (number|null);

    /** ColumnMetaData signed */
    signed?: (boolean|null);

    /** ColumnMetaData displaySize */
    displaySize?: (number|null);

    /** ColumnMetaData label */
    label?: (string|null);

    /** ColumnMetaData columnName */
    columnName?: (string|null);

    /** ColumnMetaData schemaName */
    schemaName?: (string|null);

    /** ColumnMetaData precision */
    precision?: (number|null);

    /** ColumnMetaData scale */
    scale?: (number|null);

    /** ColumnMetaData tableName */
    tableName?: (string|null);

    /** ColumnMetaData catalogName */
    catalogName?: (string|null);

    /** ColumnMetaData readOnly */
    readOnly?: (boolean|null);

    /** ColumnMetaData writable */
    writable?: (boolean|null);

    /** ColumnMetaData definitelyWritable */
    definitelyWritable?: (boolean|null);

    /** ColumnMetaData columnClassName */
    columnClassName?: (string|null);

    /** ColumnMetaData type */
    type?: (IAvaticaType|null);
}

/** Rep enum. */
export enum Rep {
    PRIMITIVE_BOOLEAN = 0,
    PRIMITIVE_BYTE = 1,
    PRIMITIVE_CHAR = 2,
    PRIMITIVE_SHORT = 3,
    PRIMITIVE_INT = 4,
    PRIMITIVE_LONG = 5,
    PRIMITIVE_FLOAT = 6,
    PRIMITIVE_DOUBLE = 7,
    BOOLEAN = 8,
    BYTE = 9,
    CHARACTER = 10,
    SHORT = 11,
    INTEGER = 12,
    LONG = 13,
    FLOAT = 14,
    DOUBLE = 15,
    BIG_INTEGER = 25,
    BIG_DECIMAL = 26,
    JAVA_SQL_TIME = 16,
    JAVA_SQL_TIMESTAMP = 17,
    JAVA_SQL_DATE = 18,
    JAVA_UTIL_DATE = 19,
    BYTE_STRING = 20,
    STRING = 21,
    NUMBER = 22,
    OBJECT = 23,
    NULL = 24,
    ARRAY = 27,
    STRUCT = 28,
    MULTISET = 29
}

/** Properties of an AvaticaType. */
export interface IAvaticaType {

    /** AvaticaType id */
    id?: (number|null);

    /** AvaticaType name */
    name?: (string|null);

    /** AvaticaType rep */
    rep?: (Rep|null);

    /** AvaticaType columns */
    columns?: (IColumnMetaData[]|null);

    /** AvaticaType component */
    component?: (IAvaticaType|null);
}

/** Properties of an AvaticaParameter. */
export interface IAvaticaParameter {

    /** AvaticaParameter signed */
    signed?: (boolean|null);

    /** AvaticaParameter precision */
    precision?: (number|null);

    /** AvaticaParameter scale */
    scale?: (number|null);

    /** AvaticaParameter parameterType */
    parameterType?: (number|null);

    /** AvaticaParameter typeName */
    typeName?: (string|null);

    /** AvaticaParameter className */
    className?: (string|null);

    /** AvaticaParameter name */
    name?: (string|null);
}

/** Properties of a CursorFactory. */
export interface ICursorFactory {

    /** CursorFactory style */
    style?: (CursorFactory.Style|null);

    /** CursorFactory className */
    className?: (string|null);

    /** CursorFactory fieldNames */
    fieldNames?: (string[]|null);
}

/** Properties of a Frame. */
export interface IFrame {

    /** Frame offset */
    offset?: (number|Long|null);

    /** Frame done */
    done?: (boolean|null);

    /** Frame rows */
    rows?: (IRow[]|null);
}

/** Properties of a Row. */
export interface IRow {

    /** Row value */
    value?: (IColumnValue[]|null);
}

/** Properties of a DatabaseProperty. */
export interface IDatabaseProperty {

    /** DatabaseProperty name */
    name?: (string|null);

    /** DatabaseProperty functions */
    functions?: (string[]|null);
}

/** Properties of a WireMessage. */
export interface IWireMessage {

    /** WireMessage name */
    name?: (string|null);

    /** WireMessage wrappedMessage */
    wrappedMessage?: (Uint8Array|null);
}

/** Properties of a ColumnValue. */
export interface IColumnValue {

    /** ColumnValue value */
    value?: (ITypedValue[]|null);

    /** ColumnValue arrayValue */
    arrayValue?: (ITypedValue[]|null);

    /** ColumnValue hasArrayValue */
    hasArrayValue?: (boolean|null);

    /** ColumnValue scalarValue */
    scalarValue?: (ITypedValue|null);
}

/** Properties of a TypedValue. */
export interface ITypedValue {

    /** TypedValue type */
    type?: (Rep|null);

    /** TypedValue boolValue */
    boolValue?: (boolean|null);

    /** TypedValue stringValue */
    stringValue?: (string|null);

    /** TypedValue numberValue */
    numberValue?: (number|Long|null);

    /** TypedValue bytesValue */
    bytesValue?: (Uint8Array|null);

    /** TypedValue doubleValue */
    doubleValue?: (number|null);

    /** TypedValue null */
    "null"?: (boolean|null);

    /** TypedValue arrayValue */
    arrayValue?: (ITypedValue[]|null);

    /** TypedValue componentType */
    componentType?: (Rep|null);

    /** TypedValue implicitlyNull */
    implicitlyNull?: (boolean|null);
}

/** Severity enum. */
export enum Severity {
    UNKNOWN_SEVERITY = 0,
    FATAL_SEVERITY = 1,
    ERROR_SEVERITY = 2,
    WARNING_SEVERITY = 3
}

/** MetaDataOperation enum. */
export enum MetaDataOperation {
    GET_ATTRIBUTES = 0,
    GET_BEST_ROW_IDENTIFIER = 1,
    GET_CATALOGS = 2,
    GET_CLIENT_INFO_PROPERTIES = 3,
    GET_COLUMN_PRIVILEGES = 4,
    GET_COLUMNS = 5,
    GET_CROSS_REFERENCE = 6,
    GET_EXPORTED_KEYS = 7,
    GET_FUNCTION_COLUMNS = 8,
    GET_FUNCTIONS = 9,
    GET_IMPORTED_KEYS = 10,
    GET_INDEX_INFO = 11,
    GET_PRIMARY_KEYS = 12,
    GET_PROCEDURE_COLUMNS = 13,
    GET_PROCEDURES = 14,
    GET_PSEUDO_COLUMNS = 15,
    GET_SCHEMAS = 16,
    GET_SCHEMAS_WITH_ARGS = 17,
    GET_SUPER_TABLES = 18,
    GET_SUPER_TYPES = 19,
    GET_TABLE_PRIVILEGES = 20,
    GET_TABLES = 21,
    GET_TABLE_TYPES = 22,
    GET_TYPE_INFO = 23,
    GET_UDTS = 24,
    GET_VERSION_COLUMNS = 25
}

/** Properties of a MetaDataOperationArgument. */
export interface IMetaDataOperationArgument {

    /** MetaDataOperationArgument stringValue */
    stringValue?: (string|null);

    /** MetaDataOperationArgument boolValue */
    boolValue?: (boolean|null);

    /** MetaDataOperationArgument intValue */
    intValue?: (number|null);

    /** MetaDataOperationArgument stringArrayValues */
    stringArrayValues?: (string[]|null);

    /** MetaDataOperationArgument intArrayValues */
    intArrayValues?: (number[]|null);

    /** MetaDataOperationArgument type */
    type?: (MetaDataOperationArgument.ArgumentType|null);
}

/** StateType enum. */
export enum StateType {
    SQL = 0,
    METADATA = 1
}

/** Properties of a QueryState. */
export interface IQueryState {

    /** QueryState type */
    type?: (StateType|null);

    /** QueryState sql */
    sql?: (string|null);

    /** QueryState op */
    op?: (MetaDataOperation|null);

    /** QueryState args */
    args?: (IMetaDataOperationArgument[]|null);

    /** QueryState hasArgs */
    hasArgs?: (boolean|null);

    /** QueryState hasSql */
    hasSql?: (boolean|null);

    /** QueryState hasOp */
    hasOp?: (boolean|null);
}

/** Properties of a ResultSetResponse. */
export interface IResultSetResponse {

    /** ResultSetResponse connectionId */
    connectionId?: (string|null);

    /** ResultSetResponse statementId */
    statementId?: (number|null);

    /** ResultSetResponse ownStatement */
    ownStatement?: (boolean|null);

    /** ResultSetResponse signature */
    signature?: (ISignature|null);

    /** ResultSetResponse firstFrame */
    firstFrame?: (IFrame|null);

    /** ResultSetResponse updateCount */
    updateCount?: (number|Long|null);

    /** ResultSetResponse metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of an ExecuteResponse. */
export interface IExecuteResponse {

    /** ExecuteResponse results */
    results?: (IResultSetResponse[]|null);

    /** ExecuteResponse missingStatement */
    missingStatement?: (boolean|null);

    /** ExecuteResponse metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of a PrepareResponse. */
export interface IPrepareResponse {

    /** PrepareResponse statement */
    statement?: (IStatementHandle|null);

    /** PrepareResponse metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of a FetchResponse. */
export interface IFetchResponse {

    /** FetchResponse frame */
    frame?: (IFrame|null);

    /** FetchResponse missingStatement */
    missingStatement?: (boolean|null);

    /** FetchResponse missingResults */
    missingResults?: (boolean|null);

    /** FetchResponse metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of a CreateStatementResponse. */
export interface ICreateStatementResponse {

    /** CreateStatementResponse connectionId */
    connectionId?: (string|null);

    /** CreateStatementResponse statementId */
    statementId?: (number|null);

    /** CreateStatementResponse metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of a CloseStatementResponse. */
export interface ICloseStatementResponse {

    /** CloseStatementResponse metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of an OpenConnectionResponse. */
export interface IOpenConnectionResponse {

    /** OpenConnectionResponse metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of a CloseConnectionResponse. */
export interface ICloseConnectionResponse {

    /** CloseConnectionResponse metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of a ConnectionSyncResponse. */
export interface IConnectionSyncResponse {

    /** ConnectionSyncResponse connProps */
    connProps?: (IConnectionProperties|null);

    /** ConnectionSyncResponse metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of a DatabasePropertyElement. */
export interface IDatabasePropertyElement {

    /** DatabasePropertyElement key */
    key?: (IDatabaseProperty|null);

    /** DatabasePropertyElement value */
    value?: (ITypedValue|null);

    /** DatabasePropertyElement metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of a DatabasePropertyResponse. */
export interface IDatabasePropertyResponse {

    /** DatabasePropertyResponse props */
    props?: (IDatabasePropertyElement[]|null);

    /** DatabasePropertyResponse metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of an ErrorResponse. */
export interface IErrorResponse {

    /** ErrorResponse exceptions */
    exceptions?: (string[]|null);

    /** ErrorResponse hasExceptions */
    hasExceptions?: (boolean|null);

    /** ErrorResponse errorMessage */
    errorMessage?: (string|null);

    /** ErrorResponse severity */
    severity?: (Severity|null);

    /** ErrorResponse errorCode */
    errorCode?: (number|null);

    /** ErrorResponse sqlState */
    sqlState?: (string|null);

    /** ErrorResponse metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of a SyncResultsResponse. */
export interface ISyncResultsResponse {

    /** SyncResultsResponse missingStatement */
    missingStatement?: (boolean|null);

    /** SyncResultsResponse moreResults */
    moreResults?: (boolean|null);

    /** SyncResultsResponse metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of a RpcMetadata. */
export interface IRpcMetadata {

    /** RpcMetadata serverAddress */
    serverAddress?: (string|null);
}

/** Properties of a CommitResponse. */
export interface ICommitResponse {
}

/** Properties of a RollbackResponse. */
export interface IRollbackResponse {
}

/** Properties of an ExecuteBatchResponse. */
export interface IExecuteBatchResponse {

    /** ExecuteBatchResponse connectionId */
    connectionId?: (string|null);

    /** ExecuteBatchResponse statementId */
    statementId?: (number|null);

    /** ExecuteBatchResponse updateCounts */
    updateCounts?: ((number|Long)[]|null);

    /** ExecuteBatchResponse missingStatement */
    missingStatement?: (boolean|null);

    /** ExecuteBatchResponse metadata */
    metadata?: (IRpcMetadata|null);
}

/** Properties of a CatalogsRequest. */
export interface ICatalogsRequest {

    /** CatalogsRequest connectionId */
    connectionId?: (string|null);
}

/** Properties of a DatabasePropertyRequest. */
export interface IDatabasePropertyRequest {

    /** DatabasePropertyRequest connectionId */
    connectionId?: (string|null);
}

/** Properties of a SchemasRequest. */
export interface ISchemasRequest {

    /** SchemasRequest catalog */
    catalog?: (string|null);

    /** SchemasRequest schemaPattern */
    schemaPattern?: (string|null);

    /** SchemasRequest connectionId */
    connectionId?: (string|null);

    /** SchemasRequest hasCatalog */
    hasCatalog?: (boolean|null);

    /** SchemasRequest hasSchemaPattern */
    hasSchemaPattern?: (boolean|null);
}

/** Properties of a TablesRequest. */
export interface ITablesRequest {

    /** TablesRequest catalog */
    catalog?: (string|null);

    /** TablesRequest schemaPattern */
    schemaPattern?: (string|null);

    /** TablesRequest tableNamePattern */
    tableNamePattern?: (string|null);

    /** TablesRequest typeList */
    typeList?: (string[]|null);

    /** TablesRequest hasTypeList */
    hasTypeList?: (boolean|null);

    /** TablesRequest connectionId */
    connectionId?: (string|null);

    /** TablesRequest hasCatalog */
    hasCatalog?: (boolean|null);

    /** TablesRequest hasSchemaPattern */
    hasSchemaPattern?: (boolean|null);

    /** TablesRequest hasTableNamePattern */
    hasTableNamePattern?: (boolean|null);
}

/** Properties of a TableTypesRequest. */
export interface ITableTypesRequest {

    /** TableTypesRequest connectionId */
    connectionId?: (string|null);
}

/** Properties of a ColumnsRequest. */
export interface IColumnsRequest {

    /** ColumnsRequest catalog */
    catalog?: (string|null);

    /** ColumnsRequest schemaPattern */
    schemaPattern?: (string|null);

    /** ColumnsRequest tableNamePattern */
    tableNamePattern?: (string|null);

    /** ColumnsRequest columnNamePattern */
    columnNamePattern?: (string|null);

    /** ColumnsRequest connectionId */
    connectionId?: (string|null);

    /** ColumnsRequest hasCatalog */
    hasCatalog?: (boolean|null);

    /** ColumnsRequest hasSchemaPattern */
    hasSchemaPattern?: (boolean|null);

    /** ColumnsRequest hasTableNamePattern */
    hasTableNamePattern?: (boolean|null);

    /** ColumnsRequest hasColumnNamePattern */
    hasColumnNamePattern?: (boolean|null);
}

/** Properties of a TypeInfoRequest. */
export interface ITypeInfoRequest {

    /** TypeInfoRequest connectionId */
    connectionId?: (string|null);
}

/** Properties of a PrepareAndExecuteRequest. */
export interface IPrepareAndExecuteRequest {

    /** PrepareAndExecuteRequest connectionId */
    connectionId?: (string|null);

    /** PrepareAndExecuteRequest sql */
    sql?: (string|null);

    /** PrepareAndExecuteRequest maxRowCount */
    maxRowCount?: (number|Long|null);

    /** PrepareAndExecuteRequest statementId */
    statementId?: (number|null);

    /** PrepareAndExecuteRequest maxRowsTotal */
    maxRowsTotal?: (number|Long|null);

    /** PrepareAndExecuteRequest firstFrameMaxSize */
    firstFrameMaxSize?: (number|null);
}

/** Properties of a PrepareRequest. */
export interface IPrepareRequest {

    /** PrepareRequest connectionId */
    connectionId?: (string|null);

    /** PrepareRequest sql */
    sql?: (string|null);

    /** PrepareRequest maxRowCount */
    maxRowCount?: (number|Long|null);

    /** PrepareRequest maxRowsTotal */
    maxRowsTotal?: (number|Long|null);
}

/** Properties of a FetchRequest. */
export interface IFetchRequest {

    /** FetchRequest connectionId */
    connectionId?: (string|null);

    /** FetchRequest statementId */
    statementId?: (number|null);

    /** FetchRequest offset */
    offset?: (number|Long|null);

    /** FetchRequest fetchMaxRowCount */
    fetchMaxRowCount?: (number|null);

    /** FetchRequest frameMaxSize */
    frameMaxSize?: (number|null);
}

/** Properties of a CreateStatementRequest. */
export interface ICreateStatementRequest {

    /** CreateStatementRequest connectionId */
    connectionId?: (string|null);
}

/** Properties of a CloseStatementRequest. */
export interface ICloseStatementRequest {

    /** CloseStatementRequest connectionId */
    connectionId?: (string|null);

    /** CloseStatementRequest statementId */
    statementId?: (number|null);
}

/** Properties of an OpenConnectionRequest. */
export interface IOpenConnectionRequest {

    /** OpenConnectionRequest connectionId */
    connectionId?: (string|null);

    /** OpenConnectionRequest info */
    info?: ({ [k: string]: string }|null);
}

/** Properties of a CloseConnectionRequest. */
export interface ICloseConnectionRequest {

    /** CloseConnectionRequest connectionId */
    connectionId?: (string|null);
}

/** Properties of a ConnectionSyncRequest. */
export interface IConnectionSyncRequest {

    /** ConnectionSyncRequest connectionId */
    connectionId?: (string|null);

    /** ConnectionSyncRequest connProps */
    connProps?: (IConnectionProperties|null);
}

/** Properties of an ExecuteRequest. */
export interface IExecuteRequest {

    /** ExecuteRequest statementHandle */
    statementHandle?: (IStatementHandle|null);

    /** ExecuteRequest parameterValues */
    parameterValues?: (ITypedValue[]|null);

    /** ExecuteRequest deprecatedFirstFrameMaxSize */
    deprecatedFirstFrameMaxSize?: (number|Long|null);

    /** ExecuteRequest hasParameterValues */
    hasParameterValues?: (boolean|null);

    /** ExecuteRequest firstFrameMaxSize */
    firstFrameMaxSize?: (number|null);
}

/** Properties of a SyncResultsRequest. */
export interface ISyncResultsRequest {

    /** SyncResultsRequest connectionId */
    connectionId?: (string|null);

    /** SyncResultsRequest statementId */
    statementId?: (number|null);

    /** SyncResultsRequest state */
    state?: (IQueryState|null);

    /** SyncResultsRequest offset */
    offset?: (number|Long|null);
}

/** Properties of a CommitRequest. */
export interface ICommitRequest {

    /** CommitRequest connectionId */
    connectionId?: (string|null);
}

/** Properties of a RollbackRequest. */
export interface IRollbackRequest {

    /** RollbackRequest connectionId */
    connectionId?: (string|null);
}

/** Properties of a PrepareAndExecuteBatchRequest. */
export interface IPrepareAndExecuteBatchRequest {

    /** PrepareAndExecuteBatchRequest connectionId */
    connectionId?: (string|null);

    /** PrepareAndExecuteBatchRequest statementId */
    statementId?: (number|null);

    /** PrepareAndExecuteBatchRequest sqlCommands */
    sqlCommands?: (string[]|null);
}

/** Properties of an UpdateBatch. */
export interface IUpdateBatch {

    /** UpdateBatch parameterValues */
    parameterValues?: (ITypedValue[]|null);
}

/** Properties of an ExecuteBatchRequest. */
export interface IExecuteBatchRequest {

    /** ExecuteBatchRequest connectionId */
    connectionId?: (string|null);

    /** ExecuteBatchRequest statementId */
    statementId?: (number|null);

    /** ExecuteBatchRequest updates */
    updates?: (IUpdateBatch[]|null);
}
