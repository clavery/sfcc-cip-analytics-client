// Main exports for @sfcc-cip-analytics-client package
export { AvaticaProtobufClient } from './avatica-client';
export { getAuthConfig, getAccessToken, getAvaticaServerUrl, AuthConfig } from './auth';
export { DateRange, QueryResult, formatDateForSQL, decodeValue, processFrame } from './data/types';
export { queryOcapiRequests, OcapiRequestRecord } from './data/aggregate/ocapi';
export { IConnectionProperties, IExecuteResponse, IWireMessage } from './protocol';