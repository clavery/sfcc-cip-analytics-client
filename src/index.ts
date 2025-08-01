
// Main exports for @sfcc-cip-analytics-client package
export { AvaticaProtobufClient } from './avatica-client';
export { getAuthConfig, getAccessToken, getAvaticaServerUrl, AuthConfig } from './auth';
export { decodeValue, processFrame } from './utils';
export { IConnectionProperties, IWireMessage } from './protocol';
export { 
  NormalizedFrame, 
  NormalizedResultSetResponse, 
  NormalizedExecuteResponse, 
  NormalizedFetchResponse 
} from './normalized-types';

// Re-export all data modules
export * from './data';
