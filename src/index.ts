
// Main exports for @sfcc-cip-analytics-client package
export { AvaticaProtobufClient } from './avatica-client';
export { getAuthConfig, getAccessToken, getAvaticaServerUrl, AuthConfig } from './auth';
export { decodeValue, processFrame } from './utils';
export { IConnectionProperties, IExecuteResponse, IWireMessage } from './protocol';

// Re-export all data modules
export * from './data';
