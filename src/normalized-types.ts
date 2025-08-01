/**
 * Normalized Avatica response types with guaranteed JavaScript number types.
 * These override the generated protobuf types to provide cleaner APIs.
 */

import { ISignature } from './protocol';

// Normalized frame interface with guaranteed number offset
export interface NormalizedFrame {
  done: boolean;
  offset: number;  // Always a number, never a protobuf Long
  rows: any[];
}

// Normalized result set response with normalized frame
export interface NormalizedResultSetResponse {
  connectionId?: string;
  statementId?: number;
  ownStatement?: boolean;
  signature?: ISignature;
  firstFrame?: NormalizedFrame;
  updateCount?: number;
  metadata?: any;
}

// Normalized execute response
export interface NormalizedExecuteResponse {
  results?: NormalizedResultSetResponse[];
  missingStatement?: boolean;
  metadata?: any;
}

// Normalized fetch response
export interface NormalizedFetchResponse {
  frame?: NormalizedFrame;
  missingStatement?: boolean;
  missingResults?: boolean;
  metadata?: any;
}
