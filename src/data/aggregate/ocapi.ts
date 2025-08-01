import { AvaticaProtobufClient } from '../../avatica-client';
import { DateRange, formatDateForSQL } from '../types';
import { processFrame } from '../../utils';

export interface OcapiRequestRecord {
  request_date: Date | string;
  api_name: string;
  api_resource: string;
  api_version: string;
  site_id: number;
  client_id: string;
  status_code: number;
  method: string;
  num_requests: number;
  response_time: number;
  num_requests_bucket1: number;
  num_requests_bucket2: number;
  num_requests_bucket3: number;
  num_requests_bucket4: number;
  num_requests_bucket5: number;
  num_requests_bucket6: number;
  num_requests_bucket7: number;
  num_requests_bucket8: number;
  num_requests_bucket9: number;
  num_requests_bucket10: number;
  num_requests_bucket11: number;
}

/**
 * Query the ccdw_aggr_ocapi_request table with optional date filtering
 * Yields batches of records as they are fetched from the server
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results by request_date
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryOcapiRequests(
  client: AvaticaProtobufClient,
  dateRange?: DateRange,
  batchSize: number = 100
): AsyncGenerator<OcapiRequestRecord[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    let sql = 'SELECT * FROM ccdw_aggr_ocapi_request';
    
    if (dateRange) {
      // Format dates as YYYY-MM-DD for SQL
      const startDateStr = formatDateForSQL(dateRange.startDate);
      const endDateStr = formatDateForSQL(dateRange.endDate);
      
      sql += ` WHERE request_date >= '${startDateStr}' AND request_date <= '${endDateStr}'`;
    }

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      // Yield the first frame data
      const firstFrameData = processFrame<OcapiRequestRecord>(result.signature, result.firstFrame);
      if (firstFrameData.length > 0) {
        yield firstFrameData;
      }

      let done = result.firstFrame.done;
      let currentFrame: typeof result.firstFrame | undefined = result.firstFrame;

      // Fetch and yield additional frames
      while (!done && currentFrame) {
        const currentOffset = currentFrame.offset || 0;
        const currentRowCount = currentFrame.rows?.length || 0;
        
        const nextResponse = await client.fetch(
          result.statementId || 0,
          currentOffset + currentRowCount,
          batchSize
        );
        
        currentFrame = nextResponse.frame;
        if (!currentFrame) break;
        
        const nextData = processFrame<OcapiRequestRecord>(result.signature, currentFrame);
        if (nextData.length > 0) {
          yield nextData;
        }
        
        done = currentFrame.done;
      }
    }
  } finally {
    await client.closeStatement(statementId);
  }
}