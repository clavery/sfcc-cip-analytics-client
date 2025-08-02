import { CIPClient } from '../../cip-client';
import { DateRange, formatDateForSQL, cleanSQL } from '../types';
import { processFrame } from '../../utils';

export interface OcapiPerformance {
  request_date: Date | string;
  api_name: string;
  api_resource: string;
  total_requests: number;
  total_response_time: number;
  avg_response_time: number;
  client_id: string;
}

export interface ScapiCacheMetrics {
  request_date: Date | string;
  api_family: string;
  api_name: string;
  total_requests: number;
  cache_hits: number;
  cache_hit_rate: number;
  avg_response_time: number;
}

export interface ControllerPerformance {
  request_date: Date | string;
  controller_name: string;
  total_requests: number;
  avg_response_time: number;
  requests_under_100ms: number;
  requests_100_500ms: number;
  requests_over_500ms: number;
}

/**
 * Query OCAPI performance metrics to monitor API health
 * Business Question: Is our system performing well enough to support customer experience?
 * Primary users: Operations and Engineering teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryOcapiPerformance(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<OcapiPerformance[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      SELECT
        o.request_date,
        o.api_name,
        o.api_resource,
        SUM(o.num_requests) as total_requests,
        SUM(o.response_time) as total_response_time,
        CASE WHEN SUM(o.num_requests) > 0
             THEN SUM(o.response_time) / SUM(o.num_requests)
             ELSE 0 END as avg_response_time,
        o.client_id
      FROM ccdw_aggr_ocapi_request o
      JOIN ccdw_dim_site s ON s.site_id = o.site_id
      WHERE o.request_date >= '${startDateStr}' AND o.request_date <= '${endDateStr}'
        AND s.nsite_id = '${siteId}'
      GROUP BY o.request_date, o.api_name, o.api_resource, o.client_id
      ORDER BY total_requests DESC
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<OcapiPerformance>(result.signature, result.firstFrame);
      if (firstFrameData.length > 0) {
        yield firstFrameData;
      }

      let done = result.firstFrame.done;
      let currentFrame: typeof result.firstFrame | undefined = result.firstFrame;

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
        
        const nextData = processFrame<OcapiPerformance>(result.signature, currentFrame);
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

/**
 * Query SCAPI cache performance to optimize caching strategy
 * Business Question: How effective is our API caching?
 * Primary users: Engineering and Performance teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryScapiCacheMetrics(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<ScapiCacheMetrics[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      SELECT
        sc.request_date,
        sc.api_family,
        sc.api_name,
        SUM(sc.num_requests) as total_requests,
        SUM(sc.num_cached_requests) as cache_hits,
        CASE WHEN SUM(sc.num_requests) > 0
             THEN (CAST(SUM(sc.num_cached_requests) AS FLOAT) / SUM(sc.num_requests)) * 100
             ELSE 0
        END as cache_hit_rate,
        CASE WHEN SUM(sc.num_requests) > 0
             THEN SUM(sc.response_time) / SUM(sc.num_requests)
             ELSE 0
        END as avg_response_time
      FROM ccdw_aggr_scapi_request sc
      JOIN ccdw_dim_site s ON s.site_id = sc.site_id
      WHERE sc.request_date >= '${startDateStr}' AND sc.request_date <= '${endDateStr}'
        AND s.nsite_id = '${siteId}'
      GROUP BY sc.request_date, sc.api_family, sc.api_name
      ORDER BY total_requests DESC
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<ScapiCacheMetrics>(result.signature, result.firstFrame);
      if (firstFrameData.length > 0) {
        yield firstFrameData;
      }

      let done = result.firstFrame.done;
      let currentFrame: typeof result.firstFrame | undefined = result.firstFrame;

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
        
        const nextData = processFrame<ScapiCacheMetrics>(result.signature, currentFrame);
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

/**
 * Query controller performance to identify slow pages
 * Business Question: Which pages are performing poorly?
 * Primary users: Engineering and Operations teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryControllerPerformance(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<ControllerPerformance[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      SELECT
        cr.request_date,
        cr.controller_name,
        SUM(cr.num_requests) as total_requests,
        CASE WHEN SUM(cr.num_requests) > 0
             THEN SUM(cr.response_time) / SUM(cr.num_requests)
             ELSE 0
        END as avg_response_time,
        SUM(cr.num_requests_bucket1 + cr.num_requests_bucket2) as requests_under_100ms,
        SUM(cr.num_requests_bucket3 + cr.num_requests_bucket4 + cr.num_requests_bucket5) as requests_100_500ms,
        SUM(cr.num_requests - cr.num_requests_bucket1 - cr.num_requests_bucket2 - 
            cr.num_requests_bucket3 - cr.num_requests_bucket4 - cr.num_requests_bucket5) as requests_over_500ms
      FROM ccdw_aggr_controller_request cr
      JOIN ccdw_dim_site s ON s.site_id = cr.site_id
      WHERE cr.request_date >= '${startDateStr}' AND cr.request_date <= '${endDateStr}'
        AND s.nsite_id = '${siteId}'
      GROUP BY cr.request_date, cr.controller_name
      ORDER BY avg_response_time DESC
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<ControllerPerformance>(result.signature, result.firstFrame);
      if (firstFrameData.length > 0) {
        yield firstFrameData;
      }

      let done = result.firstFrame.done;
      let currentFrame: typeof result.firstFrame | undefined = result.firstFrame;

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
        
        const nextData = processFrame<ControllerPerformance>(result.signature, currentFrame);
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