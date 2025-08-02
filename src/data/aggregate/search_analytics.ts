import { CIPClient } from '../../cip-client';
import { DateRange, formatDateForSQL, cleanSQL } from '../types';
import { processFrame } from '../../utils';

export interface SearchConversionRecord {
  search_date: Date | string;
  site_id: number;
  query: string;
  has_results: boolean;
  device_class_code: string;
  registered: boolean;
  num_searches: number;
  num_orders: number;
  num_units: number;
  std_revenue: number;
  locale_id: number;
  channel_id: number;
}

export interface SearchQueryPerformance {
  query: string;
  converted_searches: number;
  orders: number;
  std_revenue: number;
  std_revenue_per_order: number;
  conversion_rate: number;
}

/**
 * Query search term performance to identify revenue drivers and missed opportunities
 * Business Question: Which search terms drive revenue vs which represent missed opportunities?
 * Primary users: Merchandising and UX teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param hasResults Filter by whether searches had results (true) or not (false)
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* querySearchQueryPerformance(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  hasResults: boolean,
  batchSize: number = 100
): AsyncGenerator<SearchQueryPerformance[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      WITH conversion AS (
        SELECT
          LOWER(sc.query) AS query,
          SUM(sc.num_searches) AS converted_searches,
          SUM(sc.num_orders) AS orders,
          SUM(sc.std_revenue) AS std_revenue,
          SUM(sc.std_revenue) / NULLIF(CAST(SUM(sc.num_orders) AS FLOAT), 0) AS std_revenue_per_order
        FROM ccdw_aggr_search_conversion sc
        JOIN ccdw_dim_site s
          ON s.site_id = sc.site_id
        WHERE sc.search_date >= '${startDateStr}'
          AND sc.search_date <= '${endDateStr}'
          AND s.nsite_id = '${siteId}'
          AND sc.has_results = ${hasResults}
        GROUP BY LOWER(sc.query)
      )
      SELECT
        query,
        converted_searches,
        orders,
        std_revenue,
        std_revenue_per_order,
        CASE WHEN converted_searches > 0
             THEN (CAST(orders AS FLOAT) / converted_searches) * 100
             ELSE 0
        END AS conversion_rate
      FROM conversion
      ORDER BY std_revenue DESC
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<SearchQueryPerformance>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<SearchQueryPerformance>(result.signature, currentFrame);
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
 * Query failed searches to identify catalog gaps
 * Business Question: What are customers searching for that we don't have?
 * Primary users: Merchandising and Product teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param limit Number of top failed searches to return (default: 50)
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryFailedSearches(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  limit: number = 50,
  batchSize: number = 100
): AsyncGenerator<{
  query: string;
  search_count: number;
  unique_searchers: number;
}[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      SELECT
        LOWER(sq.query) AS query,
        SUM(sq.num_searches) AS search_count,
        COUNT(DISTINCT sq.session_id) AS unique_searchers
      FROM ccdw_aggr_search_query sq
      JOIN ccdw_dim_site s ON s.site_id = sq.site_id
      WHERE sq.search_date >= '${startDateStr}'
        AND sq.search_date <= '${endDateStr}'
        AND s.nsite_id = '${siteId}'
        AND sq.hit_count = 0
      GROUP BY LOWER(sq.query)
      ORDER BY search_count DESC
      LIMIT ${limit}
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<any>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<any>(result.signature, currentFrame);
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
 * Query raw search conversion data
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results
 * @param filters Optional filters for site, query, device, registration status
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* querySearchConversion(
  client: CIPClient,
  dateRange?: DateRange,
  filters?: {
    siteId?: string;
    query?: string;
    hasResults?: boolean;
    deviceClassCode?: string;
    registered?: boolean;
  },
  batchSize: number = 100
): AsyncGenerator<SearchConversionRecord[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    let sql = 'SELECT sc.* FROM ccdw_aggr_search_conversion sc';
    const joins: string[] = [];
    const conditions: string[] = [];
    
    if (dateRange) {
      const startDateStr = formatDateForSQL(dateRange.startDate);
      const endDateStr = formatDateForSQL(dateRange.endDate);
      conditions.push(`sc.search_date >= '${startDateStr}' AND sc.search_date <= '${endDateStr}'`);
    }
    
    if (filters?.siteId) {
      joins.push('JOIN ccdw_dim_site s ON s.site_id = sc.site_id');
      conditions.push(`s.nsite_id = '${filters.siteId}'`);
    }
    
    if (filters?.query) {
      conditions.push(`LOWER(sc.query) = LOWER('${filters.query}')`);
    }
    
    if (filters?.hasResults !== undefined) {
      conditions.push(`sc.has_results = ${filters.hasResults}`);
    }
    
    if (filters?.deviceClassCode) {
      conditions.push(`sc.device_class_code = '${filters.deviceClassCode}'`);
    }
    
    if (filters?.registered !== undefined) {
      conditions.push(`sc.registered = ${filters.registered}`);
    }
    
    if (joins.length > 0) {
      sql += ' ' + joins.join(' ');
    }
    
    if (conditions.length > 0) {
      sql += ' WHERE ' + conditions.join(' AND ');
    }

    const executeResponse = await client.execute(statementId, cleanSQL(sql), batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<SearchConversionRecord>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<SearchConversionRecord>(result.signature, currentFrame);
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