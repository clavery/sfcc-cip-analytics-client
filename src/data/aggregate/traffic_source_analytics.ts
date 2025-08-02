import { CIPClient } from '../../cip-client';
import { DateRange, formatDateForSQL, cleanSQL } from '../types';
import { processFrame } from '../../utils';

export interface VisitReferrerRecord {
  visit_date: Date | string;
  site_id: number;
  device_class_code: string;
  referrer_medium: string;
  referrer_source: string;
  referrer_term: string | null;
  referrer_content: string | null;
  referrer_name: string | null;
  num_visits: number;
  locale_id: number;
  channel_id: number;
}

export interface TopReferrerAnalytics {
  traffic_medium: string;
  traffic_source: string;
  total_visits: number;
  visit_percentage: number;
}

export interface TrafficSourceConversion {
  traffic_medium: string;
  total_visits: number;
  converted_visits: number;
  std_revenue: number;
  conversion_rate: number;
  revenue_per_visit: number;
}

/**
 * Query top referrers to identify high-value traffic sources
 * Business Question: Where is my traffic coming from and which sources drive the most valuable visitors?
 * Primary users: Marketing teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param limit Number of top referrers to return (default: 20)
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryTopReferrers(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  limit: number = 20,
  batchSize: number = 100
): AsyncGenerator<TopReferrerAnalytics[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      WITH total AS (
        SELECT SUM(num_visits) AS total_visits
        FROM ccdw_aggr_visit_referrer
        WHERE visit_date >= '${startDateStr}'
          AND visit_date <= '${endDateStr}'
      )
      SELECT
        vr.referrer_medium AS traffic_medium,
        vr.referrer_source AS traffic_source,
        SUM(vr.num_visits) AS total_visits,
        SUM(vr.num_visits) * 100.0 / total.total_visits AS visit_percentage
      FROM ccdw_aggr_visit_referrer vr
      JOIN ccdw_dim_site s
        ON s.site_id = vr.site_id
      JOIN total ON TRUE
      WHERE vr.visit_date >= '${startDateStr}'
        AND vr.visit_date <= '${endDateStr}'
        AND s.nsite_id = '${siteId}'
      GROUP BY
        vr.referrer_medium,
        vr.referrer_source,
        total.total_visits
      ORDER BY total_visits DESC
      LIMIT ${limit}
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<TopReferrerAnalytics>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<TopReferrerAnalytics>(result.signature, currentFrame);
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
 * Query traffic source conversion rates to optimize marketing spend
 * Business Question: Which traffic sources drive conversions and revenue?
 * Primary users: Marketing and Digital teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryTrafficSourceConversion(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<TrafficSourceConversion[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      SELECT
        vr.referrer_medium AS traffic_medium,
        SUM(v.num_visits) AS total_visits,
        SUM(v.num_converted_visits) AS converted_visits,
        SUM(v.std_revenue) AS std_revenue,
        CASE WHEN SUM(v.num_visits) > 0
             THEN (CAST(SUM(v.num_converted_visits) AS FLOAT) / SUM(v.num_visits)) * 100
             ELSE 0
        END AS conversion_rate,
        CASE WHEN SUM(v.num_visits) > 0
             THEN SUM(v.std_revenue) / SUM(v.num_visits)
             ELSE 0
        END AS revenue_per_visit
      FROM ccdw_aggr_visit_referrer vr
      JOIN ccdw_aggr_visit v
        ON v.visit_date = vr.visit_date
        AND v.site_id = vr.site_id
        AND v.device_class_code = vr.device_class_code
      JOIN ccdw_dim_site s
        ON s.site_id = vr.site_id
      WHERE vr.visit_date >= '${startDateStr}'
        AND vr.visit_date <= '${endDateStr}'
        AND s.nsite_id = '${siteId}'
      GROUP BY vr.referrer_medium
      ORDER BY std_revenue DESC
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<TrafficSourceConversion>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<TrafficSourceConversion>(result.signature, currentFrame);
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
 * Query raw visit referrer data for custom traffic analysis
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results
 * @param filters Optional filters for site, device, referrer medium/source
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryVisitReferrer(
  client: CIPClient,
  dateRange?: DateRange,
  filters?: {
    siteId?: string;
    deviceClassCode?: string;
    referrerMedium?: string;
    referrerSource?: string;
  },
  batchSize: number = 100
): AsyncGenerator<VisitReferrerRecord[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    let sql = 'SELECT vr.* FROM ccdw_aggr_visit_referrer vr';
    const joins: string[] = [];
    const conditions: string[] = [];
    
    if (dateRange) {
      const startDateStr = formatDateForSQL(dateRange.startDate);
      const endDateStr = formatDateForSQL(dateRange.endDate);
      conditions.push(`vr.visit_date >= '${startDateStr}' AND vr.visit_date <= '${endDateStr}'`);
    }
    
    if (filters?.siteId) {
      joins.push('JOIN ccdw_dim_site s ON s.site_id = vr.site_id');
      conditions.push(`s.nsite_id = '${filters.siteId}'`);
    }
    
    if (filters?.deviceClassCode) {
      conditions.push(`vr.device_class_code = '${filters.deviceClassCode}'`);
    }
    
    if (filters?.referrerMedium) {
      conditions.push(`vr.referrer_medium = '${filters.referrerMedium}'`);
    }
    
    if (filters?.referrerSource) {
      conditions.push(`vr.referrer_source = '${filters.referrerSource}'`);
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
      
      const firstFrameData = processFrame<VisitReferrerRecord>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<VisitReferrerRecord>(result.signature, currentFrame);
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