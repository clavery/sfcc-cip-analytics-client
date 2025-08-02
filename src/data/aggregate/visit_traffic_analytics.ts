import { CIPClient } from '../../cip-client';
import { DateRange, formatDateForSQL, cleanSQL } from '../types';
import { processFrame } from '../../utils';

export interface VisitRecord {
  visit_date: Date | string;
  site_id: number;
  device_class_code: string;
  registered: boolean;
  num_visits: number;
  num_converted_visits: number;
  num_bounced_visits: number;
  visit_duration: number;
  num_page_views: number;
  num_orders: number;
  num_units: number;
  std_revenue: number;
  locale_id: number;
  channel_id: number;
}

export interface VisitMetricsByDevice {
  visit_dt: Date | string;
  device_class_code: string;
  visits: number;
  converted_visits: number;
  total_duration: number;
  std_revenue: number;
  revenue_per_visit: number;
}

export interface CheckoutFunnelMetrics {
  step_name: string;
  step_sequence: number;
  num_visits: number;
  num_abandonments: number;
  abandonment_rate: number;
  conversion_to_next_step: number;
}

/**
 * Query visit metrics by device to understand traffic patterns
 * Business Question: What's the complete picture of site performance from traffic to revenue?
 * Primary users: Marketing and UX teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryVisitMetricsByDevice(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<VisitMetricsByDevice[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      SELECT
        v.visit_date AS visit_dt,
        v.device_class_code,
        SUM(v.num_visits) AS visits,
        SUM(v.num_converted_visits) AS converted_visits,
        SUM(v.visit_duration) AS total_duration,
        SUM(v.std_revenue) AS std_revenue,
        CASE WHEN SUM(v.num_visits) > 0
             THEN SUM(v.std_revenue) / SUM(v.num_visits)
             ELSE 0
        END AS revenue_per_visit
      FROM ccdw_aggr_visit v
      JOIN ccdw_dim_site s
        ON s.site_id = v.site_id
      WHERE v.visit_date >= '${startDateStr}'
        AND v.visit_date <= '${endDateStr}'
        AND s.nsite_id = '${siteId}'
      GROUP BY
        v.visit_date,
        v.device_class_code
      ORDER BY
        v.visit_date,
        v.device_class_code
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<VisitMetricsByDevice>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<VisitMetricsByDevice>(result.signature, currentFrame);
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
 * Query checkout funnel metrics to identify abandonment points
 * Business Question: Where are customers dropping off in the checkout process?
 * Primary users: UX and Conversion teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryCheckoutFunnelMetrics(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<CheckoutFunnelMetrics[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      WITH funnel_data AS (
        SELECT
          cs.step_name,
          cs.step_id,
          SUM(vc.num_visits) as step_visits,
          SUM(vc.num_abandonments) as step_abandonments,
          LAG(SUM(vc.num_visits), 1) OVER (ORDER BY cs.step_id) as prev_step_visits
        FROM ccdw_aggr_visit_checkout vc
        JOIN ccdw_dim_checkout_step cs ON cs.checkout_step_id = vc.checkout_step_id
        JOIN ccdw_dim_site s ON s.site_id = vc.site_id
        WHERE vc.visit_date >= '${startDateStr}'
          AND vc.visit_date <= '${endDateStr}'
          AND s.nsite_id = '${siteId}'
        GROUP BY cs.step_name, cs.step_id
      )
      SELECT
        step_name,
        step_id as step_sequence,
        step_visits as num_visits,
        step_abandonments as num_abandonments,
        CASE WHEN step_visits > 0
             THEN (CAST(step_abandonments AS FLOAT) / step_visits) * 100
             ELSE 0
        END as abandonment_rate,
        CASE WHEN prev_step_visits > 0
             THEN (CAST(step_visits AS FLOAT) / prev_step_visits) * 100
             ELSE 100
        END as conversion_to_next_step
      FROM funnel_data
      ORDER BY step_id
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<CheckoutFunnelMetrics>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<CheckoutFunnelMetrics>(result.signature, currentFrame);
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
 * Query browser and device usage patterns
 * Business Question: What browsers and devices are customers using?
 * Primary users: Engineering and UX teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param limit Maximum number of user agents to return (default: 50)
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryBrowserDeviceUsage(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  limit: number = 50,
  batchSize: number = 100
): AsyncGenerator<{
  browser: string;
  browser_version: string;
  os: string;
  device_type: string;
  visit_count: number;
  revenue_share: number;
}[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      WITH total_revenue AS (
        SELECT SUM(std_revenue) as total_rev
        FROM ccdw_aggr_visit_user_agent
        WHERE visit_date >= '${startDateStr}'
          AND visit_date <= '${endDateStr}'
      )
      SELECT
        vua.browser,
        vua.browser_version,
        vua.os,
        vua.device_type,
        SUM(vua.num_visits) as visit_count,
        (SUM(vua.std_revenue) / total.total_rev) * 100 as revenue_share
      FROM ccdw_aggr_visit_user_agent vua
      JOIN ccdw_dim_site s ON s.site_id = vua.site_id
      JOIN total_revenue total ON TRUE
      WHERE vua.visit_date >= '${startDateStr}'
        AND vua.visit_date <= '${endDateStr}'
        AND s.nsite_id = '${siteId}'
      GROUP BY
        vua.browser,
        vua.browser_version,
        vua.os,
        vua.device_type,
        total.total_rev
      ORDER BY visit_count DESC
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
 * Query raw visit data for custom analysis
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results
 * @param filters Optional filters for site, device, registration status
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryVisit(
  client: CIPClient,
  dateRange?: DateRange,
  filters?: {
    siteId?: string;
    deviceClassCode?: string;
    registered?: boolean;
  },
  batchSize: number = 100
): AsyncGenerator<VisitRecord[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    let sql = 'SELECT v.* FROM ccdw_aggr_visit v';
    const joins: string[] = [];
    const conditions: string[] = [];
    
    if (dateRange) {
      const startDateStr = formatDateForSQL(dateRange.startDate);
      const endDateStr = formatDateForSQL(dateRange.endDate);
      conditions.push(`v.visit_date >= '${startDateStr}' AND v.visit_date <= '${endDateStr}'`);
    }
    
    if (filters?.siteId) {
      joins.push('JOIN ccdw_dim_site s ON s.site_id = v.site_id');
      conditions.push(`s.nsite_id = '${filters.siteId}'`);
    }
    
    if (filters?.deviceClassCode) {
      conditions.push(`v.device_class_code = '${filters.deviceClassCode}'`);
    }
    
    if (filters?.registered !== undefined) {
      conditions.push(`v.registered = ${filters.registered}`);
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
      
      const firstFrameData = processFrame<VisitRecord>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<VisitRecord>(result.signature, currentFrame);
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