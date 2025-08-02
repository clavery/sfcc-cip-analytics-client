import { CIPClient } from '../../cip-client';
import { DateRange, formatDateForSQL, cleanSQL } from '../types';
import { processFrame } from '../../utils';

export interface SalesSummaryRecord {
  submit_date: Date | string;
  site_id: number;
  business_channel_id: number;
  registered: boolean;
  first_time_buyer: boolean;
  device_class_code: string;
  locale_code: string;
  std_revenue: number;
  num_orders: number;
  num_units: number;
  std_tax: number;
  std_shipping: number;
  std_total_discount: number;
}

export interface SalesMetrics {
  date: Date | string;
  std_revenue: number;
  orders: number;
  std_aov: number;
  units: number;
  aos: number;
  std_tax: number;
  std_shipping: number;
}

/**
 * Query sales analytics data to track daily performance with automatic AOV/AOS calculations
 * Business Question: How is my business performing across revenue and orders?
 * Primary users: Merchandising teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results by submit_date
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* querySalesAnalytics(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<SalesMetrics[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      SELECT
        CAST(ss.submit_date AS VARCHAR) AS "date",
        SUM(std_revenue) AS std_revenue,
        SUM(num_orders) AS orders,
        CAST(SUM(std_revenue) / SUM(num_orders) AS DECIMAL(15,2)) AS std_aov,
        SUM(num_units) AS units,
        CAST(SUM(num_units) / SUM(num_orders) AS DECIMAL(15,2)) AS aos,
        SUM(std_tax) AS std_tax,
        SUM(std_shipping) AS std_shipping
      FROM ccdw_aggr_sales_summary ss
      JOIN ccdw_dim_site s
        ON s.site_id = ss.site_id
      WHERE ss.submit_date >= '${startDateStr}'
        AND ss.submit_date <= '${endDateStr}'
        AND s.nsite_id = '${siteId}'
      GROUP BY ss.submit_date
      ORDER BY ss.submit_date
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<SalesMetrics>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<SalesMetrics>(result.signature, currentFrame);
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
 * Query raw sales summary data for custom aggregations
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results
 * @param filters Optional filters for site, device, registration status
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* querySalesSummary(
  client: CIPClient,
  dateRange?: DateRange,
  filters?: {
    siteId?: string;
    deviceClassCode?: string;
    registered?: boolean;
  },
  batchSize: number = 100
): AsyncGenerator<SalesSummaryRecord[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    let sql = '';
    const conditions: string[] = [];
    
    if (filters?.siteId) {
      sql = cleanSQL(`
        SELECT 
          ss.submit_date,
          ss.site_id,
          ss.business_channel_id,
          ss.registered,
          ss.first_time_buyer,
          ss.device_class_code,
          ss.locale_code,
          ss.std_revenue,
          ss.num_orders,
          ss.num_units,
          ss.std_tax,
          ss.std_shipping,
          ss.std_total_discount
        FROM ccdw_aggr_sales_summary ss
        JOIN ccdw_dim_site s ON s.site_id = ss.site_id
      `);
      conditions.push(`s.nsite_id = '${filters.siteId}'`);
    } else {
      sql = cleanSQL(`
        SELECT 
          submit_date,
          site_id,
          business_channel_id,
          registered,
          first_time_buyer,
          device_class_code,
          locale_code,
          std_revenue,
          num_orders,
          num_units,
          std_tax,
          std_shipping,
          std_total_discount
        FROM ccdw_aggr_sales_summary
      `);
    }
    
    if (dateRange) {
      const startDateStr = formatDateForSQL(dateRange.startDate);
      const endDateStr = formatDateForSQL(dateRange.endDate);
      conditions.push(`submit_date >= '${startDateStr}' AND submit_date <= '${endDateStr}'`);
    }
    
    if (filters?.deviceClassCode) {
      conditions.push(`device_class_code = '${filters.deviceClassCode}'`);
    }
    
    if (filters?.registered !== undefined) {
      conditions.push(`registered = ${filters.registered}`);
    }
    
    if (conditions.length > 0) {
      sql += ' WHERE ' + conditions.join(' AND ');
    }

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<SalesSummaryRecord>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<SalesSummaryRecord>(result.signature, currentFrame);
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
