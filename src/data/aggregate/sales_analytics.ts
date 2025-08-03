import { CIPClient } from '../../cip-client';
import { DateRange, formatDateForSQL, cleanSQL } from '../types';
import { processFrame } from '../../utils';
import {
  SiteSpecificQueryTemplateParams,
  QueryTemplateParams,
  formatDateRange,
  executeParameterizedQuery,
  validateRequiredParams,
  EnhancedQueryFunction,
} from '../helpers';

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
 * @param params Query parameters including siteId and dateRange
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const querySalesAnalytics: EnhancedQueryFunction<
  SalesMetrics,
  SiteSpecificQueryTemplateParams
> = async function* querySalesAnalytics(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100
): AsyncGenerator<SalesMetrics[], void, unknown> {
  const { sql, parameters } = querySalesAnalytics.QUERY(params);
  yield* executeParameterizedQuery<SalesMetrics>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

querySalesAnalytics.metadata = {
  name: "sales-analytics",
  description: "Track daily sales performance with automatic AOV and AOS calculations",
  category: "Sales Analytics",
  requiredParams: ["siteId", "from", "to"],
};

querySalesAnalytics.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
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
    WHERE ss.submit_date >= '${startDate}'
      AND ss.submit_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY ss.submit_date
    ORDER BY ss.submit_date
  `;

  return {
    sql,
    parameters: [],
  };
};

interface SalesSummaryQueryParams extends QueryTemplateParams {
  siteId?: string;
  deviceClassCode?: string;
  registered?: boolean;
}

/**
 * Query raw sales summary data for custom aggregations
 * @param client The Avatica client instance (must have an open connection)
 * @param params Query parameters including optional filters
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const querySalesSummary: EnhancedQueryFunction<
  SalesSummaryRecord,
  SalesSummaryQueryParams
> = async function* querySalesSummary(
  client: CIPClient,
  params: SalesSummaryQueryParams,
  batchSize: number = 100
): AsyncGenerator<SalesSummaryRecord[], void, unknown> {
  // Ensure dateRange has a default if not provided
  const queryParams: SalesSummaryQueryParams = {
    ...params,
    dateRange: params.dateRange || { startDate: new Date(0), endDate: new Date() },
  };
  const { sql, parameters } = querySalesSummary.QUERY(queryParams);
  yield* executeParameterizedQuery<SalesSummaryRecord>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

querySalesSummary.metadata = {
  name: "sales-summary",
  description: "Query raw sales summary data for custom aggregations",
  category: "Sales Analytics",
  requiredParams: [],
  optionalParams: ["siteId", "deviceClassCode", "registered", "from", "to"],
};

querySalesSummary.QUERY = (
  params: SalesSummaryQueryParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["dateRange"]);

  let sql = '';
  const joins: string[] = [];
  const conditions: string[] = [];
  
  if (params.siteId) {
    sql = `
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
    `;
    conditions.push(`s.nsite_id = '${params.siteId}'`);
  } else {
    sql = `
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
    `;
  }
  
  if (params.dateRange) {
    const { startDate, endDate } = formatDateRange(params.dateRange);
    conditions.push(`submit_date >= '${startDate}' AND submit_date <= '${endDate}'`);
  }
  
  if (params.deviceClassCode) {
    conditions.push(`device_class_code = '${params.deviceClassCode}'`);
  }
  
  if (params.registered !== undefined) {
    conditions.push(`registered = ${params.registered}`);
  }
  
  if (conditions.length > 0) {
    sql += ' WHERE ' + conditions.join(' AND ');
  }

  return {
    sql,
    parameters: [],
  };
};
