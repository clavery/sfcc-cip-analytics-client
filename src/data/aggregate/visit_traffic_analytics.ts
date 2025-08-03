import { CIPClient } from "../../cip-client";
import { DateRange, cleanSQL } from "../types";
import {
  SiteSpecificQueryTemplateParams,
  formatDateRange,
  executeQuery,
  executeParameterizedQuery,
  QueryTemplateParams,
  validateRequiredParams,
  EnhancedQueryFunction,
} from "../helpers";

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
export const queryVisitMetricsByDevice: EnhancedQueryFunction<
  VisitMetricsByDevice,
  SiteSpecificQueryTemplateParams
> = function queryVisitMetricsByDevice(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<VisitMetricsByDevice[], void, unknown> {
  const { sql, parameters } = queryVisitMetricsByDevice.QUERY(params);
  return executeParameterizedQuery<VisitMetricsByDevice>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryVisitMetricsByDevice.metadata = {
  name: "visit-metrics-by-device",
  description:
    "Analyze visit patterns and performance metrics by device type",
  category: "Traffic Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryVisitMetricsByDevice.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
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
    WHERE v.visit_date >= '${startDate}'
      AND v.visit_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY
      v.visit_date,
      v.device_class_code
    ORDER BY
      v.visit_date,
      v.device_class_code
  `;

  return {
    sql,
    parameters: [],
  };
};

/**
 * Query checkout funnel metrics to identify abandonment points
 * Business Question: Where are customers dropping off in the checkout process?
 * Primary users: UX and Conversion teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryCheckoutFunnelMetrics: EnhancedQueryFunction<
  CheckoutFunnelMetrics,
  SiteSpecificQueryTemplateParams
> = async function* queryCheckoutFunnelMetrics(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<CheckoutFunnelMetrics[], void, unknown> {
  const { sql, parameters } = queryCheckoutFunnelMetrics.QUERY(params);
  yield* executeParameterizedQuery<CheckoutFunnelMetrics>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryCheckoutFunnelMetrics.metadata = {
  name: "checkout-funnel-metrics",
  description: "Analyze checkout process abandonment points and conversion rates",
  category: "Traffic Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryCheckoutFunnelMetrics.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
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
      WHERE vc.visit_date >= '${startDate}'
        AND vc.visit_date <= '${endDate}'
        AND s.nsite_id = '${params.siteId}'
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
  `;

  return {
    sql,
    parameters: [],
  };
};

export interface BrowserDeviceUsage {
  browser: string;
  browser_version: string;
  os: string;
  device_type: string;
  visit_count: number;
  revenue_share: number;
}

interface BrowserDeviceUsageParams extends SiteSpecificQueryTemplateParams {
  limit?: number;
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
export const queryBrowserDeviceUsage: EnhancedQueryFunction<
  BrowserDeviceUsage,
  BrowserDeviceUsageParams
> = async function* queryBrowserDeviceUsage(
  client: CIPClient,
  params: BrowserDeviceUsageParams,
  batchSize: number = 100,
): AsyncGenerator<BrowserDeviceUsage[], void, unknown> {
  const { sql, parameters } = queryBrowserDeviceUsage.QUERY(params);
  yield* executeParameterizedQuery<BrowserDeviceUsage>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryBrowserDeviceUsage.metadata = {
  name: "browser-device-usage",
  description: "Analyze browser and device usage patterns for UX optimization",
  category: "Traffic Analytics",
  requiredParams: ["siteId", "from", "to"],
  optionalParams: ["limit"],
};

queryBrowserDeviceUsage.QUERY = (
  params: BrowserDeviceUsageParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);
  const limit = params.limit || 50;

  const sql = `
    WITH total_revenue AS (
      SELECT SUM(std_revenue) as total_rev
      FROM ccdw_aggr_visit_user_agent
      WHERE visit_date >= '${startDate}'
        AND visit_date <= '${endDate}'
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
    WHERE vua.visit_date >= '${startDate}'
      AND vua.visit_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY
      vua.browser,
      vua.browser_version,
      vua.os,
      vua.device_type,
      total.total_rev
    ORDER BY visit_count DESC
    LIMIT ${limit}
  `;

  return {
    sql,
    parameters: [],
  };
};

interface VisitQueryParams extends QueryTemplateParams {
  siteId?: string;
  deviceClassCode?: string;
  registered?: boolean;
}

/**
 * Query raw visit data for custom analysis
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results
 * @param filters Optional filters for site, device, registration status
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryVisit: EnhancedQueryFunction<
  VisitRecord,
  VisitQueryParams
> = async function* queryVisit(
  client: CIPClient,
  params: VisitQueryParams,
  batchSize: number = 100,
): AsyncGenerator<VisitRecord[], void, unknown> {
  // Ensure dateRange has a default if not provided
  const queryParams: VisitQueryParams = {
    ...params,
    dateRange: params.dateRange || { startDate: new Date(0), endDate: new Date() },
  };
  const { sql, parameters } = queryVisit.QUERY(queryParams);
  yield* executeParameterizedQuery<VisitRecord>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryVisit.metadata = {
  name: "visit-raw",
  description: "Query raw visit data for custom analysis",
  category: "Traffic Analytics",
  requiredParams: [],
  optionalParams: ["siteId", "deviceClassCode", "registered", "from", "to"],
};

queryVisit.QUERY = (
  params: VisitQueryParams,
): { sql: string; parameters: any[] } => {
  // dateRange is required in the params structure, even if it was optional in the function signature
  validateRequiredParams(params, ["dateRange"]);

  let sql = "SELECT v.* FROM ccdw_aggr_visit v";
  const joins: string[] = [];
  const conditions: string[] = [];

  if (params.dateRange) {
    const { startDate, endDate } = formatDateRange(params.dateRange);
    conditions.push(`v.visit_date >= '${startDate}' AND v.visit_date <= '${endDate}'`);
  }

  if (params.siteId) {
    joins.push("JOIN ccdw_dim_site s ON s.site_id = v.site_id");
    conditions.push(`s.nsite_id = '${params.siteId}'`);
  }

  if (params.deviceClassCode) {
    conditions.push(`v.device_class_code = '${params.deviceClassCode}'`);
  }

  if (params.registered !== undefined) {
    conditions.push(`v.registered = ${params.registered}`);
  }

  if (joins.length > 0) {
    sql += " " + joins.join(" ");
  }

  if (conditions.length > 0) {
    sql += " WHERE " + conditions.join(" AND ");
  }

  return {
    sql,
    parameters: [],
  };
};