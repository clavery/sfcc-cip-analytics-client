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

interface TopReferrersQueryParams extends SiteSpecificQueryTemplateParams {
  limit?: number;
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
export const queryTopReferrers: EnhancedQueryFunction<
  TopReferrerAnalytics,
  TopReferrersQueryParams
> = function queryTopReferrers(
  client: CIPClient,
  params: TopReferrersQueryParams,
  batchSize: number = 100,
): AsyncGenerator<TopReferrerAnalytics[], void, unknown> {
  const { sql, parameters } = queryTopReferrers.QUERY(params);
  return executeParameterizedQuery<TopReferrerAnalytics>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryTopReferrers.metadata = {
  name: "top-referrers",
  description:
    "Identify high-value traffic sources and referrer performance",
  category: "Traffic Analytics",
  requiredParams: ["siteId", "from", "to"],
  optionalParams: ["limit"],
};

queryTopReferrers.QUERY = (
  params: TopReferrersQueryParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);
  const limit = params.limit || 20;

  const sql = `
    WITH total AS (
      SELECT SUM(num_visits) AS total_visits
      FROM ccdw_aggr_visit_referrer
      WHERE visit_date >= '${startDate}'
        AND visit_date <= '${endDate}'
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
    WHERE vr.visit_date >= '${startDate}'
      AND vr.visit_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY
      vr.referrer_medium,
      vr.referrer_source,
      total.total_visits
    ORDER BY total_visits DESC
    LIMIT ${limit}
  `;

  return {
    sql,
    parameters: [],
  };
};

/**
 * Query traffic source conversion rates to optimize marketing spend
 * Business Question: Which traffic sources drive conversions and revenue?
 * Primary users: Marketing and Digital teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryTrafficSourceConversion: EnhancedQueryFunction<
  TrafficSourceConversion,
  SiteSpecificQueryTemplateParams
> = async function* queryTrafficSourceConversion(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<TrafficSourceConversion[], void, unknown> {
  const { sql, parameters } = queryTrafficSourceConversion.QUERY(params);
  yield* executeParameterizedQuery<TrafficSourceConversion>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryTrafficSourceConversion.metadata = {
  name: "traffic-source-conversion",
  description: "Analyze traffic source conversion rates and revenue impact",
  category: "Traffic Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryTrafficSourceConversion.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
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
    WHERE vr.visit_date >= '${startDate}'
      AND vr.visit_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY vr.referrer_medium
    ORDER BY std_revenue DESC
  `;

  return {
    sql,
    parameters: [],
  };
};

interface VisitReferrerQueryParams extends QueryTemplateParams {
  siteId?: string;
  deviceClassCode?: string;
  referrerMedium?: string;
  referrerSource?: string;
}

/**
 * Query raw visit referrer data for custom traffic analysis
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results
 * @param filters Optional filters for site, device, referrer medium/source
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryVisitReferrer: EnhancedQueryFunction<
  VisitReferrerRecord,
  VisitReferrerQueryParams
> = async function* queryVisitReferrer(
  client: CIPClient,
  params: VisitReferrerQueryParams,
  batchSize: number = 100,
): AsyncGenerator<VisitReferrerRecord[], void, unknown> {
  // Ensure dateRange has a default if not provided
  const queryParams: VisitReferrerQueryParams = {
    ...params,
    dateRange: params.dateRange || { startDate: new Date(0), endDate: new Date() },
  };
  const { sql, parameters } = queryVisitReferrer.QUERY(queryParams);
  yield* executeParameterizedQuery<VisitReferrerRecord>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryVisitReferrer.metadata = {
  name: "visit-referrer-raw",
  description: "Query raw visit referrer data for custom traffic analysis",
  category: "Traffic Analytics",
  requiredParams: [],
  optionalParams: ["siteId", "deviceClassCode", "referrerMedium", "referrerSource", "from", "to"],
};

queryVisitReferrer.QUERY = (
  params: VisitReferrerQueryParams,
): { sql: string; parameters: any[] } => {
  // dateRange is required in the params structure, even if it was optional in the function signature
  validateRequiredParams(params, ["dateRange"]);

  let sql = "SELECT vr.* FROM ccdw_aggr_visit_referrer vr";
  const joins: string[] = [];
  const conditions: string[] = [];

  if (params.dateRange) {
    const { startDate, endDate } = formatDateRange(params.dateRange);
    conditions.push(`vr.visit_date >= '${startDate}' AND vr.visit_date <= '${endDate}'`);
  }

  if (params.siteId) {
    joins.push("JOIN ccdw_dim_site s ON s.site_id = vr.site_id");
    conditions.push(`s.nsite_id = '${params.siteId}'`);
  }

  if (params.deviceClassCode) {
    conditions.push(`vr.device_class_code = '${params.deviceClassCode}'`);
  }

  if (params.referrerMedium) {
    conditions.push(`vr.referrer_medium = '${params.referrerMedium}'`);
  }

  if (params.referrerSource) {
    conditions.push(`vr.referrer_source = '${params.referrerSource}'`);
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