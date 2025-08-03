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
export const queryOcapiPerformance: EnhancedQueryFunction<
  OcapiPerformance,
  SiteSpecificQueryTemplateParams
> = function queryOcapiPerformance(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<OcapiPerformance[], void, unknown> {
  const { sql, parameters } = queryOcapiPerformance.QUERY(params);
  return executeParameterizedQuery<OcapiPerformance>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryOcapiPerformance.metadata = {
  name: "ocapi-performance",
  description:
    "Monitor OCAPI performance metrics and system health",
  category: "Technical Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryOcapiPerformance.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
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
    WHERE o.request_date >= '${startDate}' AND o.request_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY o.request_date, o.api_name, o.api_resource, o.client_id
    ORDER BY total_requests DESC
  `;

  return {
    sql,
    parameters: [],
  };
};

/**
 * Query SCAPI cache performance to optimize caching strategy
 * Business Question: How effective is our API caching?
 * Primary users: Engineering and Performance teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryScapiCacheMetrics: EnhancedQueryFunction<
  ScapiCacheMetrics,
  SiteSpecificQueryTemplateParams
> = async function* queryScapiCacheMetrics(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<ScapiCacheMetrics[], void, unknown> {
  const { sql, parameters } = queryScapiCacheMetrics.QUERY(params);
  yield* executeParameterizedQuery<ScapiCacheMetrics>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryScapiCacheMetrics.metadata = {
  name: "scapi-cache-metrics",
  description: "Analyze SCAPI cache performance and optimization opportunities",
  category: "Technical Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryScapiCacheMetrics.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
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
    WHERE sc.request_date >= '${startDate}' AND sc.request_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY sc.request_date, sc.api_family, sc.api_name
    ORDER BY total_requests DESC
  `;

  return {
    sql,
    parameters: [],
  };
};

/**
 * Query controller performance to identify slow pages
 * Business Question: Which pages are performing poorly?
 * Primary users: Engineering and Operations teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryControllerPerformance: EnhancedQueryFunction<
  ControllerPerformance,
  SiteSpecificQueryTemplateParams
> = async function* queryControllerPerformance(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<ControllerPerformance[], void, unknown> {
  const { sql, parameters } = queryControllerPerformance.QUERY(params);
  yield* executeParameterizedQuery<ControllerPerformance>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryControllerPerformance.metadata = {
  name: "controller-performance",
  description: "Identify slow pages and controller performance bottlenecks",
  category: "Technical Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryControllerPerformance.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
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
    WHERE cr.request_date >= '${startDate}' AND cr.request_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY cr.request_date, cr.controller_name
    ORDER BY avg_response_time DESC
  `;

  return {
    sql,
    parameters: [],
  };
};