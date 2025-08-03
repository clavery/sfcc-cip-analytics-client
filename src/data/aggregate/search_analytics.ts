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

interface SearchQueryPerformanceParams extends SiteSpecificQueryTemplateParams {
  hasResults: boolean;
}

/**
 * Query search term performance to identify revenue drivers and missed opportunities
 * Business Question: Which search terms drive revenue vs which represent missed opportunities?
 * Primary users: Merchandising and UX teams
 * @param client The Avatica client instance (must have an open connection)
 * @param params Query parameters including siteId, dateRange, and hasResults
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const querySearchQueryPerformance: EnhancedQueryFunction<
  SearchQueryPerformance,
  SearchQueryPerformanceParams
> = async function* querySearchQueryPerformance(
  client: CIPClient,
  params: SearchQueryPerformanceParams,
  batchSize: number = 100
): AsyncGenerator<SearchQueryPerformance[], void, unknown> {
  const { sql, parameters } = querySearchQueryPerformance.QUERY(params);
  yield* executeParameterizedQuery<SearchQueryPerformance>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

querySearchQueryPerformance.metadata = {
  name: "search-query-performance",
  description: "Identify which search terms drive revenue vs missed opportunities",
  category: "Search Analytics",
  requiredParams: ["siteId", "hasResults", "from", "to"],
};

querySearchQueryPerformance.QUERY = (
  params: SearchQueryPerformanceParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange", "hasResults"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
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
      WHERE sc.search_date >= '${startDate}'
        AND sc.search_date <= '${endDate}'
        AND s.nsite_id = '${params.siteId}'
        AND sc.has_results = ${params.hasResults}
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
  `;

  return {
    sql,
    parameters: [],
  };
};

export interface FailedSearchRecord {
  query: string;
  search_count: number;
  unique_searchers: number;
}

interface FailedSearchParams extends SiteSpecificQueryTemplateParams {
  limit?: number;
}

/**
 * Query failed searches to identify catalog gaps
 * Business Question: What are customers searching for that we don't have?
 * Primary users: Merchandising and Product teams
 * @param client The Avatica client instance (must have an open connection)
 * @param params Query parameters including siteId, dateRange, and optional limit
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryFailedSearches: EnhancedQueryFunction<
  FailedSearchRecord,
  FailedSearchParams
> = async function* queryFailedSearches(
  client: CIPClient,
  params: FailedSearchParams,
  batchSize: number = 100
): AsyncGenerator<FailedSearchRecord[], void, unknown> {
  const { sql, parameters } = queryFailedSearches.QUERY(params);
  yield* executeParameterizedQuery<FailedSearchRecord>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryFailedSearches.metadata = {
  name: "failed-searches",
  description: "Identify catalog gaps through failed search analysis",
  category: "Search Analytics",
  requiredParams: ["siteId", "from", "to"],
  optionalParams: ["limit"],
};

queryFailedSearches.QUERY = (
  params: FailedSearchParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);
  const limit = params.limit || 50;

  const sql = `
    SELECT
      LOWER(sq.query) AS query,
      SUM(sq.num_searches) AS search_count,
      COUNT(DISTINCT sq.session_id) AS unique_searchers
    FROM ccdw_aggr_search_query sq
    JOIN ccdw_dim_site s ON s.site_id = sq.site_id
    WHERE sq.search_date >= '${startDate}'
      AND sq.search_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
      AND sq.hit_count = 0
    GROUP BY LOWER(sq.query)
    ORDER BY search_count DESC
    LIMIT ${limit}
  `;

  return {
    sql,
    parameters: [],
  };
};

interface SearchConversionQueryParams extends QueryTemplateParams {
  siteId?: string;
  query?: string;
  hasResults?: boolean;
  deviceClassCode?: string;
  registered?: boolean;
}

/**
 * Query raw search conversion data
 * @param client The Avatica client instance (must have an open connection)
 * @param params Query parameters including optional filters
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const querySearchConversion: EnhancedQueryFunction<
  SearchConversionRecord,
  SearchConversionQueryParams
> = async function* querySearchConversion(
  client: CIPClient,
  params: SearchConversionQueryParams,
  batchSize: number = 100
): AsyncGenerator<SearchConversionRecord[], void, unknown> {
  // Ensure dateRange has a default if not provided
  const queryParams: SearchConversionQueryParams = {
    ...params,
    dateRange: params.dateRange || { startDate: new Date(0), endDate: new Date() },
  };
  const { sql, parameters } = querySearchConversion.QUERY(queryParams);
  yield* executeParameterizedQuery<SearchConversionRecord>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

querySearchConversion.metadata = {
  name: "search-conversion",
  description: "Analyze search patterns and conversion across customer segments",
  category: "Search Analytics",
  requiredParams: [],
  optionalParams: ["siteId", "query", "hasResults", "deviceClassCode", "registered", "from", "to"],
};

querySearchConversion.QUERY = (
  params: SearchConversionQueryParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["dateRange"]);

  let sql = 'SELECT sc.* FROM ccdw_aggr_search_conversion sc';
  const joins: string[] = [];
  const conditions: string[] = [];
  
  if (params.dateRange) {
    const { startDate, endDate } = formatDateRange(params.dateRange);
    conditions.push(`sc.search_date >= '${startDate}' AND sc.search_date <= '${endDate}'`);
  }
  
  if (params.siteId) {
    joins.push('JOIN ccdw_dim_site s ON s.site_id = sc.site_id');
    conditions.push(`s.nsite_id = '${params.siteId}'`);
  }
  
  if (params.query) {
    conditions.push(`LOWER(sc.query) = LOWER('${params.query}')`);
  }
  
  if (params.hasResults !== undefined) {
    conditions.push(`sc.has_results = ${params.hasResults}`);
  }
  
  if (params.deviceClassCode) {
    conditions.push(`sc.device_class_code = '${params.deviceClassCode}'`);
  }
  
  if (params.registered !== undefined) {
    conditions.push(`sc.registered = ${params.registered}`);
  }
  
  if (joins.length > 0) {
    sql += ' ' + joins.join(' ');
  }
  
  if (conditions.length > 0) {
    sql += ' WHERE ' + conditions.join(' AND ');
  }

  return {
    sql,
    parameters: [],
  };
};