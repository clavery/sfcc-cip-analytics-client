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

