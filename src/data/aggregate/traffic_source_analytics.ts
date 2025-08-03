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

