import { CIPClient } from "../../cip-client";
import { DateRange, cleanSQL } from "../types";
import {
  SiteSpecificQueryTemplateParams,
  formatDateRange,
  executeParameterizedQuery,
  QueryTemplateParams,
  validateRequiredParams,
  EnhancedQueryFunction,
} from "../helpers";

export interface RecommendationPerformance {
  nsite_id: string;
  recommender_name: string;
  recommender_views_count: number;
  product_views_count: number;
  clicks_count: number;
  add_to_cart_count: number;
  product_purchased_count: number;
  order_count: number;
  std_attributed_revenue: number;
  ctr: number;
  atc_rate: number;
  conversion_rate: number;
}

interface RecommendationByAlgorithmParams extends SiteSpecificQueryTemplateParams {
  recommenderName: string;
}

/**
 * Query recommendation algorithm performance to optimize personalization strategy
 * Business Question: How effective are our product recommendations and personalization?
 * Primary users: Merchandising and Personalization teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param recommenderName Filter by specific recommendation algorithm
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryRecommendationPerformanceByAlgorithm: EnhancedQueryFunction<
  RecommendationPerformance,
  RecommendationByAlgorithmParams
> = function queryRecommendationPerformanceByAlgorithm(
  client: CIPClient,
  params: RecommendationByAlgorithmParams,
  batchSize: number = 100,
): AsyncGenerator<RecommendationPerformance[], void, unknown> {
  const { sql, parameters } = queryRecommendationPerformanceByAlgorithm.QUERY(params);
  return executeParameterizedQuery<RecommendationPerformance>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryRecommendationPerformanceByAlgorithm.metadata = {
  name: "recommendation-performance-by-algorithm",
  description:
    "Optimize personalization strategy through algorithm performance analysis",
  category: "Recommendation Analytics",
  requiredParams: ["siteId", "recommenderName", "from", "to"],
};

queryRecommendationPerformanceByAlgorithm.QUERY = (
  params: RecommendationByAlgorithmParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange", "recommenderName"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    SELECT 
      s.nsite_id,
      rpr.recommender_name,
      SUM(rpr.num_recommender_views) AS recommender_views_count,
      SUM(rpr.num_product_views) AS product_views_count,
      SUM(rpr.num_clicks) AS clicks_count,
      SUM(rpr.num_cart_adds) AS add_to_cart_count,
      SUM(rpr.num_products_purchased) AS product_purchased_count,
      SUM(rpr.num_orders) AS order_count,
      SUM(rpr.std_attributed_revenue) AS std_attributed_revenue,
      CASE SUM(rpr.num_recommender_views)
        WHEN 0 THEN 0
        ELSE CAST(SUM(rpr.num_clicks) AS FLOAT) / SUM(rpr.num_recommender_views) 
      END AS ctr,
      CASE SUM(rpr.num_clicks)
        WHEN 0 THEN 0
        ELSE CAST(SUM(rpr.num_cart_adds) AS FLOAT) / SUM(rpr.num_clicks) 
      END AS atc_rate,
      CASE SUM(rpr.num_cart_adds)
        WHEN 0 THEN 0
        ELSE CAST(SUM(rpr.num_products_purchased) AS FLOAT) / SUM(rpr.num_cart_adds) 
      END AS conversion_rate
    FROM (
      SELECT 
        site_id, recommender_name, recommendation_date,
        num_recommender_views, num_product_views, num_clicks,
        num_cart_adds, num_products_purchased, num_orders, std_attributed_revenue
      FROM ccdw_aggr_product_recommendation_recommender
    ) rpr
    JOIN (
      SELECT site_id, nsite_id
      FROM ccdw_dim_site
    ) s ON s.site_id = rpr.site_id
    WHERE rpr.recommendation_date >= DATE '${startDate}'
      AND rpr.recommendation_date <= DATE '${endDate}'
      AND s.nsite_id = '${params.siteId}'
      AND rpr.recommender_name = '${params.recommenderName}'
    GROUP BY s.nsite_id, rpr.recommender_name
    ORDER BY std_attributed_revenue DESC
  `;

  return {
    sql,
    parameters: [],
  };
};

export interface OverallRecommendationPerformance {
  recommender_name: string;
  total_revenue: number;
  total_orders: number;
  total_clicks: number;
  total_views: number;
  overall_ctr: number;
  overall_conversion_rate: number;
  avg_order_value: number;
}

/**
 * Query overall recommendation performance across all algorithms
 * Business Question: Which recommendation algorithms should we invest in?
 * Primary users: Product and Engineering teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryOverallRecommendationPerformance: EnhancedQueryFunction<
  OverallRecommendationPerformance,
  SiteSpecificQueryTemplateParams
> = async function* queryOverallRecommendationPerformance(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<OverallRecommendationPerformance[], void, unknown> {
  const { sql, parameters } = queryOverallRecommendationPerformance.QUERY(params);
  yield* executeParameterizedQuery<OverallRecommendationPerformance>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryOverallRecommendationPerformance.metadata = {
  name: "overall-recommendation-performance",
  description: "Compare and optimize recommendation algorithm investments",
  category: "Recommendation Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryOverallRecommendationPerformance.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    SELECT
      pr.recommender_name,
      SUM(pr.std_attributed_revenue) as total_revenue,
      SUM(pr.num_orders) as total_orders,
      SUM(pr.num_clicks) as total_clicks,
      SUM(pr.num_recommender_views) as total_views,
      CASE WHEN SUM(pr.num_recommender_views) > 0
           THEN (CAST(SUM(pr.num_clicks) AS FLOAT) / SUM(pr.num_recommender_views)) * 100
           ELSE 0
      END as overall_ctr,
      CASE WHEN SUM(pr.num_clicks) > 0
           THEN (CAST(SUM(pr.num_orders) AS FLOAT) / SUM(pr.num_clicks)) * 100
           ELSE 0
      END as overall_conversion_rate,
      CASE WHEN SUM(pr.num_orders) > 0
           THEN SUM(pr.std_attributed_revenue) / SUM(pr.num_orders)
           ELSE 0
      END as avg_order_value
    FROM ccdw_aggr_product_recommendation pr
    JOIN ccdw_dim_site s ON s.site_id = pr.site_id
    WHERE pr.recommendation_date >= '${startDate}'
      AND pr.recommendation_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY pr.recommender_name
    ORDER BY total_revenue DESC
  `;

  return {
    sql,
    parameters: [],
  };
};

export interface RecommendationWidgetPlacement {
  widget_location: string;
  widget_type: string;
  total_views: number;
  total_clicks: number;
  total_revenue: number;
  location_ctr: number;
  revenue_per_view: number;
}

/**
 * Query recommendation widget placement effectiveness
 * Business Question: Which page locations work best for recommendations?
 * Primary users: UX and Merchandising teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryRecommendationWidgetPlacement: EnhancedQueryFunction<
  RecommendationWidgetPlacement,
  SiteSpecificQueryTemplateParams
> = async function* queryRecommendationWidgetPlacement(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<RecommendationWidgetPlacement[], void, unknown> {
  const { sql, parameters } = queryRecommendationWidgetPlacement.QUERY(params);
  yield* executeParameterizedQuery<RecommendationWidgetPlacement>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryRecommendationWidgetPlacement.metadata = {
  name: "recommendation-widget-placement",
  description: "Optimize recommendation widget locations and types",
  category: "Recommendation Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryRecommendationWidgetPlacement.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    SELECT
      pr.widget_location,
      pr.widget_type,
      SUM(pr.num_recommender_views) as total_views,
      SUM(pr.num_clicks) as total_clicks,
      SUM(pr.std_attributed_revenue) as total_revenue,
      CASE WHEN SUM(pr.num_recommender_views) > 0
           THEN (CAST(SUM(pr.num_clicks) AS FLOAT) / SUM(pr.num_recommender_views)) * 100
           ELSE 0
      END as location_ctr,
      CASE WHEN SUM(pr.num_recommender_views) > 0
           THEN SUM(pr.std_attributed_revenue) / SUM(pr.num_recommender_views)
           ELSE 0
      END as revenue_per_view
    FROM ccdw_aggr_product_recommendation pr
    JOIN ccdw_dim_site s ON s.site_id = pr.site_id
    WHERE pr.recommendation_date >= '${startDate}'
      AND pr.recommendation_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
      AND pr.widget_location IS NOT NULL
      AND pr.widget_type IS NOT NULL
    GROUP BY pr.widget_location, pr.widget_type
    ORDER BY total_revenue DESC
  `;

  return {
    sql,
    parameters: [],
  };
};