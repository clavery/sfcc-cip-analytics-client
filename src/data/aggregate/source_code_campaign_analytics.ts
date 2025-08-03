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

export interface SourceCodeActivation {
  site: string;
  group_id: string;
  status: string;
  activations: number;
}

export interface SourceCodeSalesPerformance {
  site: string;
  group_id: string;
  status: string;
  orders: number;
  units: number;
  std_revenue: number;
}

export interface CampaignROIAnalysis {
  group_id: string;
  campaign_name: string;
  total_activations: number;
  total_orders: number;
  conversion_rate: number;
  std_revenue: number;
  revenue_per_activation: number;
}

interface SourceCodeActivationParams extends SiteSpecificQueryTemplateParams {
  deviceClassCode: string;
  registered: boolean;
}

/**
 * Query source code activations to measure campaign traffic volume
 * Business Question: Which marketing campaigns are driving actual sales vs just traffic?
 * Primary users: Marketing and Campaign Management teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param deviceClassCode Device type filter (e.g., 'mobile', 'desktop')
 * @param registered Whether to include registered customers only
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const querySourceCodeActivations: EnhancedQueryFunction<
  SourceCodeActivation,
  SourceCodeActivationParams
> = function querySourceCodeActivations(
  client: CIPClient,
  params: SourceCodeActivationParams,
  batchSize: number = 100,
): AsyncGenerator<SourceCodeActivation[], void, unknown> {
  const { sql, parameters } = querySourceCodeActivations.QUERY(params);
  return executeParameterizedQuery<SourceCodeActivation>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

querySourceCodeActivations.metadata = {
  name: "source-code-activations",
  description:
    "Measure campaign traffic volume and activation patterns",
  category: "Campaign Analytics",
  requiredParams: ["siteId", "deviceClassCode", "registered", "from", "to"],
};

querySourceCodeActivations.QUERY = (
  params: SourceCodeActivationParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange", "deviceClassCode", "registered"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    SELECT
      s.nsite_id AS site,
      g.nsource_code_group_id AS group_id,
      CASE
        WHEN sca.source_code_status = '0' THEN 'ACTIVE'
        WHEN sca.source_code_status = '1' THEN 'INACTIVE'
        WHEN sca.source_code_status = '2' THEN 'INVALID'
        ELSE sca.source_code_status
      END AS status,
      SUM(sca.num_activations) AS activations
    FROM ccdw_aggr_source_code_activation sca
    JOIN ccdw_dim_site s
      ON s.site_id = sca.site_id
    JOIN ccdw_dim_source_code_group g
      ON g.source_code_group_id = sca.source_code_group_id
    WHERE sca.activation_date >= '${startDate}'
      AND sca.activation_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
      AND sca.device_class_code = '${params.deviceClassCode}'
      AND sca.registered = ${params.registered}
    GROUP BY
      s.nsite_id,
      g.nsource_code_group_id,
      sca.source_code_status
    ORDER BY
      s.nsite_id ASC,
      g.nsource_code_group_id ASC
  `;

  return {
    sql,
    parameters: [],
  };
};

interface SourceCodeSalesParams extends SiteSpecificQueryTemplateParams {
  deviceClassCode: string;
}

/**
 * Query source code sales performance for ROI analysis
 * Business Question: What actual sales results are attributed to marketing campaigns?
 * Primary users: Marketing teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param deviceClassCode Device type filter (e.g., 'mobile', 'desktop')
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const querySourceCodeSalesPerformance: EnhancedQueryFunction<
  SourceCodeSalesPerformance,
  SourceCodeSalesParams
> = async function* querySourceCodeSalesPerformance(
  client: CIPClient,
  params: SourceCodeSalesParams,
  batchSize: number = 100,
): AsyncGenerator<SourceCodeSalesPerformance[], void, unknown> {
  const { sql, parameters } = querySourceCodeSalesPerformance.QUERY(params);
  yield* executeParameterizedQuery<SourceCodeSalesPerformance>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

querySourceCodeSalesPerformance.metadata = {
  name: "source-code-sales-performance",
  description: "Analyze sales performance attributed to marketing campaigns",
  category: "Campaign Analytics",
  requiredParams: ["siteId", "deviceClassCode", "from", "to"],
};

querySourceCodeSalesPerformance.QUERY = (
  params: SourceCodeSalesParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange", "deviceClassCode"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    SELECT
      s.nsite_id AS site,
      g.nsource_code_group_id AS group_id,
      CASE
        WHEN sco.source_code_status = '0' THEN 'ACTIVE'
        WHEN sco.source_code_status = '1' THEN 'INACTIVE'
        WHEN sco.source_code_status = '2' THEN 'INVALID'
        ELSE sco.source_code_status
      END AS status,
      SUM(sco.num_orders) AS orders,
      SUM(sco.num_units) AS units,
      SUM(sco.std_revenue) AS std_revenue
    FROM ccdw_aggr_source_code_sales sco
    JOIN ccdw_dim_site s
      ON s.site_id = sco.site_id
    JOIN ccdw_dim_source_code_group g
      ON g.source_code_group_id = sco.source_code_group_id
    WHERE sco.submit_date >= '${startDate}'
      AND sco.submit_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
      AND sco.device_class_code = '${params.deviceClassCode}'
      AND sco.registered = TRUE
    GROUP BY
      s.nsite_id,
      g.nsource_code_group_id,
      sco.source_code_status
    ORDER BY
      s.nsite_id ASC,
      g.nsource_code_group_id ASC
  `;

  return {
    sql,
    parameters: [],
  };
};

/**
 * Query campaign ROI analysis combining activations and sales data
 * Business Question: What is the conversion rate and ROI of each campaign?
 * Primary users: Marketing managers
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryCampaignROIAnalysis: EnhancedQueryFunction<
  CampaignROIAnalysis,
  SiteSpecificQueryTemplateParams
> = async function* queryCampaignROIAnalysis(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<CampaignROIAnalysis[], void, unknown> {
  const { sql, parameters } = queryCampaignROIAnalysis.QUERY(params);
  yield* executeParameterizedQuery<CampaignROIAnalysis>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryCampaignROIAnalysis.metadata = {
  name: "campaign-roi-analysis",
  description: "Analyze campaign ROI combining activations and sales data",
  category: "Campaign Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryCampaignROIAnalysis.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    WITH activations AS (
      SELECT
        g.nsource_code_group_id,
        g.display_name as campaign_name,
        SUM(sca.num_activations) as total_activations
      FROM ccdw_aggr_source_code_activation sca
      JOIN ccdw_dim_source_code_group g ON g.source_code_group_id = sca.source_code_group_id
      JOIN ccdw_dim_site s ON s.site_id = sca.site_id
      WHERE sca.activation_date >= '${startDate}'
        AND sca.activation_date <= '${endDate}'
        AND s.nsite_id = '${params.siteId}'
        AND sca.source_code_status = '0'
      GROUP BY g.nsource_code_group_id, g.display_name
    ),
    sales AS (
      SELECT
        g.nsource_code_group_id,
        SUM(sco.num_orders) as total_orders,
        SUM(sco.std_revenue) as std_revenue
      FROM ccdw_aggr_source_code_sales sco
      JOIN ccdw_dim_source_code_group g ON g.source_code_group_id = sco.source_code_group_id
      JOIN ccdw_dim_site s ON s.site_id = sco.site_id
      WHERE sco.submit_date >= '${startDate}'
        AND sco.submit_date <= '${endDate}'
        AND s.nsite_id = '${params.siteId}'
        AND sco.source_code_status = '0'
      GROUP BY g.nsource_code_group_id
    )
    SELECT
      a.nsource_code_group_id as group_id,
      a.campaign_name,
      COALESCE(a.total_activations, 0) as total_activations,
      COALESCE(s.total_orders, 0) as total_orders,
      CASE WHEN a.total_activations > 0
           THEN (CAST(COALESCE(s.total_orders, 0) AS FLOAT) / a.total_activations) * 100
           ELSE 0
      END as conversion_rate,
      COALESCE(s.std_revenue, 0) as std_revenue,
      CASE WHEN a.total_activations > 0
           THEN COALESCE(s.std_revenue, 0) / a.total_activations
           ELSE 0
      END as revenue_per_activation
    FROM activations a
    FULL OUTER JOIN sales s ON a.nsource_code_group_id = s.nsource_code_group_id
    ORDER BY std_revenue DESC
  `;

  return {
    sql,
    parameters: [],
  };
};