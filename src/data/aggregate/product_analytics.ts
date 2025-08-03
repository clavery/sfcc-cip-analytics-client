import { CIPClient } from "../../cip-client";
import { DateRange, cleanSQL } from "../types";
import {
  SiteSpecificQueryTemplateParams,
  formatDateRange,
  executeParameterizedQuery,
  validateRequiredParams,
  EnhancedQueryFunction,
} from "../helpers";

export interface TopSellingProduct {
  nproduct_id: string;
  product_display_name: string;
  units_sold: number;
  std_revenue: number;
  order_count: number;
  device_class_code: string;
  registered: boolean;
  nsite_id: string;
}

export interface ProductCoPurchase {
  product_1_id: string;
  product_1_name: string;
  product_2_id: string;
  product_2_name: string;
  co_purchase_count: number;
  std_cobuy_revenue: number;
}

/**
 * Query top selling products by performance dimensions
 * Business Question: How do my products perform across different channels?
 * Primary users: Buyers and Merchandising teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryTopSellingProducts: EnhancedQueryFunction<
  TopSellingProduct,
  SiteSpecificQueryTemplateParams
> = function queryTopSellingProducts(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<TopSellingProduct[], void, unknown> {
  const { sql, parameters } = queryTopSellingProducts.QUERY(params);
  return executeParameterizedQuery<TopSellingProduct>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryTopSellingProducts.metadata = {
  name: "top-selling-products",
  description: "Analyze product performance across different channels and dimensions",
  category: "Product Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryTopSellingProducts.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    SELECT
      p.nproduct_id,
      p.product_display_name,
      SUM(pss.num_units) as units_sold,
      SUM(pss.std_revenue) as std_revenue,
      SUM(pss.num_orders) as order_count,
      pss.device_class_code,
      pss.registered,
      s.nsite_id
    FROM ccdw_aggr_product_sales_summary pss
    JOIN ccdw_dim_product p ON p.product_id = pss.product_id
    JOIN ccdw_dim_site s ON s.site_id = pss.site_id
    WHERE pss.submit_date >= '${startDate}' AND pss.submit_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY p.nproduct_id, p.product_display_name, pss.device_class_code, pss.registered, s.nsite_id
    ORDER BY std_revenue DESC
  `;

  return {
    sql,
    parameters: [],
  };
};

/**
 * Query product co-purchase analysis for cross-sell optimization
 * Business Question: Which products are frequently bought together?
 * Primary users: Merchandising and Product teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryProductCoPurchaseAnalysis: EnhancedQueryFunction<
  ProductCoPurchase,
  SiteSpecificQueryTemplateParams
> = async function* queryProductCoPurchaseAnalysis(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<ProductCoPurchase[], void, unknown> {
  const { sql, parameters } = queryProductCoPurchaseAnalysis.QUERY(params);
  yield* executeParameterizedQuery<ProductCoPurchase>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryProductCoPurchaseAnalysis.metadata = {
  name: "product-co-purchase-analysis",
  description: "Analyze frequently co-purchased products for cross-sell optimization",
  category: "Product Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryProductCoPurchaseAnalysis.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    SELECT
      p1.nproduct_id as product_1_id,
      p1.product_display_name as product_1_name,
      p2.nproduct_id as product_2_id,
      p2.product_display_name as product_2_name,
      pcb.frequency_count as co_purchase_count,
      pcb.std_cobuy_revenue
    FROM ccdw_aggr_product_cobuy pcb
    JOIN ccdw_dim_product p1 ON p1.product_id = pcb.product_one_id
    JOIN ccdw_dim_product p2 ON p2.product_id = pcb.product_two_id
    JOIN ccdw_dim_site s ON s.nsite_id = pcb.nsite_id
    WHERE pcb.submit_date >= '${startDate}' AND pcb.submit_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    ORDER BY pcb.frequency_count DESC
  `;

  return {
    sql,
    parameters: [],
  };
};