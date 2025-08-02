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

export interface ProductSalesSummaryRecord {
  submit_date: Date | string;
  site_id: number;
  product_id: number;
  device_class_code: string;
  registered: boolean;
  num_orders: number;
  num_units: number;
  std_revenue: number;
  std_subtotal: number;
  std_discount: number;
  locale_id: number;
  channel_id: number;
}

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
 * Query top selling products across different dimensions
 * Business Question: How do my products perform across different channels and devices?
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
  description:
    "Analyze product performance across different channels and devices",
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
 * Query product co-purchase patterns for cross-sell opportunities
 * Business Question: Which products are frequently bought together?
 * Primary users: Merchandising teams
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
  name: "product-copurchase-analysis",
  description: "Identify cross-sell opportunities through co-purchase patterns",
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

export interface ProductPerformanceByDimension {
  dimension_value: string;
  units_sold: number;
  std_revenue: number;
  order_count: number;
  avg_unit_price: number;
}

interface ProductPerformanceQueryParams extends QueryTemplateParams {
  productId: string;
  dimension: 'device' | 'site' | 'customer_type';
}

/**
 * Query product performance by specific dimensions
 * @param client The Avatica client instance (must have an open connection)
 * @param productId The natural product ID to analyze
 * @param dateRange Date range to filter results
 * @param dimension The dimension to group by ('device', 'site', 'customer_type')
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryProductPerformanceByDimension: EnhancedQueryFunction<
  ProductPerformanceByDimension,
  ProductPerformanceQueryParams
> = async function* queryProductPerformanceByDimension(
  client: CIPClient,
  params: ProductPerformanceQueryParams,
  batchSize: number = 100,
): AsyncGenerator<ProductPerformanceByDimension[], void, unknown> {
  const { sql, parameters } = queryProductPerformanceByDimension.QUERY(params);
  yield* executeParameterizedQuery<ProductPerformanceByDimension>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryProductPerformanceByDimension.metadata = {
  name: "product-performance-by-dimension",
  description: "Analyze product performance across specific dimensions",
  category: "Product Analytics",
  requiredParams: ["productId", "dimension", "from", "to"],
};

queryProductPerformanceByDimension.QUERY = (
  params: ProductPerformanceQueryParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["dateRange", "productId", "dimension"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);
  
  let groupByColumn: string;
  let selectColumn: string;
  
  switch (params.dimension) {
    case 'device':
      groupByColumn = 'pss.device_class_code';
      selectColumn = 'pss.device_class_code AS dimension_value';
      break;
    case 'site':
      groupByColumn = 's.nsite_id';
      selectColumn = 's.nsite_id AS dimension_value';
      break;
    case 'customer_type':
      groupByColumn = 'pss.registered';
      selectColumn = "CASE WHEN pss.registered THEN 'Registered' ELSE 'Guest' END AS dimension_value";
      break;
  }
  
  const sql = `
    SELECT
      ${selectColumn},
      SUM(pss.num_units) as units_sold,
      SUM(pss.std_revenue) as std_revenue,
      SUM(pss.num_orders) as order_count,
      SUM(pss.std_revenue) / NULLIF(SUM(pss.num_units), 0) as avg_unit_price
    FROM ccdw_aggr_product_sales_summary pss
    JOIN ccdw_dim_product p ON p.product_id = pss.product_id
    JOIN ccdw_dim_site s ON s.site_id = pss.site_id
    WHERE pss.submit_date >= '${startDate}' AND pss.submit_date <= '${endDate}'
      AND p.nproduct_id = '${params.productId}'
    GROUP BY ${groupByColumn}
    ORDER BY std_revenue DESC
  `;

  return {
    sql,
    parameters: [],
  };
};

interface ProductSalesQueryParams extends QueryTemplateParams {
  siteId?: string;
  productId?: string;
  deviceClassCode?: string;
  registered?: boolean;
}

/**
 * Query raw product sales summary data
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results
 * @param filters Optional filters for site, product, device, registration status
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryProductSalesSummary: EnhancedQueryFunction<
  ProductSalesSummaryRecord,
  ProductSalesQueryParams
> = async function* queryProductSalesSummary(
  client: CIPClient,
  params: ProductSalesQueryParams,
  batchSize: number = 100,
): AsyncGenerator<ProductSalesSummaryRecord[], void, unknown> {
  // Ensure dateRange has a default if not provided
  const queryParams: ProductSalesQueryParams = {
    ...params,
    dateRange: params.dateRange || { startDate: new Date(0), endDate: new Date() },
  };
  const { sql, parameters } = queryProductSalesSummary.QUERY(queryParams);
  yield* executeParameterizedQuery<ProductSalesSummaryRecord>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryProductSalesSummary.metadata = {
  name: "product-sales-summary",
  description: "Query raw product sales summary data for custom analysis",
  category: "Product Analytics",
  requiredParams: [],
  optionalParams: ["siteId", "productId", "deviceClassCode", "registered", "from", "to"],
};

queryProductSalesSummary.QUERY = (
  params: ProductSalesQueryParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["dateRange"]);

  let sql = "SELECT pss.* FROM ccdw_aggr_product_sales_summary pss";
  const joins: string[] = [];
  const conditions: string[] = [];

  if (params.dateRange) {
    const { startDate, endDate } = formatDateRange(params.dateRange);
    conditions.push(`pss.submit_date >= '${startDate}' AND pss.submit_date <= '${endDate}'`);
  }

  if (params.siteId) {
    joins.push("JOIN ccdw_dim_site s ON s.site_id = pss.site_id");
    conditions.push(`s.nsite_id = '${params.siteId}'`);
  }

  if (params.productId) {
    joins.push("JOIN ccdw_dim_product p ON p.product_id = pss.product_id");
    conditions.push(`p.nproduct_id = '${params.productId}'`);
  }

  if (params.deviceClassCode) {
    conditions.push(`pss.device_class_code = '${params.deviceClassCode}'`);
  }

  if (params.registered !== undefined) {
    conditions.push(`pss.registered = ${params.registered}`);
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