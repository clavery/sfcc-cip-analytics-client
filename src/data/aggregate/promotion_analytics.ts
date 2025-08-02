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

export interface PromotionSalesSummaryRecord {
  submit_date: Date | string;
  site_id: number;
  promotion_id: number;
  device_class_code: string;
  registered: boolean;
  num_orders: number;
  num_units: number;
  std_revenue: number;
  std_total_discount: number;
  std_order_discount: number;
  std_product_discount: number;
  std_shipping_discount: number;
  locale_id: number;
  channel_id: number;
}

export interface PromotionDiscountAnalysis {
  submit_day: Date | string;
  total_orders: number;
  promotion_class: string;
  std_total_discount: number;
  promotion_orders: number;
  avg_discount_per_order: number;
}

/**
 * Query promotion effectiveness with discount analysis
 * Business Question: Are my promotions driving incremental sales or just discounting existing sales?
 * Primary users: Marketing and Merchandising teams
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryPromotionDiscountAnalysis: EnhancedQueryFunction<
  PromotionDiscountAnalysis,
  QueryTemplateParams
> = function queryPromotionDiscountAnalysis(
  client: CIPClient,
  params: QueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<PromotionDiscountAnalysis[], void, unknown> {
  const { sql, parameters } = queryPromotionDiscountAnalysis.QUERY(params);
  return executeParameterizedQuery<PromotionDiscountAnalysis>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryPromotionDiscountAnalysis.metadata = {
  name: "promotion-discount-analysis",
  description:
    "Analyze promotion effectiveness and discount impact on sales",
  category: "Promotion Analytics",
  requiredParams: ["from", "to"],
};

queryPromotionDiscountAnalysis.QUERY = (
  params: QueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    WITH TOTAL_ORDERS AS (
      SELECT
        ss.submit_date AS submit_day,
        SUM(num_orders) AS total_orders
      FROM ccdw_aggr_sales_summary ss
      WHERE ss.submit_date >= '${startDate}'
        AND ss.submit_date <= '${endDate}'
      GROUP BY ss.submit_date
    ),
    PROMOTION_DISCOUNT AS (
      SELECT
        pss.submit_date AS submit_day,
        p.promotion_class AS promotion_class,
        SUM(std_total_discount) AS std_total_discount,
        SUM(num_orders) AS promotion_orders
      FROM ccdw_aggr_promotion_sales_summary pss
      JOIN ccdw_dim_promotion p
        ON p.promotion_id = pss.promotion_id
      WHERE pss.submit_date >= '${startDate}'
        AND pss.submit_date <= '${endDate}'
      GROUP BY pss.submit_date, p.promotion_class
    )
    SELECT
      t.submit_day,
      t.total_orders,
      p.promotion_class,
      p.std_total_discount,
      p.promotion_orders,
      p.std_total_discount / p.promotion_orders AS avg_discount_per_order
    FROM TOTAL_ORDERS t
    LEFT JOIN PROMOTION_DISCOUNT p
      ON t.submit_day = p.submit_day
  `;

  return {
    sql,
    parameters: [],
  };
};

export interface PromotionPerformanceByType {
  promotion_class: string;
  promotion_name: string;
  total_orders: number;
  total_units: number;
  std_revenue: number;
  std_total_discount: number;
  avg_discount_percentage: number;
}

/**
 * Query promotion performance by type
 * Business Question: Which promotion types (product/order/shipping) perform best?
 * Primary users: Marketing teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryPromotionPerformanceByType: EnhancedQueryFunction<
  PromotionPerformanceByType,
  SiteSpecificQueryTemplateParams
> = async function* queryPromotionPerformanceByType(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<PromotionPerformanceByType[], void, unknown> {
  const { sql, parameters } = queryPromotionPerformanceByType.QUERY(params);
  yield* executeParameterizedQuery<PromotionPerformanceByType>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryPromotionPerformanceByType.metadata = {
  name: "promotion-performance-by-type",
  description: "Compare performance across different promotion types",
  category: "Promotion Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryPromotionPerformanceByType.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    SELECT
      p.promotion_class,
      p.npromotion_id as promotion_name,
      SUM(pss.num_orders) as total_orders,
      SUM(pss.num_units) as total_units,
      SUM(pss.std_revenue) as std_revenue,
      SUM(pss.std_total_discount) as std_total_discount,
      CASE WHEN SUM(pss.std_revenue) > 0
           THEN (SUM(pss.std_total_discount) / SUM(pss.std_revenue)) * 100
           ELSE 0
      END as avg_discount_percentage
    FROM ccdw_aggr_promotion_sales_summary pss
    JOIN ccdw_dim_promotion p ON p.promotion_id = pss.promotion_id
    JOIN ccdw_dim_site s ON s.site_id = pss.site_id
    WHERE pss.submit_date >= '${startDate}'
      AND pss.submit_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY p.promotion_class, p.npromotion_id
    ORDER BY std_revenue DESC
  `;

  return {
    sql,
    parameters: [],
  };
};

interface PromotionSalesQueryParams extends QueryTemplateParams {
  siteId?: string;
  promotionId?: string;
  promotionClass?: string;
  deviceClassCode?: string;
  registered?: boolean;
}

/**
 * Query raw promotion sales summary data
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results
 * @param filters Optional filters for site, promotion, device, registration status
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryPromotionSalesSummary: EnhancedQueryFunction<
  PromotionSalesSummaryRecord,
  PromotionSalesQueryParams
> = async function* queryPromotionSalesSummary(
  client: CIPClient,
  params: PromotionSalesQueryParams,
  batchSize: number = 100,
): AsyncGenerator<PromotionSalesSummaryRecord[], void, unknown> {
  // Ensure dateRange has a default if not provided
  const queryParams: PromotionSalesQueryParams = {
    ...params,
    dateRange: params.dateRange || { startDate: new Date(0), endDate: new Date() },
  };
  const { sql, parameters } = queryPromotionSalesSummary.QUERY(queryParams);
  yield* executeParameterizedQuery<PromotionSalesSummaryRecord>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryPromotionSalesSummary.metadata = {
  name: "promotion-sales-summary",
  description: "Query raw promotion sales summary data for custom analysis",
  category: "Promotion Analytics",
  requiredParams: [],
  optionalParams: ["siteId", "promotionId", "promotionClass", "deviceClassCode", "registered", "from", "to"],
};

queryPromotionSalesSummary.QUERY = (
  params: PromotionSalesQueryParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["dateRange"]);

  let sql = "SELECT pss.* FROM ccdw_aggr_promotion_sales_summary pss";
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

  if (params.promotionId || params.promotionClass) {
    joins.push("JOIN ccdw_dim_promotion p ON p.promotion_id = pss.promotion_id");
    if (params.promotionId) {
      conditions.push(`p.npromotion_id = '${params.promotionId}'`);
    }
    if (params.promotionClass) {
      conditions.push(`p.promotion_class = '${params.promotionClass}'`);
    }
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