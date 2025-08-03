import { CIPClient } from "../../cip-client";
import { DateRange, cleanSQL } from "../types";
import {
  formatDateRange,
  executeParameterizedQuery,
  QueryTemplateParams,
  validateRequiredParams,
  EnhancedQueryFunction,
} from "../helpers";

export interface PromotionDiscountAnalysis {
  submit_day: Date | string;
  total_orders: number;
  promotion_class: string;
  std_total_discount: number;
  promotion_orders: number;
  avg_discount_per_order: number;
}

/**
 * Query promotion discount analysis to measure incremental impact
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
    "Measure promotional impact on sales and identify incremental revenue",
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