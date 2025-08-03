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


export interface PaymentMethodPerformance {
  payment_method: string;
  total_payments: number;
  orders_with_payment: number;
  std_captured_amount: number;
  std_refunded_amount: number;
  std_transaction_amount: number;
  avg_payment_amount: number;
}


/**
 * Query payment method performance metrics to track adoption and optimize processing costs
 * Business Question: Which payment methods are customers using and how do they perform?
 * Primary users: Operations teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryPaymentMethodPerformance: EnhancedQueryFunction<
  PaymentMethodPerformance,
  SiteSpecificQueryTemplateParams
> = function queryPaymentMethodPerformance(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<PaymentMethodPerformance[], void, unknown> {
  const { sql, parameters } = queryPaymentMethodPerformance.QUERY(params);
  return executeParameterizedQuery<PaymentMethodPerformance>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryPaymentMethodPerformance.metadata = {
  name: "payment-method-performance",
  description:
    "Track payment method adoption and performance metrics",
  category: "Payment Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryPaymentMethodPerformance.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    SELECT
      pm.display_name AS payment_method,
      SUM(pss.num_payments) AS total_payments,
      SUM(pss.num_orders) AS orders_with_payment,
      SUM(pss.std_captured_amount) AS std_captured_amount,
      SUM(pss.std_refunded_amount) AS std_refunded_amount,
      SUM(pss.std_transaction_amount) AS std_transaction_amount,
      (SUM(pss.std_captured_amount) / SUM(pss.num_payments)) AS avg_payment_amount
    FROM ccdw_aggr_payment_sales_summary pss
    JOIN ccdw_dim_payment_method pm ON pm.payment_method_id = pss.payment_method_id
    JOIN ccdw_dim_site s ON s.site_id = pss.site_id
    WHERE pss.submit_date >= '${startDate}' AND pss.submit_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY pm.display_name
    ORDER BY std_captured_amount DESC
  `;

  return {
    sql,
    parameters: [],
  };
};


