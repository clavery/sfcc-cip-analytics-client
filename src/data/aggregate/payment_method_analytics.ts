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

export interface PaymentSalesSummaryRecord {
  submit_date: Date | string;
  site_id: number;
  payment_method_id: number;
  device_class_code: string;
  registered: boolean;
  num_payments: number;
  num_orders: number;
  std_captured_amount: number;
  std_refunded_amount: number;
  std_transaction_amount: number;
  locale_id: number;
  channel_id: number;
}

export interface PaymentMethodPerformance {
  payment_method: string;
  total_payments: number;
  orders_with_payment: number;
  std_captured_amount: number;
  std_refunded_amount: number;
  std_transaction_amount: number;
  avg_payment_amount: number;
}

export interface GiftCertificateAnalytics {
  date: Date | string;
  site: string;
  redemption_count: number;
  std_redemption_value: number;
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

/**
 * Query gift certificate redemption patterns to optimize gift certificate marketing campaigns
 * Business Question: How are gift certificates being redeemed and what is their value?
 * Primary users: Operations and Marketing teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryGiftCertificateAnalytics: EnhancedQueryFunction<
  GiftCertificateAnalytics,
  SiteSpecificQueryTemplateParams
> = async function* queryGiftCertificateAnalytics(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<GiftCertificateAnalytics[], void, unknown> {
  const { sql, parameters } = queryGiftCertificateAnalytics.QUERY(params);
  yield* executeParameterizedQuery<GiftCertificateAnalytics>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryGiftCertificateAnalytics.metadata = {
  name: "gift-certificate-analytics",
  description: "Analyze gift certificate redemption patterns and value",
  category: "Payment Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryGiftCertificateAnalytics.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    SELECT
      pss.submit_date AS "date",
      s.nsite_id AS site,
      SUM(pss.num_orders) AS redemption_count,
      SUM(pss.std_transaction_amount) AS std_redemption_value
    FROM ccdw_aggr_payment_sales_summary pss
    JOIN ccdw_dim_site s
      ON s.site_id = pss.site_id
    JOIN ccdw_dim_payment_method pm
      ON pm.payment_method_id = pss.payment_method_id
    WHERE pss.submit_date >= '${startDate}'
      AND pss.submit_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
      AND pm.npayment_method_id = 'GIFT_CERTIFICATE'
    GROUP BY
      pss.submit_date,
      s.nsite_id
    ORDER BY
      pss.submit_date ASC,
      s.nsite_id ASC
  `;

  return {
    sql,
    parameters: [],
  };
};

interface PaymentSalesQueryParams extends QueryTemplateParams {
  siteId?: string;
  paymentMethodId?: string;
  deviceClassCode?: string;
  registered?: boolean;
}

/**
 * Query raw payment sales summary data for custom analysis
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results
 * @param filters Optional filters for site, payment method, device, registration status
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryPaymentSalesSummary: EnhancedQueryFunction<
  PaymentSalesSummaryRecord,
  PaymentSalesQueryParams
> = async function* queryPaymentSalesSummary(
  client: CIPClient,
  params: PaymentSalesQueryParams,
  batchSize: number = 100,
): AsyncGenerator<PaymentSalesSummaryRecord[], void, unknown> {
  // Ensure dateRange has a default if not provided
  const queryParams: PaymentSalesQueryParams = {
    ...params,
    dateRange: params.dateRange || { startDate: new Date(0), endDate: new Date() },
  };
  const { sql, parameters } = queryPaymentSalesSummary.QUERY(queryParams);
  yield* executeParameterizedQuery<PaymentSalesSummaryRecord>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryPaymentSalesSummary.metadata = {
  name: "payment-sales-summary-raw",
  description: "Query raw payment sales summary data for custom analysis",
  category: "Payment Analytics",
  requiredParams: [],
  optionalParams: ["siteId", "paymentMethodId", "deviceClassCode", "registered", "from", "to"],
};

queryPaymentSalesSummary.QUERY = (
  params: PaymentSalesQueryParams,
): { sql: string; parameters: any[] } => {
  // dateRange is required in the params structure, even if it was optional in the function signature
  validateRequiredParams(params, ["dateRange"]);

  let sql = "SELECT pss.* FROM ccdw_aggr_payment_sales_summary pss";
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

  if (params.paymentMethodId) {
    joins.push("JOIN ccdw_dim_payment_method pm ON pm.payment_method_id = pss.payment_method_id");
    conditions.push(`pm.npayment_method_id = '${params.paymentMethodId}'`);
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