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

export interface RegistrationRecord {
  registration_date: Date;
  site_id: number;
  device_class_code: string;
  num_registrations: number;
  locale_id: number;
  channel_id: number;
}

export interface CustomerRegistrationTrends {
  date: Date;
  new_registrations: number;
  device_class_code: string;
  nsite_id: string;
}

export interface CustomerListSnapshot {
  snapshot_date: Date;
  total_customers: number;
}

/**
 * Query customer registration trends to track acquisition effectiveness
 * Business Question: How effectively are we acquiring new customers and what drives registrations?
 * Primary users: Marketing and CRM teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryCustomerRegistrationTrends: EnhancedQueryFunction<
  CustomerRegistrationTrends,
  SiteSpecificQueryTemplateParams
> = function queryCustomerRegistrationTrends(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<CustomerRegistrationTrends[], void, unknown> {
  const { sql, parameters } = queryCustomerRegistrationTrends.QUERY(params);
  return executeParameterizedQuery<CustomerRegistrationTrends>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryCustomerRegistrationTrends.metadata = {
  name: "customer-registration-trends",
  description:
    "Track customer acquisition effectiveness and registration drivers",
  category: "Customer Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryCustomerRegistrationTrends.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    SELECT
      r.registration_date AS "date",
      SUM(r.num_registrations) AS new_registrations,
      r.device_class_code,
      s.nsite_id
    FROM ccdw_aggr_registration r
    JOIN ccdw_dim_site s ON s.site_id = r.site_id
    WHERE r.registration_date >= '${startDate}' AND r.registration_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY r.registration_date, r.device_class_code, s.nsite_id
    ORDER BY r.registration_date
  `;

  return {
    sql,
    parameters: [],
  };
};

/**
 * Query total customer growth over time using customer list snapshots
 * Business Question: How is our customer base growing over time?
 * Primary users: Marketing and Executive teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryTotalCustomerGrowth: EnhancedQueryFunction<
  CustomerListSnapshot,
  SiteSpecificQueryTemplateParams
> = async function* queryTotalCustomerGrowth(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100,
): AsyncGenerator<CustomerListSnapshot[], void, unknown> {
  const { sql, parameters } = queryTotalCustomerGrowth.QUERY(params);
  yield* executeParameterizedQuery<CustomerListSnapshot>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryTotalCustomerGrowth.metadata = {
  name: "customer-growth",
  description: "Analyze customer base growth over time",
  category: "Customer Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryTotalCustomerGrowth.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    WITH customer_snapshots AS (
      SELECT
        cls.site_id,
        cls.ncustomer_list_id,
        CAST(cls.utc_record_timestamp AS DATE) AS snapshot_date,
        cls.utc_record_timestamp,
        cls.num_customers,
        ROW_NUMBER() OVER (
          PARTITION BY cls.site_id, CAST(cls.utc_record_timestamp AS DATE)
          ORDER BY cls.utc_record_timestamp DESC
        ) AS rn
      FROM ccdw_fact_customer_list_snapshot cls
      JOIN ccdw_dim_site s
        ON cls.site_id = s.site_id
      WHERE CAST(cls.utc_record_timestamp AS DATE) >= '${startDate}'
        AND CAST(cls.utc_record_timestamp AS DATE) <= '${endDate}'
        AND s.nsite_id = '${params.siteId}'
    ),
    unique_lists AS (
      SELECT
        ncustomer_list_id,
        snapshot_date,
        num_customers,
        ROW_NUMBER() OVER (
          PARTITION BY ncustomer_list_id, snapshot_date
          ORDER BY utc_record_timestamp DESC
        ) AS rn
      FROM customer_snapshots
      WHERE rn = 1
    )
    SELECT
      snapshot_date,
      SUM(num_customers) AS total_customers
    FROM unique_lists
    WHERE rn = 1
    GROUP BY snapshot_date
    ORDER BY snapshot_date
  `;

  return {
    sql,
    parameters: [],
  };
};

interface RegistrationQueryParams extends QueryTemplateParams {
  siteId?: string;
  deviceClassCode?: string;
}

/**
 * Query raw registration data for custom analysis
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results
 * @param filters Optional filters for site, device class
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryRegistration: EnhancedQueryFunction<
  RegistrationRecord,
  RegistrationQueryParams
> = async function* queryRegistration(
  client: CIPClient,
  params: RegistrationQueryParams,
  batchSize: number = 100,
): AsyncGenerator<RegistrationRecord[], void, unknown> {
  // Ensure dateRange has a default if not provided
  const queryParams: RegistrationQueryParams = {
    ...params,
    dateRange: params.dateRange || { startDate: new Date(0), endDate: new Date() },
  };
  const { sql, parameters } = queryRegistration.QUERY(queryParams);
  yield* executeParameterizedQuery<RegistrationRecord>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryRegistration.metadata = {
  name: "customer-registrations-raw",
  description: "Query raw registration data for custom analysis",
  category: "Customer Analytics",
  requiredParams: [],
  optionalParams: ["siteId", "deviceClassCode", "from", "to"],
};

queryRegistration.QUERY = (
  params: RegistrationQueryParams,
): { sql: string; parameters: any[] } => {
  // dateRange is required in the params structure, even if it was optional in the function signature
  validateRequiredParams(params, ["dateRange"]);

  let sql = "SELECT r.* FROM ccdw_aggr_registration r";
  const joins: string[] = [];
  const conditions: string[] = [];

  if (params.dateRange) {
    const { startDate, endDate } = formatDateRange(params.dateRange);
    conditions.push(`r.registration_date >= '${startDate}' AND r.registration_date <= '${endDate}'`);
  }

  if (params.siteId) {
    joins.push("JOIN ccdw_dim_site s ON s.site_id = r.site_id");
    conditions.push(`s.nsite_id = '${params.siteId}'`);
  }

  if (params.deviceClassCode) {
    conditions.push(`r.device_class_code = '${params.deviceClassCode}'`);
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
