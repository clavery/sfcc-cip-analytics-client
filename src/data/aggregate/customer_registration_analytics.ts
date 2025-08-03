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

