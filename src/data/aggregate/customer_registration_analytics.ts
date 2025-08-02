import { CIPClient } from '../../cip-client';
import { DateRange, cleanSQL } from '../types';
import { 
  SiteSpecificQueryTemplateParams, 
  formatDateRange,
  executeQuery,
  QueryTemplateParams,
  validateRequiredParams
} from '../helpers';

export interface RegistrationRecord {
  registration_date: Date | string;
  site_id: number;
  device_class_code: string;
  num_registrations: number;
  locale_id: number;
  channel_id: number;
}

export interface CustomerRegistrationTrends {
  date: Date | string;
  new_registrations: number;
  device_class_code: string;
  nsite_id: string;
}

export interface CustomerListSnapshot {
  snapshot_date: Date | string;
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
export function queryCustomerRegistrationTrends(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<CustomerRegistrationTrends[], void, unknown> {
  const sql = queryCustomerRegistrationTrends.QUERY({ siteId, dateRange });
  return executeQuery<CustomerRegistrationTrends>(client, cleanSQL(sql), batchSize);
}

queryCustomerRegistrationTrends.QUERY = (params: SiteSpecificQueryTemplateParams): string => {
  validateRequiredParams(params, ['siteId', 'dateRange']);
  const { startDate, endDate } = formatDateRange(params.dateRange);
  
  return `
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
export async function* queryTotalCustomerGrowth(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<CustomerListSnapshot[], void, unknown> {
  const sql = queryTotalCustomerGrowth.QUERY({ siteId, dateRange });
  yield* executeQuery<CustomerListSnapshot>(client, cleanSQL(sql), batchSize);
}

queryTotalCustomerGrowth.QUERY = (params: SiteSpecificQueryTemplateParams): string => {
  validateRequiredParams(params, ['siteId', 'dateRange']);
  const { startDate, endDate } = formatDateRange(params.dateRange);
  
  return `
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
};

interface RegistrationQueryParams extends QueryTemplateParams {
  filters?: {
    siteId?: string;
    deviceClassCode?: string;
  };
}

/**
 * Query raw registration data for custom analysis
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results
 * @param filters Optional filters for site, device class
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryRegistration(
  client: CIPClient,
  dateRange?: DateRange,
  filters?: {
    siteId?: string;
    deviceClassCode?: string;
  },
  batchSize: number = 100
): AsyncGenerator<RegistrationRecord[], void, unknown> {
  const params: RegistrationQueryParams = { 
    dateRange: dateRange || { startDate: new Date(0), endDate: new Date() },
    filters 
  };
  const sql = queryRegistration.QUERY(params);
  yield* executeQuery<RegistrationRecord>(client, cleanSQL(sql), batchSize);
}

queryRegistration.QUERY = (params: RegistrationQueryParams): string => {
  // dateRange is required in the params structure, even if it was optional in the function signature
  validateRequiredParams(params, ['dateRange']);
  
  let sql = 'SELECT r.* FROM ccdw_aggr_registration r';
  const joins: string[] = [];
  const conditions: string[] = [];
  
  if (params.dateRange) {
    const { startDate, endDate } = formatDateRange(params.dateRange);
    conditions.push(`r.registration_date >= '${startDate}' AND r.registration_date <= '${endDate}'`);
  }
  
  if (params.filters?.siteId) {
    joins.push('JOIN ccdw_dim_site s ON s.site_id = r.site_id');
    conditions.push(`s.nsite_id = '${params.filters.siteId}'`);
  }
  
  if (params.filters?.deviceClassCode) {
    conditions.push(`r.device_class_code = '${params.filters.deviceClassCode}'`);
  }
  
  if (joins.length > 0) {
    sql += ' ' + joins.join(' ');
  }
  
  if (conditions.length > 0) {
    sql += ' WHERE ' + conditions.join(' AND ');
  }
  
  return sql;
};
