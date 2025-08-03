import { CIPClient } from '../../cip-client';
import { DateRange, formatDateForSQL, cleanSQL } from '../types';
import {
  SiteSpecificQueryTemplateParams,
  formatDateRange,
  executeParameterizedQuery,
  validateRequiredParams,
  EnhancedQueryFunction,
} from '../helpers';

export interface OcapiPerformanceRecord {
  request_date: Date | string;
  api_name: string;
  api_resource: string;
  total_requests: number;
  total_response_time: number;
  avg_response_time: number;
  client_id: string;
}

/**
 * Query OCAPI performance metrics to monitor system health and customer experience
 * Business Question: Is our system performing well enough to support customer experience?
 * Primary users: Operations teams, Engineering
 * @param client The Avatica client instance (must have an open connection)
 * @param params Query parameters including siteId and dateRange
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryOcapiRequests: EnhancedQueryFunction<
  OcapiPerformanceRecord,
  SiteSpecificQueryTemplateParams
> = async function* queryOcapiRequests(
  client: CIPClient,
  params: SiteSpecificQueryTemplateParams,
  batchSize: number = 100
): AsyncGenerator<OcapiPerformanceRecord[], void, unknown> {
  const { sql, parameters } = queryOcapiRequests.QUERY(params);
  yield* executeParameterizedQuery<OcapiPerformanceRecord>(
    client,
    cleanSQL(sql),
    parameters,
    batchSize,
  );
};

queryOcapiRequests.metadata = {
  name: "ocapi-requests",
  description: "Analyze API performance including response times and request volumes",
  category: "Technical Analytics",
  requiredParams: ["siteId", "from", "to"],
};

queryOcapiRequests.QUERY = (
  params: SiteSpecificQueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["siteId", "dateRange"]);
  const { startDate, endDate } = formatDateRange(params.dateRange);

  const sql = `
    SELECT
      o.request_date,
      o.api_name,
      o.api_resource,
      SUM(o.num_requests) as total_requests,
      SUM(o.response_time) as total_response_time,
      CASE WHEN SUM(o.num_requests) > 0
           THEN SUM(o.response_time) / SUM(o.num_requests)
           ELSE 0 END as avg_response_time,
      o.client_id
    FROM ccdw_aggr_ocapi_request o
    JOIN ccdw_dim_site s ON s.site_id = o.site_id
    WHERE o.request_date >= '${startDate}' AND o.request_date <= '${endDate}'
      AND s.nsite_id = '${params.siteId}'
    GROUP BY o.request_date, o.api_name, o.api_resource, o.client_id
    ORDER BY total_requests DESC
  `;

  return {
    sql,
    parameters: [],
  };
};