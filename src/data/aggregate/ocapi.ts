import { CIPClient } from '../../cip-client';
import { DateRange, formatDateForSQL, cleanSQL } from '../types';
import { processFrame } from '../../utils';
import {
  QueryTemplateParams,
  formatDateRange,
  executeParameterizedQuery,
  validateRequiredParams,
  EnhancedQueryFunction,
} from '../helpers';

export interface OcapiRequestRecord {
  request_date: Date | string;
  api_name: string;
  api_resource: string;
  api_version: string;
  site_id: number;
  client_id: string;
  status_code: number;
  method: string;
  num_requests: number;
  response_time: number;
  num_requests_bucket1: number;
  num_requests_bucket2: number;
  num_requests_bucket3: number;
  num_requests_bucket4: number;
  num_requests_bucket5: number;
  num_requests_bucket6: number;
  num_requests_bucket7: number;
  num_requests_bucket8: number;
  num_requests_bucket9: number;
  num_requests_bucket10: number;
  num_requests_bucket11: number;
}

/**
 * Query the ccdw_aggr_ocapi_request table with optional date filtering
 * Business Question: How are our APIs performing in terms of response times and request volumes?
 * Primary users: Technical teams, Operations
 * @param client The Avatica client instance (must have an open connection)
 * @param params Query parameters including optional date range
 * @param batchSize Size of each batch to yield (default: 100)
 */
export const queryOcapiRequests: EnhancedQueryFunction<
  OcapiRequestRecord,
  QueryTemplateParams
> = async function* queryOcapiRequests(
  client: CIPClient,
  params: QueryTemplateParams,
  batchSize: number = 100
): AsyncGenerator<OcapiRequestRecord[], void, unknown> {
  // Ensure dateRange has a default if not provided
  const queryParams: QueryTemplateParams = {
    ...params,
    dateRange: params.dateRange || { startDate: new Date(0), endDate: new Date() },
  };
  const { sql, parameters } = queryOcapiRequests.QUERY(queryParams);
  yield* executeParameterizedQuery<OcapiRequestRecord>(
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
  requiredParams: [],
  optionalParams: ["from", "to"],
};

queryOcapiRequests.QUERY = (
  params: QueryTemplateParams,
): { sql: string; parameters: any[] } => {
  validateRequiredParams(params, ["dateRange"]);

  let sql = "SELECT * FROM ccdw_aggr_ocapi_request";
  const conditions: string[] = [];

  if (params.dateRange) {
    const { startDate, endDate } = formatDateRange(params.dateRange);
    conditions.push(`request_date >= '${startDate}' AND request_date <= '${endDate}'`);
  }

  if (conditions.length > 0) {
    sql += " WHERE " + conditions.join(" AND ");
  }

  return {
    sql,
    parameters: [],
  };
};