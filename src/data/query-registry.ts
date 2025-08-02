import { CIPClient } from '../cip-client';
import { DateRange } from './types';
import * as customerRegistration from './aggregate/customer_registration_analytics';

export interface QueryDefinition {
  name: string;
  description: string;
  category: string;
  requiredParams: string[];
  optionalParams?: string[];
  execute: (client: CIPClient, params: any) => AsyncGenerator<any[], void, unknown>;
}

// Convert our business functions to a common interface
const queries: QueryDefinition[] = [
  {
    name: 'customer-registration-trends',
    description: 'Track customer acquisition effectiveness and registration drivers',
    category: 'Customer Analytics',
    requiredParams: ['siteId', 'from', 'to'],
    execute: function (client: CIPClient, params: any) {
      const dateRange: DateRange = {
        startDate: new Date(params.from),
        endDate: new Date(params.to)
      };
      
      return customerRegistration.queryCustomerRegistrationTrends(
        client,
        params.siteId,
        dateRange,
        100
      );
    }
  },
  {
    name: 'customer-growth',
    description: 'Analyze customer base growth over time',
    category: 'Customer Analytics',
    requiredParams: ['siteId', 'from', 'to'],
    execute: async function* (client: CIPClient, params: any) {
      const dateRange: DateRange = {
        startDate: new Date(params.from),
        endDate: new Date(params.to)
      };
      
      yield* customerRegistration.queryTotalCustomerGrowth(
        client,
        params.siteId,
        dateRange,
        100
      );
    }
  },
  {
    name: 'customer-registrations-raw',
    description: 'Query raw registration data for custom analysis',
    category: 'Customer Analytics',
    requiredParams: [],
    optionalParams: ['siteId', 'deviceClassCode', 'from', 'to'],
    execute: async function* (client: CIPClient, params: any) {
      const dateRange = params.from && params.to ? {
        startDate: new Date(params.from),
        endDate: new Date(params.to)
      } : undefined;
      
      const filters = {
        siteId: params.siteId,
        deviceClassCode: params.deviceClassCode
      };
      
      yield* customerRegistration.queryRegistration(
        client,
        dateRange,
        filters,
        100
      );
    }
  }
];

export function getQueryByName(name: string): QueryDefinition | undefined {
  return queries.find(q => q.name === name);
}

export function listQueries(): QueryDefinition[] {
  return queries;
}

export function getQueriesByCategory(): Map<string, QueryDefinition[]> {
  const byCategory = new Map<string, QueryDefinition[]>();
  
  for (const query of queries) {
    const categoryQueries = byCategory.get(query.category) || [];
    categoryQueries.push(query);
    byCategory.set(query.category, categoryQueries);
  }
  
  return byCategory;
}
