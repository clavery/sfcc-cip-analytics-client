import { CIPClient } from '../../cip-client';
import { DateRange, formatDateForSQL, cleanSQL } from '../types';
import { processFrame } from '../../utils';

export interface CustomerRecord {
  customer_id: number;
  ncustomer_id: string;
  email: string | null;
  first_name: string | null;
  last_name: string | null;
  registered_date: Date | string | null;
  last_login_date: Date | string | null;
  preferred_locale: string | null;
  customer_type: string;
  email_confirmed: boolean;
  account_locked: boolean;
  disabled: boolean;
  phone_home: string | null;
  phone_business: string | null;
  phone_mobile: string | null;
  title: string | null;
  suffix: string | null;
  salutation: string | null;
  company_name: string | null;
  job_title: string | null;
  birthday: Date | string | null;
  gender: string | null;
  creation_date: Date | string;
  last_modified: Date | string;
}

export interface CustomerRegistrationTrend {
  registration_dt: Date | string;
  new_registrations: number;
  device_class_code: string;
  nsite_id: string;
}

export interface CustomerSnapshot {
  snapshot_date: Date | string;
  total_customers: number;
  nsite_id: string;
}

/**
 * Query customer registration trends to understand acquisition patterns
 * Business Question: How are we acquiring and retaining customers?
 * Primary users: Marketing and CRM teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryCustomerRegistrationTrendsByDimension(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<CustomerRegistrationTrend[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      SELECT
        r.registration_date AS registration_dt,
        SUM(r.num_registrations) AS new_registrations,
        r.device_class_code,
        s.nsite_id
      FROM ccdw_aggr_registration r
      JOIN ccdw_dim_site s
        ON s.site_id = r.site_id
      WHERE r.registration_date >= '${startDateStr}'
        AND r.registration_date <= '${endDateStr}'
        AND s.nsite_id = '${siteId}'
      GROUP BY
        r.registration_date,
        r.device_class_code,
        s.nsite_id
      ORDER BY r.registration_date
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<CustomerRegistrationTrend>(result.signature, result.firstFrame);
      if (firstFrameData.length > 0) {
        yield firstFrameData;
      }

      let done = result.firstFrame.done;
      let currentFrame: typeof result.firstFrame | undefined = result.firstFrame;

      while (!done && currentFrame) {
        const currentOffset = currentFrame.offset || 0;
        const currentRowCount = currentFrame.rows?.length || 0;
        
        const nextResponse = await client.fetch(
          result.statementId || 0,
          currentOffset + currentRowCount,
          batchSize
        );
        
        currentFrame = nextResponse.frame;
        if (!currentFrame) break;
        
        const nextData = processFrame<CustomerRegistrationTrend>(result.signature, currentFrame);
        if (nextData.length > 0) {
          yield nextData;
        }
        
        done = currentFrame.done;
      }
    }
  } finally {
    await client.closeStatement(statementId);
  }
}

/**
 * Query total customer counts over time
 * Business Question: How is our customer base growing?
 * Primary users: Executive and Marketing teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryTotalCustomerCounts(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<CustomerSnapshot[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      SELECT
        cls.std_record_timestamp as snapshot_date,
        SUM(cls.num_customers) as total_customers,
        s.nsite_id
      FROM ccdw_fact_customer_list_snapshot cls
      JOIN ccdw_dim_site s ON s.site_id = cls.site_id
      WHERE cls.std_record_timestamp >= '${startDateStr}' AND cls.std_record_timestamp <= '${endDateStr}'
        AND s.nsite_id = '${siteId}'
      GROUP BY cls.std_record_timestamp, s.nsite_id
      ORDER BY cls.std_record_timestamp
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<CustomerSnapshot>(result.signature, result.firstFrame);
      if (firstFrameData.length > 0) {
        yield firstFrameData;
      }

      let done = result.firstFrame.done;
      let currentFrame: typeof result.firstFrame | undefined = result.firstFrame;

      while (!done && currentFrame) {
        const currentOffset = currentFrame.offset || 0;
        const currentRowCount = currentFrame.rows?.length || 0;
        
        const nextResponse = await client.fetch(
          result.statementId || 0,
          currentOffset + currentRowCount,
          batchSize
        );
        
        currentFrame = nextResponse.frame;
        if (!currentFrame) break;
        
        const nextData = processFrame<CustomerSnapshot>(result.signature, currentFrame);
        if (nextData.length > 0) {
          yield nextData;
        }
        
        done = currentFrame.done;
      }
    }
  } finally {
    await client.closeStatement(statementId);
  }
}

/**
 * Query customer demographics and segments
 * @param client The Avatica client instance (must have an open connection)
 * @param filters Optional filters for customer attributes
 * @param limit Maximum number of customers to return (default: 1000)
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryCustomerDemographics(
  client: CIPClient,
  filters?: {
    customerType?: string;
    gender?: string;
    emailConfirmed?: boolean;
    registeredAfter?: Date;
    registeredBefore?: Date;
  },
  limit: number = 1000,
  batchSize: number = 100
): AsyncGenerator<CustomerRecord[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    let sql = 'SELECT * FROM ccdw_dim_customer';
    const conditions: string[] = [];
    
    if (filters?.customerType) {
      conditions.push(`customer_type = '${filters.customerType}'`);
    }
    
    if (filters?.gender) {
      conditions.push(`gender = '${filters.gender}'`);
    }
    
    if (filters?.emailConfirmed !== undefined) {
      conditions.push(`email_confirmed = ${filters.emailConfirmed}`);
    }
    
    if (filters?.registeredAfter) {
      const dateStr = formatDateForSQL(filters.registeredAfter);
      conditions.push(`registered_date >= '${dateStr}'`);
    }
    
    if (filters?.registeredBefore) {
      const dateStr = formatDateForSQL(filters.registeredBefore);
      conditions.push(`registered_date <= '${dateStr}'`);
    }
    
    if (conditions.length > 0) {
      sql += ' WHERE ' + conditions.join(' AND ');
    }
    
    sql += ` LIMIT ${limit}`;

    const executeResponse = await client.execute(statementId, cleanSQL(sql), batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<CustomerRecord>(result.signature, result.firstFrame);
      if (firstFrameData.length > 0) {
        yield firstFrameData;
      }

      let done = result.firstFrame.done;
      let currentFrame: typeof result.firstFrame | undefined = result.firstFrame;

      while (!done && currentFrame) {
        const currentOffset = currentFrame.offset || 0;
        const currentRowCount = currentFrame.rows?.length || 0;
        
        const nextResponse = await client.fetch(
          result.statementId || 0,
          currentOffset + currentRowCount,
          batchSize
        );
        
        currentFrame = nextResponse.frame;
        if (!currentFrame) break;
        
        const nextData = processFrame<CustomerRecord>(result.signature, currentFrame);
        if (nextData.length > 0) {
          yield nextData;
        }
        
        done = currentFrame.done;
      }
    }
  } finally {
    await client.closeStatement(statementId);
  }
}