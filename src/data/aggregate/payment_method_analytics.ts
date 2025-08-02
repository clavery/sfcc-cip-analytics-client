import { CIPClient } from '../../cip-client';
import { DateRange, formatDateForSQL, cleanSQL } from '../types';
import { processFrame } from '../../utils';

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
export async function* queryPaymentMethodPerformance(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<PaymentMethodPerformance[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
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
      WHERE pss.submit_date >= '${startDateStr}' AND pss.submit_date <= '${endDateStr}'
        AND s.nsite_id = '${siteId}'
      GROUP BY pm.display_name
      ORDER BY std_captured_amount DESC
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<PaymentMethodPerformance>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<PaymentMethodPerformance>(result.signature, currentFrame);
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
 * Query gift certificate redemption patterns to optimize gift certificate marketing campaigns
 * Business Question: How are gift certificates being redeemed and what is their value?
 * Primary users: Operations and Marketing teams
 * @param client The Avatica client instance (must have an open connection)
 * @param siteId The natural site ID to filter by
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryGiftCertificateAnalytics(
  client: CIPClient,
  siteId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<GiftCertificateAnalytics[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
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
      WHERE pss.submit_date >= '${startDateStr}'
        AND pss.submit_date <= '${endDateStr}'
        AND s.nsite_id = '${siteId}'
        AND pm.npayment_method_id = 'GIFT_CERTIFICATE'
      GROUP BY
        pss.submit_date,
        s.nsite_id
      ORDER BY
        pss.submit_date ASC,
        s.nsite_id ASC
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<GiftCertificateAnalytics>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<GiftCertificateAnalytics>(result.signature, currentFrame);
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
 * Query raw payment sales summary data for custom analysis
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results
 * @param filters Optional filters for site, payment method, device, registration status
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryPaymentSalesSummary(
  client: CIPClient,
  dateRange?: DateRange,
  filters?: {
    siteId?: string;
    paymentMethodId?: string;
    deviceClassCode?: string;
    registered?: boolean;
  },
  batchSize: number = 100
): AsyncGenerator<PaymentSalesSummaryRecord[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    let sql = 'SELECT pss.* FROM ccdw_aggr_payment_sales_summary pss';
    const joins: string[] = [];
    const conditions: string[] = [];
    
    if (dateRange) {
      const startDateStr = formatDateForSQL(dateRange.startDate);
      const endDateStr = formatDateForSQL(dateRange.endDate);
      conditions.push(`pss.submit_date >= '${startDateStr}' AND pss.submit_date <= '${endDateStr}'`);
    }
    
    if (filters?.siteId) {
      joins.push('JOIN ccdw_dim_site s ON s.site_id = pss.site_id');
      conditions.push(`s.nsite_id = '${filters.siteId}'`);
    }
    
    if (filters?.paymentMethodId) {
      joins.push('JOIN ccdw_dim_payment_method pm ON pm.payment_method_id = pss.payment_method_id');
      conditions.push(`pm.npayment_method_id = '${filters.paymentMethodId}'`);
    }
    
    if (filters?.deviceClassCode) {
      conditions.push(`pss.device_class_code = '${filters.deviceClassCode}'`);
    }
    
    if (filters?.registered !== undefined) {
      conditions.push(`pss.registered = ${filters.registered}`);
    }
    
    if (joins.length > 0) {
      sql += ' ' + joins.join(' ');
    }
    
    if (conditions.length > 0) {
      sql += ' WHERE ' + conditions.join(' AND ');
    }

    const executeResponse = await client.execute(statementId, cleanSQL(sql), batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<PaymentSalesSummaryRecord>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<PaymentSalesSummaryRecord>(result.signature, currentFrame);
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