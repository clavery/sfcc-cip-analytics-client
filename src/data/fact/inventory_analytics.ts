import { CIPClient } from '../../cip-client';
import { DateRange, formatDateForSQL, cleanSQL } from '../types';
import { processFrame } from '../../utils';

export interface InventorySnapshotRecord {
  utc_record_timestamp: Date | string;
  sku_id: number;
  location_group_id: number;
  available_to_fulfill: number;
  available_to_order: number;
  on_hand: number;
  reserved: number;
  safety_stock: number;
  turnover: number;
}

export interface InventoryTrends {
  timestamp: Date | string;
  nsku_id: string;
  nproduct_id: string;
  available_to_fulfill: number;
  available_to_order: number;
  on_hand: number;
  reserved: number;
}

export interface InventoryByLocation {
  nlocation_group_id: string;
  nsku_id: string;
  nproduct_id: string;
  available_to_fulfill: number;
  available_to_order: number;
  on_hand: number;
  reserved: number;
}

/**
 * Query inventory trends over time for specific SKUs
 * Business Question: Do we have the right products available where customers want them?
 * Primary users: Operations teams, Supply Chain
 * @param client The Avatica client instance (must have an open connection)
 * @param skuId The natural SKU ID to track
 * @param locationGroupId The natural location group ID
 * @param dateRange Date range to filter results
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryInventoryTrends(
  client: CIPClient,
  skuId: string,
  locationGroupId: string,
  dateRange: DateRange,
  batchSize: number = 100
): AsyncGenerator<InventoryTrends[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const startDateStr = formatDateForSQL(dateRange.startDate);
    const endDateStr = formatDateForSQL(dateRange.endDate);
    
    const sql = cleanSQL(`
      SELECT
        ih.utc_record_timestamp AS "timestamp",
        p.nsku_id,
        p.nproduct_id,
        SUM(ih.available_to_fulfill) AS available_to_fulfill,
        SUM(ih.available_to_order) AS available_to_order,
        SUM(ih.on_hand) AS on_hand,
        SUM(ih.reserved) AS reserved
      FROM
        (SELECT utc_record_timestamp, sku_id, location_group_id,
                available_to_fulfill, available_to_order, on_hand, reserved
         FROM ccdw_fact_inventory_record_snapshot_hourly) ih
      JOIN
        (SELECT sku_id, nsku_id, nproduct_id FROM ccdw_dim_product) p
        ON p.sku_id = ih.sku_id
      JOIN
        (SELECT location_group_id, nlocation_group_id FROM ccdw_dim_location_group) lg
        ON lg.location_group_id = ih.location_group_id
      WHERE
        ih.utc_record_timestamp >= '${startDateStr}'
        AND ih.utc_record_timestamp <= '${endDateStr}'
        AND p.nsku_id = '${skuId}'
        AND lg.nlocation_group_id = '${locationGroupId}'
      GROUP BY
        ih.utc_record_timestamp, p.nsku_id, p.nproduct_id
      ORDER BY
        ih.utc_record_timestamp
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<InventoryTrends>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<InventoryTrends>(result.signature, currentFrame);
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
 * Query current inventory levels by location group
 * Business Question: What is the current inventory distribution across locations?
 * Primary users: Operations teams, Fulfillment managers
 * @param client The Avatica client instance (must have an open connection)
 * @param locationGroupId The natural location group ID
 * @param limit Maximum number of SKUs to return (default: 100)
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryCurrentInventoryByLocation(
  client: CIPClient,
  locationGroupId: string,
  limit: number = 100,
  batchSize: number = 100
): AsyncGenerator<InventoryByLocation[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const sql = cleanSQL(`
      SELECT
        lg.nlocation_group_id,
        p.nsku_id,
        p.nproduct_id,
        SUM(ibl.available_to_fulfill) AS available_to_fulfill,
        SUM(ibl.available_to_order) AS available_to_order,
        SUM(ibl.on_hand) AS on_hand,
        SUM(ibl.reserved) AS reserved
      FROM ccdw_aggr_inventory_by_location_group ibl
      JOIN ccdw_dim_location_group lg ON lg.location_group_id = ibl.location_group_id
      JOIN ccdw_dim_product p ON p.sku_id = ibl.sku_id
      WHERE ibl.record_date = CURRENT_DATE
        AND lg.nlocation_group_id = '${locationGroupId}'
      GROUP BY lg.nlocation_group_id, p.nsku_id, p.nproduct_id
      ORDER BY available_to_fulfill DESC
      LIMIT ${limit}
    `);

    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      const firstFrameData = processFrame<InventoryByLocation>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<InventoryByLocation>(result.signature, currentFrame);
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
 * Query raw inventory snapshot data for custom analysis
 * @param client The Avatica client instance (must have an open connection)
 * @param dateRange Optional date range to filter results
 * @param filters Optional filters for SKU, location group
 * @param batchSize Size of each batch to yield (default: 100)
 */
export async function* queryInventorySnapshot(
  client: CIPClient,
  dateRange?: DateRange,
  filters?: {
    skuId?: string;
    locationGroupId?: string;
  },
  batchSize: number = 100
): AsyncGenerator<InventorySnapshotRecord[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    let sql = 'SELECT ih.* FROM ccdw_fact_inventory_record_snapshot_hourly ih';
    const joins: string[] = [];
    const conditions: string[] = [];
    
    if (dateRange) {
      const startDateStr = formatDateForSQL(dateRange.startDate);
      const endDateStr = formatDateForSQL(dateRange.endDate);
      conditions.push(`ih.utc_record_timestamp >= '${startDateStr}' AND ih.utc_record_timestamp <= '${endDateStr}'`);
    }
    
    if (filters?.skuId) {
      joins.push('JOIN ccdw_dim_product p ON p.sku_id = ih.sku_id');
      conditions.push(`p.nsku_id = '${filters.skuId}'`);
    }
    
    if (filters?.locationGroupId) {
      joins.push('JOIN ccdw_dim_location_group lg ON lg.location_group_id = ih.location_group_id');
      conditions.push(`lg.nlocation_group_id = '${filters.locationGroupId}'`);
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
      
      const firstFrameData = processFrame<InventorySnapshotRecord>(result.signature, result.firstFrame);
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
        
        const nextData = processFrame<InventorySnapshotRecord>(result.signature, currentFrame);
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