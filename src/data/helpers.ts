import { CIPClient } from '../cip-client';
import { DateRange, formatDateForSQL, cleanSQL } from './types';
import { processFrame } from '../utils';

export interface QueryTemplateParams {
  dateRange: DateRange;
}

export interface SiteSpecificQueryTemplateParams extends QueryTemplateParams {
  siteId: string;
}

export type SqlTemplateFunction<T extends QueryTemplateParams = QueryTemplateParams> = (params: T) => string;

export async function* executeQuery<T>(
  client: CIPClient,
  sql: string,
  batchSize: number = 100
): AsyncGenerator<T[], void, unknown> {
  const statementId = await client.createStatement();

  try {
    const executeResponse = await client.execute(statementId, sql, batchSize);
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        return;
      }
      
      debugger;
      const firstFrameData = processFrame<T>(result.signature, result.firstFrame);
      if (firstFrameData.length > 0) {
        console.log(`Yielding first frame with ${firstFrameData.length} records`);
        yield firstFrameData;
      }

      console.log(result, result.signature);
      let done = result.firstFrame.done;
      let currentFrame: typeof result.firstFrame | undefined = result.firstFrame;

      console.log(`First frame done: ${done}, offset: ${currentFrame.offset}, rows: ${currentFrame.rows?.length}`);
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
        
        const nextData = processFrame<T>(result.signature, currentFrame);
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

export function formatDateRange(dateRange: DateRange): { startDate: string; endDate: string } {
  return {
    startDate: formatDateForSQL(dateRange.startDate),
    endDate: formatDateForSQL(dateRange.endDate)
  };
}

export function validateRequiredParams<T extends Record<string, any>>(
  params: T,
  requiredFields: (keyof T)[]
): void {
  const missingFields: string[] = [];
  
  for (const field of requiredFields) {
    if (params[field] === undefined || params[field] === null) {
      missingFields.push(String(field));
    }
  }
  
  if (missingFields.length > 0) {
    throw new Error(`Missing required parameters: ${missingFields.join(', ')}`);
  }
  
  // Validate nested required fields
  if ('dateRange' in params && params.dateRange) {
    const dateRange = params.dateRange as any;
    if (!dateRange.startDate || !dateRange.endDate) {
      throw new Error('DateRange must include both startDate and endDate');
    }
  }
}
