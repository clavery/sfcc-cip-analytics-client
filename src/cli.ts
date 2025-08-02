#!/usr/bin/env node

import { parseArgs } from 'util';
import { CIPClient } from './cip-client';
import { processFrame } from './utils';
import { NormalizedFrame } from './normalized-types';
import { formatDateForSQL } from './data/types';
import { queryCustomerRegistrationTrends, queryTotalCustomerGrowth, queryRegistration } from './data/aggregate/customer_registration_analytics';
import { queryTopSellingProducts, queryProductCoPurchaseAnalysis, queryProductPerformanceByDimension, queryProductSalesSummary } from './data/aggregate/product_analytics';
import { queryPromotionDiscountAnalysis, queryPromotionPerformanceByType, queryPromotionSalesSummary } from './data/aggregate/promotion_analytics';
import { queryRecommendationPerformanceByAlgorithm, queryOverallRecommendationPerformance, queryRecommendationWidgetPlacement } from './data/aggregate/recommendation_analytics';
import { EnhancedQueryFunction, QueryMetadata } from './data/helpers';
import { DateRange } from './data/types';

interface ParsedDate {
  date: Date;
  formatted: string;
}

// Registry of available enhanced query functions
const availableQueries: EnhancedQueryFunction<any, any>[] = [
  // Customer Analytics
  queryCustomerRegistrationTrends,
  queryTotalCustomerGrowth,
  queryRegistration,
  // Product Analytics
  queryTopSellingProducts,
  queryProductCoPurchaseAnalysis,
  queryProductPerformanceByDimension,
  queryProductSalesSummary,
  // Promotion Analytics
  queryPromotionDiscountAnalysis,
  queryPromotionPerformanceByType,
  queryPromotionSalesSummary,
  // Recommendation Analytics
  queryRecommendationPerformanceByAlgorithm,
  queryOverallRecommendationPerformance,
  queryRecommendationWidgetPlacement
];

interface QueryDefinition {
  name: string;
  description: string;
  category: string;
  requiredParams: string[];
  optionalParams?: string[];
  execute: (client: CIPClient, params: any) => AsyncGenerator<any[], void, unknown>;
}

function getQueryByName(name: string): QueryDefinition | undefined {
  const queryFn = availableQueries.find(q => q.metadata.name === name);
  if (!queryFn) return undefined;

  return {
    name: queryFn.metadata.name,
    description: queryFn.metadata.description,
    category: queryFn.metadata.category,
    requiredParams: queryFn.metadata.requiredParams,
    optionalParams: queryFn.metadata.optionalParams,
    execute: (client: CIPClient, params: any) => {
      // Only handle dateRange conversion - pass everything else through directly
      const queryParams: any = {
        ...params,
        dateRange: params.from && params.to ? {
          startDate: new Date(params.from),
          endDate: new Date(params.to)
        } : undefined
      };
      
      // Remove the CLI-specific from/to params since they're now in dateRange
      delete queryParams.from;
      delete queryParams.to;
      
      // Call the query function with the standardized params
      return queryFn(client, queryParams, params.batchSize || 100);
    }
  };
}

function listQueries(): QueryDefinition[] {
  return availableQueries.map(queryFn => ({
    name: queryFn.metadata.name,
    description: queryFn.metadata.description,
    category: queryFn.metadata.category,
    requiredParams: queryFn.metadata.requiredParams,
    optionalParams: queryFn.metadata.optionalParams,
    execute: getQueryByName(queryFn.metadata.name)!.execute
  }));
}

function getQueriesByCategory(): Map<string, QueryDefinition[]> {
  const byCategory = new Map<string, QueryDefinition[]>();
  
  for (const queryDef of listQueries()) {
    const categoryQueries = byCategory.get(queryDef.category) || [];
    categoryQueries.push(queryDef);
    byCategory.set(queryDef.category, categoryQueries);
  }
  
  return byCategory;
}

function parseDate(input: string): ParsedDate {
  const timestamp = Date.parse(input);
  
  if (isNaN(timestamp)) {
    throw new Error(`Unable to parse date: "${input}". Use standard date formats like "2024-01-15", "Jan 15, 2024", "2024-01-15T00:00:00"`);
  }
  
  const date = new Date(timestamp);
  return { date, formatted: formatDateForSQL(date) };
}


function replacePlaceholders(sql: string, fromDate?: string, toDate?: string): string {
  let result = sql;
  
  if (fromDate) {
    result = result.replace(/<FROM>/g, `'${fromDate}'`);
  }
  
  if (toDate) {
    result = result.replace(/<TO>/g, `'${toDate}'`);
  }
  
  return result;
}

function outputAsCSV(data: any[]): void {
  if (data.length === 0) {
    console.log('No data');
    return;
  }
  
  const headers = Object.keys(data[0]);
  console.log(headers.join(','));
  
  for (const row of data) {
    const values = headers.map(header => {
      const value = row[header];
      if (value === null || value === undefined) return '';
      const stringValue = String(value);
      // Escape quotes and wrap in quotes if contains comma or quote
      if (stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')) {
        return `"${stringValue.replace(/"/g, '""')}"`;
      }
      return stringValue;
    });
    console.log(values.join(','));
  }
}

function outputAsJSON(data: any[]): void {
  console.log(JSON.stringify(data, null, 2));
}

function outputAsTable(data: any[]): void {
  if (data.length === 0) {
    console.log('No data');
    return;
  }
  console.table(data);
}

async function readStdin(): Promise<string> {
  return new Promise((resolve, reject) => {
    let data = '';
    
    process.stdin.setEncoding('utf8');
    
    process.stdin.on('readable', () => {
      let chunk;
      while (null !== (chunk = process.stdin.read())) {
        data += chunk;
      }
    });
    
    process.stdin.on('end', () => {
      // Replace newlines with spaces and trim leading/trailing whitespace
      // This preserves the SQL structure while making it a single line
      const normalizedSQL = data.replace(/\s+/g, ' ').trim();
      resolve(normalizedSQL);
    });
    
    process.stdin.on('error', reject);
  });
}

function showHelp(): void {
  console.log(`
CIP Analytics Client CLI

Usage:
  cip-query <command> [options]

Commands:
  sql                Execute arbitrary SQL queries
  query              Execute predefined business queries

SQL Command:
  cip-query sql [options] <sql>
  cip-query sql [options] < file.sql
  echo "SELECT * FROM table" | cip-query sql [options]

  Options:
    --from <date>    From date for <FROM> placeholder
    --to <date>      To date for <TO> placeholder
    --format <type>  Output format: table (default), json, csv

Query Command:
  cip-query query --name <query-name> [options]
  cip-query query --list

  Options:
    --name <name>    Name of the predefined query to execute
    --list           List all available queries
    --from <date>    From date (maps to 'from' parameter)
    --to <date>      To date (maps to 'to' parameter)
    --param <k=v>    Additional query parameters (can be used multiple times)
    --format <type>  Output format: table (default), json, csv

  Common parameters:
    --param siteId=<id>              Site ID
    --param deviceClassCode=<code>   Device class code
    --param batchSize=<size>         Batch size for results

  Note: The --from and --to flags automatically provide the 'from' and 'to' parameters to queries.

Global Options:
  --help             Show this help message

Environment Variables:
  SFCC_CLIENT_ID     Your SFCC client ID (required)
  SFCC_CLIENT_SECRET Your SFCC client secret (required)
  SFCC_CIP_INSTANCE  Your SFCC CIP instance (required)
  SFCC_DEBUG         Enable debug logging (optional)

Examples:
  # Execute arbitrary SQL
  cip-query sql "SELECT * FROM ccdw_aggr_ocapi_request LIMIT 10"
  
  # Execute SQL with date placeholders
  cip-query sql --format json --from "2024-01-01" --to "2024-01-02" <<SQL
    SELECT * FROM ccdw_aggr_ocapi_request 
    WHERE request_date >= <FROM> AND request_date <= <TO>
  SQL
  
  # List available business queries
  cip-query query --list
  
  # Execute a business query
  cip-query query --name customer-registration-trends \\
    --param siteId=my-site \\
    --from 2024-01-01 \\
    --to 2024-01-31 \\
    --format csv
`);
}

function parseParams(paramArgs: string[]): Record<string, any> {
  const params: Record<string, any> = {};
  
  for (const arg of paramArgs) {
    const [key, ...valueParts] = arg.split('=');
    const value = valueParts.join('='); // Handle values with '=' in them
    
    if (!key || !value) {
      throw new Error(`Invalid parameter format: "${arg}". Use key=value format.`);
    }
    
    params[key] = value;
  }
  
  return params;
}

function listAvailableQueries(): void {
  const queriesByCategory = getQueriesByCategory();
  
  console.log('\nAvailable Queries:\n');
  
  for (const [category, queries] of queriesByCategory) {
    console.log(`${category}:`);
    for (const query of queries) {
      console.log(`  ${query.name}`);
      console.log(`    ${query.description}`);
      console.log(`    Required: ${query.requiredParams.join(', ') || 'none'}`);
      
      if (query.optionalParams && query.optionalParams.length > 0) {
        console.log(`    Optional: ${query.optionalParams.join(', ')}`);
      }
      console.log();
    }
  }
}

async function executeSqlCommand(args: string[]): Promise<void> {
  const { values, positionals } = parseArgs({
    args,
    options: {
      from: { type: 'string' },
      to: { type: 'string' },
      format: { type: 'string', default: 'table' },
      help: { type: 'boolean', short: 'h' }
    },
    allowPositionals: true
  });

  if (values.help) {
    showHelp();
    return;
  }

  // Get SQL from positional args or stdin
  let sql: string;
  if (positionals.length === 0) {
    // Check if stdin has data
    if (process.stdin.isTTY) {
      console.error('Error: SQL query is required (provide as argument or via stdin)');
      showHelp();
      process.exit(1);
    }
    // Read from stdin
    sql = await readStdin();
    if (!sql.trim()) {
      console.error('Error: No SQL query provided via stdin');
      process.exit(1);
    }
  } else {
    sql = positionals.join(' ');
  }
  const format = values.format as string;
  
  if (!['table', 'json', 'csv'].includes(format)) {
    console.error('Error: Format must be one of: table, json, csv');
    process.exit(1);
  }

  // Parse dates if provided
  let fromDate: string | undefined;
  let toDate: string | undefined;
  
  if (values.from) {
    const parsed = parseDate(values.from);
    fromDate = parsed.formatted;
    console.info(`Using from date: ${parsed.formatted} (parsed from "${values.from}")`);
  }
  
  if (values.to) {
    const parsed = parseDate(values.to);
    toDate = parsed.formatted;
    console.info(`Using to date: ${parsed.formatted} (parsed from "${values.to}")`);
  }

  // Replace placeholders in SQL
  const finalSQL = replacePlaceholders(sql, fromDate, toDate);
  console.info(`Executing SQL: ${finalSQL}`);

  // Get environment variables
  const clientId = process.env.SFCC_CLIENT_ID;
  const clientSecret = process.env.SFCC_CLIENT_SECRET;
  const instance = process.env.SFCC_CIP_INSTANCE;

  if (!clientId || !clientSecret || !instance) {
    console.error('Error: Required environment variables: SFCC_CLIENT_ID, SFCC_CLIENT_SECRET, SFCC_CIP_INSTANCE');
    process.exit(1);
  }

  // Create client and execute query
  const client = new CIPClient(clientId, clientSecret, instance);
  
  try {
    await client.openConnection({});
    const statementId = await client.createStatement();
    
    // Execute query and collect all results
    const executeResponse = await client.execute(statementId, finalSQL);
    const allData: any[] = [];
    
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (result.firstFrame) {
        const firstFrameData = processFrame(result.signature, result.firstFrame);
        allData.push(...firstFrameData);
        
        let done = result.firstFrame.done;
        let currentFrame: NormalizedFrame | undefined = result.firstFrame;
        
        // Fetch remaining data
        while (!done && currentFrame) {
          const currentOffset = currentFrame.offset || 0;
          const currentRowCount = currentFrame.rows?.length || 0;
          
          const nextResponse = await client.fetch(
            result.statementId || 0,
            currentOffset + currentRowCount,
            1000 // larger batch size for CLI
          );
          
          currentFrame = nextResponse.frame;
          if (!currentFrame) break;
          
          const nextData = processFrame(result.signature, currentFrame);
          allData.push(...nextData);
          done = currentFrame.done;
        }
      }
    }
    
    await client.closeStatement(statementId);
    
    // Output results in requested format
    switch (format) {
      case 'json':
        outputAsJSON(allData);
        break;
      case 'csv':
        outputAsCSV(allData);
        break;
      default:
        outputAsTable(allData);
        break;
    }
    
    console.info(`\nQuery completed. Retrieved ${allData.length} rows.`);
    
  } finally {
    await client.closeConnection();
  }
}

async function executeQueryCommand(args: string[]): Promise<void> {
  const { values } = parseArgs({
    args,
    options: {
      name: { type: 'string' },
      list: { type: 'boolean' },
      param: { type: 'string', multiple: true },
      from: { type: 'string' },
      to: { type: 'string' },
      format: { type: 'string', default: 'table' },
      help: { type: 'boolean', short: 'h' }
    },
    allowPositionals: false
  });

  if (values.help) {
    showHelp();
    return;
  }

  if (values.list) {
    listAvailableQueries();
    return;
  }

  if (!values.name) {
    console.error('Error: --name or --list is required for query command');
    showHelp();
    process.exit(1);
  }

  const queryDef = getQueryByName(values.name);
  if (!queryDef) {
    console.error(`Error: Unknown query "${values.name}"`);
    console.error('Use --list to see available queries');
    process.exit(1);
  }

  // Parse parameters
  const params = parseParams((values.param as string[]) || []);
  
  // Add date parameters if provided
  if (values.from) {
    const parsed = parseDate(values.from);
    params.from = parsed.formatted;
    console.info(`Using from date: ${parsed.formatted} (parsed from "${values.from}")`);
  }
  
  if (values.to) {
    const parsed = parseDate(values.to);
    params.to = parsed.formatted;
    console.info(`Using to date: ${parsed.formatted} (parsed from "${values.to}")`);
  }
  
  // Validate required parameters
  const missingParams = queryDef.requiredParams.filter(p => !params[p]);
  if (missingParams.length > 0) {
    console.error(`Error: Missing required parameters: ${missingParams.join(', ')}`);
    console.error(`Required parameters for ${queryDef.name}: ${queryDef.requiredParams.join(', ')}`);
    if (queryDef.optionalParams && queryDef.optionalParams.length > 0) {
      console.error(`Optional parameters: ${queryDef.optionalParams.join(', ')}`);
    }
    process.exit(1);
  }

  const format = values.format as string;
  if (!['table', 'json', 'csv'].includes(format)) {
    console.error('Error: Format must be one of: table, json, csv');
    process.exit(1);
  }

  // Get environment variables
  const clientId = process.env.SFCC_CLIENT_ID;
  const clientSecret = process.env.SFCC_CLIENT_SECRET;
  const instance = process.env.SFCC_CIP_INSTANCE;

  if (!clientId || !clientSecret || !instance) {
    console.error('Error: Required environment variables: SFCC_CLIENT_ID, SFCC_CLIENT_SECRET, SFCC_CIP_INSTANCE');
    process.exit(1);
  }

  console.info(`Executing query: ${queryDef.name}`);
  console.info(`Parameters: ${JSON.stringify(params)}`);

  // Create client and execute query
  const client = new CIPClient(clientId, clientSecret, instance);
  
  try {
    await client.openConnection({});
    
    // Execute the business query
    const allData: any[] = [];
    
    for await (const batch of queryDef.execute(client, params)) {
      allData.push(...batch);
    }
    
    // Output results in requested format
    switch (format) {
      case 'json':
        outputAsJSON(allData);
        break;
      case 'csv':
        outputAsCSV(allData);
        break;
      default:
        outputAsTable(allData);
        break;
    }
    
    console.info(`\nQuery completed. Retrieved ${allData.length} rows.`);
    
  } finally {
    await client.closeConnection();
  }
}

async function main(): Promise<void> {
  try {
    const args = process.argv.slice(2);
    
    if (args.length === 0 || args[0] === '--help' || args[0] === '-h') {
      showHelp();
      return;
    }

    const command = args[0];
    const commandArgs = args.slice(1);

    switch (command) {
      case 'sql':
        await executeSqlCommand(commandArgs);
        break;
      case 'query':
        await executeQueryCommand(commandArgs);
        break;
      default:
        console.error(`Error: Unknown command "${command}"`);
        console.error('Valid commands: sql, query');
        showHelp();
        process.exit(1);
    }
    
  } catch (error) {
    console.error('Error:', error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}