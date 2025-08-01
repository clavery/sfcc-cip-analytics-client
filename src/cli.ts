#!/usr/bin/env node

import { parseArgs } from 'util';
import { CIPClient } from './cip-client';
import { processFrame } from './utils';
import { NormalizedFrame } from './normalized-types';
import { formatDateForSQL } from './data/types';

interface ParsedDate {
  date: Date;
  formatted: string;
}

function parseHumanDate(input: string): ParsedDate {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  
  // Handle relative dates
  const lowerInput = input.toLowerCase().trim();
  
  if (lowerInput === 'today') {
    return { date: today, formatted: formatDateForSQL(today) };
  }
  
  if (lowerInput === 'yesterday') {
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    return { date: yesterday, formatted: formatDateForSQL(yesterday) };
  }
  
  // Handle "N days ago"
  const daysAgoMatch = lowerInput.match(/^(\d+)\s*days?\s*ago$/);
  if (daysAgoMatch) {
    const daysAgo = parseInt(daysAgoMatch[1]);
    const date = new Date(today);
    date.setDate(date.getDate() - daysAgo);
    return { date, formatted: formatDateForSQL(date) };
  }
  
  // Handle "last week", "last month"
  if (lowerInput === 'last week') {
    const date = new Date(today);
    date.setDate(date.getDate() - 7);
    return { date, formatted: formatDateForSQL(date) };
  }
  
  if (lowerInput === 'last month') {
    const date = new Date(today);
    date.setMonth(date.getMonth() - 1);
    return { date, formatted: formatDateForSQL(date) };
  }
  
  // Try to parse as a regular date
  const parsed = new Date(input);
  if (isNaN(parsed.getTime())) {
    throw new Error(`Unable to parse date: "${input}". Try formats like "2024-01-15", "today", "yesterday", "3 days ago", "last week"`);
  }
  
  return { date: parsed, formatted: formatDateForSQL(parsed) };
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
  cip-query [options] <sql>
  cip-query [options] < file.sql
  echo "SELECT * FROM table" | cip-query [options]

Arguments:
  sql                SQL query to execute (required, or read from stdin)

Options:
  --from <date>      From date for <FROM> placeholder (e.g., "2024-01-01", "today", "yesterday", "3 days ago")
  --to <date>        To date for <TO> placeholder (e.g., "2024-01-31", "today", "last week")
  --format <type>    Output format: table (default), json, csv
  --help             Show this help message

Environment Variables:
  SFCC_CLIENT_ID     Your SFCC client ID (required)
  SFCC_CLIENT_SECRET Your SFCC client secret (required)
  SFCC_CIP_INSTANCE  Your SFCC CIP instance (required)
  SFCC_DEBUG         Enable debug logging (optional)

Examples:
  # Command line argument
  cip-query "SELECT * FROM ccdw_aggr_ocapi_request LIMIT 10"
  
  # From stdin with heredoc
  cip-query --format json --from "yesterday" --to "today" <<SQL
    SELECT * FROM ccdw_aggr_ocapi_request 
    WHERE request_date >= <FROM> AND request_date <= <TO>
  SQL
  
  # From file
  cip-query --format csv --from "2024-01-15" < query.sql
  
  # From pipe
  echo "SELECT api_name, COUNT(*) FROM ccdw_aggr_ocapi_request WHERE request_date = <FROM> GROUP BY api_name" | cip-query --from "2024-01-15"
`);
}

async function main(): Promise<void> {
  try {
    const { values, positionals } = parseArgs({
      args: process.argv.slice(2),
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
      const parsed = parseHumanDate(values.from);
      fromDate = parsed.formatted;
      console.info(`Using from date: ${parsed.formatted} (parsed from "${values.from}")`);
    }
    
    if (values.to) {
      const parsed = parseHumanDate(values.to);
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
    
  } catch (error) {
    console.error('Error:', error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}