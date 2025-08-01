import { AvaticaProtobufClient } from '@sfcc-cip-analytics-client/avatica-client';
import { queryOcapiRequests, OcapiRequestRecord } from '@sfcc-cip-analytics-client/data/aggregate/ocapi';
import { getAccessToken, getAuthConfig, getAvaticaServerUrl } from '@sfcc-cip-analytics-client/auth';
import * as path from 'path';

async function main() {
  console.log('Getting access token...');

  const token = await getAccessToken();
  const config = getAuthConfig();
  const AVATICA_SERVER_URL = getAvaticaServerUrl();
  
  const protoFiles = [
    path.join(__dirname, '../proto/common.proto'),
    path.join(__dirname, '../proto/requests.proto'),
    path.join(__dirname, '../proto/responses.proto'),
  ];
  
  console.log('Creating Avatica client...');
  const client = await AvaticaProtobufClient.create(AVATICA_SERVER_URL, config.instance, protoFiles, token);
  console.log('Client created.');

  try {
    // Open connection
    console.log('\nOpening connection...');
    await client.openConnection({});
    console.log('Connection opened');

    // Example 1: Query all OCAPI requests using async generator
    console.log('\nExample 1: Querying all OCAPI requests (streaming)...');
    let totalCount = 0;
    let batchCount = 0;
    
    // Example 3: Query OCAPI requests for a specific date and collect all results
    const specificDate = new Date('2024-05-01');
    console.log(`\nExample 3: Collecting all OCAPI requests for ${specificDate.toISOString().split('T')[0]}...`);
    
    const allResults: OcapiRequestRecord[] = [];
    
    for await (const batch of queryOcapiRequests(
      client,
      { startDate: specificDate, endDate: specificDate },
      50
    )) {
      allResults.push(...batch);
      console.log(`  Collected ${batch.length} records (total: ${allResults.length})`);
    }
    
    console.log(`\nFound ${allResults.length} total requests on ${specificDate.toISOString().split('T')[0]}`);
    if (allResults.length > 0) {
      // Group by API name
      const apiGroups = allResults.reduce((acc, req) => {
        acc[req.api_name] = (acc[req.api_name] || 0) + 1;
        return acc;
      }, {} as Record<string, number>);
      
      console.log('\nRequests by API:');
      console.table(apiGroups);
    }

  } catch (error) {
    console.error('Error querying data:', error);
  } finally {
    // Always close the connection when done
    console.log('\nClosing connection...');
    await client.closeConnection();
    console.log('Connection closed.');
  }
}

main().catch(err => {
  console.error("Failed to run example:", err);
});
