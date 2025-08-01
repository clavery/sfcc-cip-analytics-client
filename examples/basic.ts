// examples/basic.ts - Basic usage example

import { NormalizedFrame } from '@sfcc-cip-analytics-client';
import { CIPClient } from '@sfcc-cip-analytics-client/cip-client';
import { decodeValue, processFrame } from '@sfcc-cip-analytics-client/utils';
import * as path from 'path';

// --- Main Execution Logic ---
async function main() {
  const clientId = process.env.SFCC_CLIENT_ID;
  const clientSecret = process.env.SFCC_CLIENT_SECRET;
  const instance = process.env.SFCC_CIP_INSTANCE;

  if (!clientId || !clientSecret || !instance) {
    throw new Error('Required environment variables: SFCC_CLIENT_ID, SFCC_CLIENT_SECRET, SFCC_CIP_INSTANCE');
  }
  
  console.log('Creating Avatica client...');
  const client = new CIPClient(clientId, clientSecret, instance);
  console.log('Client created.');

  try {
    // 1. Open Connection
    console.log('\n1. Opening connection...');
    // Add credentials if your server requires them
    await client.openConnection({});
    console.log('   Connection opened');

    // Create a statement first
    console.log('Creating a statement...');
    const statementId = await client.createStatement();
    console.log(`   Statement created with ID: ${statementId}`);
    // 2. Execute a Query
    console.log('\n2. Executing a query...');
    // const sql = "SELECT customer_id, login, email_address FROM ccdw_dim_customer"; 
    //const sql = "SELECT * FROM ccdw_dim_user_agent"; 
    const sql = "SELECT * FROM ccdw_aggr_ocapi_request"; 
    const executeResponse = await client.execute(statementId, sql);
    console.log('   Query executed.');

    // 3. Process Results
    console.log('\n3. Processing results...');
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      
      if (!result.firstFrame) {
        console.log('   No data frame returned.');
        await client.closeStatement(statementId);
        return;
      }
      
      const data = processFrame(result.signature, result.firstFrame);
      console.log('   Decoded Data:');
      console.table(data);

      let done = result.firstFrame.done;
      let currentFrame : NormalizedFrame|undefined = result.firstFrame;
      
      // Example of fetching more data if the result set was large
      while (!done && currentFrame) {
          console.log('\n   Result set is not complete, fetching next frame...');
          
          const currentOffset = currentFrame.offset || 0;
          const currentRowCount = currentFrame.rows?.length || 0;
          
          const nextResponse = await client.fetch(
              result.statementId || 0, 
              currentOffset + currentRowCount,
              100 // fetch next 100 rows
          );
          
          currentFrame = nextResponse.frame;
          if (!currentFrame) {
              console.log('   No more frames available.');
              break;
          }
          
          const nextData = processFrame(result.signature, currentFrame);
          done = currentFrame.done;
          console.log('   Next Frame Data:');
          console.table(nextData);
      }

      await client.closeStatement(statementId);
    } else {
      console.log('   Query executed, but no result sets were returned.');
    }

  } catch (error) {
    console.error('\n--- An error occurred ---');
    console.error(error);
  } finally {
    // 5. Close Connection
    console.log('\n5. Closing connection...');
    await client.closeConnection();
    console.log('   Connection closed.');
  }
}

main().catch(err => {
    console.error("Failed to run example:", err);
});
