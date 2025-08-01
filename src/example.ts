// src/example.ts

import { AvaticaProtobufClient } from './avatica-client';
import { getAccessToken, getAuthConfig, getAvaticaServerUrl } from './auth';
import * as path from 'path';

// --- Helper Functions to Process Results ---

/**
 * Converts an Avatica TypedValue into a standard JavaScript type.
 * @param typedValue The Avatica TypedValue object.
 * @returns The corresponding JavaScript primitive or object.
 */
function decodeValue(columnValue: any): any {
  // Check if this is using the new scalar_value field
  if (columnValue.scalarValue) {
    const typedValue = columnValue.scalarValue;
    
    if (typedValue.null) {
      return null;
    }
    
    // Check which field actually has a value based on the type
    switch (typedValue.type) {
      case 'BOOLEAN':
      case 'PRIMITIVE_BOOLEAN':
        return typedValue.boolValue;
      case 'STRING':
        return typedValue.stringValue;
      case 'BYTE':
      case 'PRIMITIVE_BYTE':
      case 'SHORT':
      case 'PRIMITIVE_SHORT':
      case 'INTEGER':
      case 'PRIMITIVE_INT':
      case 'LONG':
      case 'PRIMITIVE_LONG':
      case 'BIG_INTEGER':
      case 'NUMBER':
        // numberValue might be a Long object from protobuf
        if (typedValue.numberValue && typeof typedValue.numberValue === 'object' && typedValue.numberValue.toNumber) {
          return typedValue.numberValue.toNumber();
        }
        return Number(typedValue.numberValue);
      case 'FLOAT':
      case 'PRIMITIVE_FLOAT':
      case 'DOUBLE':
      case 'PRIMITIVE_DOUBLE':
      case 'BIG_DECIMAL':
        return typedValue.doubleValue;
      case 'BYTE_STRING':
        return typedValue.bytesValue;
      case 'ARRAY':
        return typedValue.arrayValue;
      default:
        // Try to find any defined value
        if (typedValue.stringValue !== undefined && typedValue.stringValue !== '') return typedValue.stringValue;
        if (typedValue.numberValue !== undefined) {
          if (typedValue.numberValue && typeof typedValue.numberValue === 'object' && typedValue.numberValue.toNumber) {
            return typedValue.numberValue.toNumber();
          }
          return Number(typedValue.numberValue);
        }
        if (typedValue.doubleValue !== undefined) return typedValue.doubleValue;
        if (typedValue.bytesValue !== undefined) return typedValue.bytesValue;
        if (typedValue.arrayValue !== undefined) return typedValue.arrayValue;
        if (typedValue.boolValue !== undefined) return typedValue.boolValue;
    }
    
    return null;
  }
  
  // Fall back to the deprecated value field (which is an array)
  if (columnValue.value && columnValue.value.length > 0) {
    const typedValue = columnValue.value[0];
    
    if (typedValue.null) {
      return null;
    }
    
    // Use the same logic as above
    switch (typedValue.type) {
      case 'BOOLEAN':
      case 'PRIMITIVE_BOOLEAN':
        return typedValue.boolValue;
      case 'STRING':
        return typedValue.stringValue;
      case 'BYTE':
      case 'PRIMITIVE_BYTE':
      case 'SHORT':
      case 'PRIMITIVE_SHORT':
      case 'INTEGER':
      case 'PRIMITIVE_INT':
      case 'LONG':
      case 'PRIMITIVE_LONG':
      case 'BIG_INTEGER':
      case 'NUMBER':
        if (typedValue.numberValue && typeof typedValue.numberValue === 'object' && typedValue.numberValue.toNumber) {
          return typedValue.numberValue.toNumber();
        }
        return Number(typedValue.numberValue);
      case 'FLOAT':
      case 'PRIMITIVE_FLOAT':
      case 'DOUBLE':
      case 'PRIMITIVE_DOUBLE':
      case 'BIG_DECIMAL':
        return typedValue.doubleValue;
      case 'BYTE_STRING':
        return typedValue.bytesValue;
      case 'ARRAY':
        return typedValue.arrayValue;
    }
  }
  
  return null;
}

/**
 * Processes a result frame into a more usable format (array of objects).
 * @param signature The signature from the execute response, containing column info.
 * @param frame The data frame containing rows of typed values.
 * @returns An array of objects, where each object represents a row.
 */
function processFrame(signature: any, frame: any): Record<string, any>[] {
  const columnNames = signature.columns.map((c: any) => c.label);
  
  if (!frame || !frame.rows) {
      return [];
  }

  return frame.rows.map((row: any) => {
    const rowObject: Record<string, any> = {};
    
    // row.value is an array of ColumnValue objects
    if (!row.value || !Array.isArray(row.value)) {
      return rowObject;
    }
    
    const decodedValues = row.value.map((colValue: any) => decodeValue(colValue));
    columnNames.forEach((name: string, i: number) => {
      rowObject[name] = decodedValues[i];
    });
    return rowObject;
  });
}


// --- Main Execution Logic ---
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

  let connectionId: string | null = null;
  
  try {
    // 1. Open Connection
    console.log('\n1. Opening connection...');
    // Add credentials if your server requires them
    connectionId = await client.openConnection({
    });
    console.log(`   Connection opened with ID: ${connectionId}`);

    // Create a statement first
    console.log('Creating a statement...');
    const statementId = await client.createStatement(connectionId)
    console.log(`   Statement created with ID: ${statementId}`);
    // 2. Execute a Query
    console.log('\n2. Executing a query...');
    // const sql = "SELECT customer_id, login, email_address FROM ccdw_dim_customer"; 
    //const sql = "SELECT * FROM ccdw_dim_user_agent"; 
    const sql = "SELECT * FROM ccdw_aggr_ocapi_request"; 
    const executeResponse = await client.execute(connectionId, statementId, sql);
    console.log('   Query executed.');

    // 3. Process Results
    console.log('\n3. Processing results...');
    if (executeResponse.results && executeResponse.results.length > 0) {
      const result = executeResponse.results[0];
      const data = processFrame(result.signature, result.firstFrame);
      console.log('   Decoded Data:');
      console.table(data);

      var done = result.firstFrame.done;
      var _result = result.firstFrame;
      // Example of fetching more data if the result set was large
      while (!done) {
          console.log('\n   Result set is not complete, fetching next frame...');
          const nextResponse = await client.fetch(
              connectionId, 
              result.statementId, 
              _result.offset + _result.rows.length,
              100 // fetch next 100 rows
          );
          _result = nextResponse.frame;
          if (!_result) {
              console.log('   No more frames available.');
              break;
          }
          const nextData = processFrame(result.signature, _result);
          done = _result.done;
          console.log('   Next Frame Data:');
          console.table(nextData);
      }

      await client.closeStatement(connectionId, statementId);
    } else {
      console.log('   Query executed, but no result sets were returned.');
    }

  } catch (error) {
    console.error('\n--- An error occurred ---');
    console.error(error);
  } finally {
    // 5. Close Connection
    if (connectionId) {
      console.log('\n5. Closing connection...');
      await client.closeConnection(connectionId);
      console.log('   Connection closed.');
    }
  }
}

main().catch(err => {
    console.error("Failed to run example:", err);
});
