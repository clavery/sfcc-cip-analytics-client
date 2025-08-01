/**
 * Avatica protobuf utility functions for decoding and processing result data.
 */

/**
 * Converts an Avatica TypedValue into a standard JavaScript type.
 * @param columnValue The Avatica ColumnValue object containing the typed value.
 * @returns The corresponding JavaScript primitive or object.
 */
export function decodeValue(columnValue: any): any {
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
 * Processes an Avatica result frame into a more usable format (array of objects).
 * @param signature The signature from the execute response, containing column info.
 * @param frame The data frame containing rows of typed values.
 * @returns An array of objects, where each object represents a row with column names as keys.
 */
export function processFrame<T = any>(signature: any, frame: any): T[] {
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
    return rowObject as T;
  });
}