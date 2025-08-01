export interface DateRange {
  startDate: Date;
  endDate: Date;
}

export interface QueryResult<T = any> {
  data: T[];
  totalRows: number;
}

export function formatDateForSQL(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

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