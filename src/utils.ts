/**
 * Avatica protobuf utility functions for decoding and processing result data.
 */

import { Rep, ISignature, IFrame, IColumnMetaData } from './protocol';

/**
 * Converts an Avatica TypedValue into a standard JavaScript type.
 * @param columnValue The Avatica ColumnValue object containing the typed value.
 * @param columnMetadata Optional column metadata for enhanced type detection.
 * @returns The corresponding JavaScript primitive or object.
 */
export function decodeValue(columnValue: any, columnMetadata?: IColumnMetaData): any {
  // Check if this is using the new scalar_value field
  if (columnValue.scalarValue) {
    const typedValue = columnValue.scalarValue;
    
    if (typedValue.null) {
      return null;
    }
    
    // Check which field actually has a value based on the type
    switch (typedValue.type) {
      case Rep.BOOLEAN:
      case Rep.PRIMITIVE_BOOLEAN:
        return typedValue.boolValue;
      case Rep.STRING:
        return typedValue.stringValue;
      case Rep.BYTE:
      case Rep.PRIMITIVE_BYTE:
      case Rep.SHORT:
      case Rep.PRIMITIVE_SHORT:
      case Rep.INTEGER:
      case Rep.PRIMITIVE_INT:
      case Rep.LONG:
      case Rep.PRIMITIVE_LONG:
      case Rep.BIG_INTEGER:
      case Rep.NUMBER:
        // Check if this is actually a date/timestamp based on column metadata
        if (columnMetadata?.type) {
          const avaticaType = columnMetadata.type;
          if (avaticaType.name === 'DATE' || avaticaType.id === 91) {
            // Date values stored as days since epoch (1970-01-01)
            const numValue = typeof typedValue.numberValue === 'object' && typedValue.numberValue.toNumber 
              ? typedValue.numberValue.toNumber() 
              : Number(typedValue.numberValue);
            return new Date(numValue * 24 * 60 * 60 * 1000);
          }
          if (avaticaType.name === 'TIMESTAMP' || avaticaType.id === 93) {
            // Timestamp values stored as milliseconds since epoch
            const numValue = typeof typedValue.numberValue === 'object' && typedValue.numberValue.toNumber 
              ? typedValue.numberValue.toNumber() 
              : Number(typedValue.numberValue);
            return new Date(numValue);
          }
        }
        // numberValue might be a Long object from protobuf
        if (typedValue.numberValue && typeof typedValue.numberValue === 'object' && typedValue.numberValue.toNumber) {
          return typedValue.numberValue.toNumber();
        }
        return Number(typedValue.numberValue);
      case Rep.FLOAT:
      case Rep.PRIMITIVE_FLOAT:
      case Rep.DOUBLE:
      case Rep.PRIMITIVE_DOUBLE:
      case Rep.BIG_DECIMAL:
        return typedValue.doubleValue;
      case Rep.BYTE_STRING:
        return typedValue.bytesValue;
      case Rep.ARRAY:
        return typedValue.arrayValue;
      case Rep.JAVA_SQL_DATE:
      case Rep.JAVA_UTIL_DATE:
        // Date values are typically stored as milliseconds since epoch
        if (typedValue.numberValue !== undefined) {
          const timestamp = typeof typedValue.numberValue === 'object' && typedValue.numberValue.toNumber 
            ? typedValue.numberValue.toNumber() 
            : Number(typedValue.numberValue);
          return new Date(timestamp);
        }
        if (typedValue.stringValue !== undefined) {
          return new Date(typedValue.stringValue);
        }
        return null;
      case Rep.JAVA_SQL_TIMESTAMP:
        // Timestamp values are typically stored as milliseconds since epoch
        if (typedValue.numberValue !== undefined) {
          const timestamp = typeof typedValue.numberValue === 'object' && typedValue.numberValue.toNumber 
            ? typedValue.numberValue.toNumber() 
            : Number(typedValue.numberValue);
          return new Date(timestamp);
        }
        if (typedValue.stringValue !== undefined) {
          return new Date(typedValue.stringValue);
        }
        return null;
      case Rep.JAVA_SQL_TIME:
        // Time values might be stored as milliseconds since midnight or as strings
        if (typedValue.numberValue !== undefined) {
          const timeMs = typeof typedValue.numberValue === 'object' && typedValue.numberValue.toNumber 
            ? typedValue.numberValue.toNumber() 
            : Number(typedValue.numberValue);
          // For time-only values, create a Date object for today with the given time
          const today = new Date();
          today.setHours(0, 0, 0, 0);
          return new Date(today.getTime() + timeMs);
        }
        if (typedValue.stringValue !== undefined) {
          return new Date(`1970-01-01T${typedValue.stringValue}`);
        }
        return null;
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
  
  return null;
}

/**
 * Processes an Avatica result frame into a more usable format (array of objects).
 * @param signature The signature from the execute response, containing column info.
 * @param frame The data frame containing rows of typed values.
 * @returns An array of objects, where each object represents a row with column names as keys.
 */
export function processFrame<T = any>(signature: ISignature | undefined, frame: IFrame | undefined): T[] {
  if (!signature || !frame || !frame.rows) {
    return [];
  }
  
  const columnNames = signature.columns?.map((c) => c.label || '') || [];

  return frame.rows.map((row) => {
    const rowObject: Record<string, any> = {};
    
    // row.value is an array of ColumnValue objects
    if (!row.value || !Array.isArray(row.value)) {
      return rowObject as T;
    }
    
    const decodedValues = row.value.map((colValue, index) => {
      const columnMetadata = signature.columns?.[index];
      return decodeValue(colValue, columnMetadata);
    });
    
    columnNames.forEach((name, i) => {
      if (name) {
        rowObject[name] = decodedValues[i];
      }
    });
    return rowObject as T;
  });
}
