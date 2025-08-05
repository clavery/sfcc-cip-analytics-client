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

/**
 * Apache calcite does not seem to like trailing semicolons in SQL queries
 * @param sql The SQL string to clean
 * @returns Cleaned SQL string with normalized whitespace
 */
export function cleanSQL(sql: string): string {
  // Remove trailing semicolons, matching across multiple lines
  sql = sql.replace(/;\s*$/, '');
  return sql;
}
