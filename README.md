# SFCC Commerce Intelligence Platform Analytics Client

A TypeScript/Node.js client library and CLI tool for querying Salesforce Commerce Cloud (SFCC) Commerce Intelligence Platform (CIP) analytics data through the use of the JDBC protocol. This client abstracts the underlying Avatica protobuf protocol and provides both programmatic and command-line interfaces for accessing your commerce analytics data.

See [https://developer.salesforce.com/docs/commerce/b2c-commerce/guide/jdbc_intro.html](https://developer.salesforce.com/docs/commerce/b2c-commerce/guide/jdbc_intro.html) for the details of the JDBC driver used with Java applications as well as the business object and schema reference.

![Screenshot of CLI output](https://raw.githubusercontent.com/clavery/sfcc-cip-analytics-client/refs/heads/main/assets/screenshot.png)

> [!IMPORTANT]  
> Many queries may not work and return an SQL error. This is a work in progress to discover the proper queries applied the CIP schemas.


## Installation

> [!IMPORTANT]  
> This is currently NOT published to npm at the package below. Instead install it manually by cloning the repo or release artifacts and using file: urls
> ```
> git clone ...
> cd my-project
> npm install ../sfcc-cip-analytics-client
> ```


```bash
npm install sfcc-cip-analytics-client
```

### Global CLI Installation

> [!IMPORTANT]  
> This is currently NOT published to npm at the package below. Instead install it manually by cloning the repo or release artifacts and installing with npm install
> ```
> git clone ...
> cd sfcc-cip-analytics-client
> npm install -g .
> cip-query --help
> ```


```bash
npm install -g sfcc-cip-analytics-client
# Now you can use: cip-query "SELECT * FROM table"
```

## CLI Usage

### Environment Variables

Set the following environment variables. Your Account Manager API client must have the `SALESFORCE_COMMERCE_API:[instance]` role and tenant filter.

```bash
export SFCC_CLIENT_ID="your-client-id"
export SFCC_CLIENT_SECRET="your-client-secret"
export SFCC_CIP_INSTANCE="your-instance-name" # example abcd_prd

# Optional: Enable debug logging
export SFCC_DEBUG=true
```

### Commands

The CLI supports two main commands:

#### `sql` - Execute custom SQL queries

```bash
cip-query sql "SELECT * FROM ccdw_aggr_ocapi_request LIMIT 10"
```

#### `query` - Execute predefined business object queries

```bash
cip-query query ocapi-requests --from "2024-01-01" --to "2024-01-31"
```

### Basic Query

```bash
cip-query sql "SELECT * FROM ccdw_aggr_ocapi_request LIMIT 10"
```

### With Date Placeholders

```bash
cip-query sql --from "2024-07-01" --to "2024-07-31" \
  "SELECT * FROM ccdw_aggr_ocapi_request WHERE request_date >= <FROM> AND request_date <= <TO>"
```

### Different Output Formats

```bash
# JSON output
cip-query sql --format json "SELECT api_name, COUNT(*) as count FROM ccdw_aggr_ocapi_request GROUP BY api_name"

# CSV output for spreadsheet analysis
cip-query sql --format csv --from "last week" \
  "SELECT * FROM ccdw_aggr_ocapi_request WHERE request_date >= <FROM>"
```

### Using Heredocs (Multi-line SQL)

```bash
cip-query sql --format json --from "2024-01-01" --to "2024-01-31" <<SQL
  SELECT 
    api_name,
    "method",
    AVG(response_time) as avg_response_time,
    COUNT(*) as request_count
  FROM ccdw_aggr_ocapi_request 
  WHERE request_date >= <FROM> 
    AND request_date <= <TO>
  GROUP BY api_name, "method"
  ORDER BY request_count DESC
SQL
```

### From Files

```bash
# Save complex queries in .sql files
cip-query sql --format csv < my-analytics-query.sql
```

## API Usage

### Low Level Client

```typescript
import { CIPClient } from 'sfcc-cip-analytics-client';

const client = new CIPClient(
  process.env.SFCC_CLIENT_ID!,
  process.env.SFCC_CLIENT_SECRET!,
  process.env.SFCC_CIP_INSTANCE!
);

async function queryData() {
  try {
    await client.openConnection();
    const statementId = await client.createStatement();
    
    const result = await client.execute(
      statementId, 
      "SELECT * FROM ccdw_aggr_ocapi_request LIMIT 10"
    );
    
    // see example/basic.ts for more details on result structure
    console.table(result);
    
    await client.closeStatement(statementId);
  } finally {
    await client.closeConnection();
  }
}

queryData();
```

### Business Use Case Queries

The client provides specialized query functions for common business analytics use cases. These return simple arrays of plain old JavaScript objects, making it easy to work with the data.

For complete documentation of all available business queries, see [Business Queries Reference](./docs/BUSINESS_QUERIES.md).

```typescript
import { CIPClient, querySalesAnalytics } from 'sfcc-cip-analytics-client';

const client = new CIPClient(clientId, clientSecret, instance);

async function analyzeSalesData() {
  await client.openConnection();
  
  const query = querySalesAnalytics(
    client,
    'mysite', // site ID
    { startDate: new Date('2024-01-01'), endDate: new Date('2024-01-31') },
    100 // batch size
  );
  
  for await (const batch of query) {
    console.log(`Processed ${batch.length} daily sales metrics`);
    // Each record has: date, std_revenue, orders, std_aov, units, aos
  }
  
  await client.closeConnection();
}
```

## Examples

See the [`examples/`](./examples/) directory for complete working examples:

- [`examples/basic.ts`](./examples/basic.ts) - Basic usage with result pagination
- [`examples/ocapi.ts`](./examples/ocapi.ts) - Using data helper functions for OCAPI analytics

## Development Setup

### Prerequisites

- Node.js 18+ or Bun
- TypeScript knowledge
- SFCC Commerce Cloud production access with CIP Analytics enabled

### Getting Started

1. **Clone and install**
   ```bash
   git clone <repository-url>
   # ...
   npm install
   ```

3. **Build the project**
   ```bash
   npm run build
   ```

4. **Run linting**
   ```bash
   npm run lint
   ```

## API Documentation

### CIPClient

Main client class for interacting with CIP.

#### Constructor

```typescript
new CIPClient(clientId: string, clientSecret: string, instance: string, options?: CIPClientOptions)
```

#### Methods

- `openConnection(info?: IConnectionProperties): Promise<void>` - Open database connection
- `closeConnection(): Promise<void>` - Close database connection  
- `createStatement(): Promise<number>` - Create a new SQL statement
- `closeStatement(statementId: number): Promise<void>` - Close a statement
- `execute(statementId: number, sql: string, maxRowCount?: number): Promise<NormalizedExecuteResponse>` - Execute SQL query
- `fetch(statementId: number, offset: number, fetchMaxRowCount: number): Promise<NormalizedFetchResponse>` - Fetch more results

## Todo

- [ ] static build of protobuf for web use
- [ ] prepared statements for business use cases
- [ ] packaging; release artifacts
- [ ] CI/CD actions
- [ ] testing
- [ ] Implement more business object helpers for primary documented use cases


## License

Licensed under the current NDA and licensing agreement in place with your organization. (This is explicitly **not** open source licensing.)

### Support

**This project should not be treated as Salesforce Product.** It is a tool B2C instances. Customers and partners implement this at-will with no expectation of roadmap, technical support, defect resolution, production-style SLAs.

This project is maintained by the **Salesforce Community**. Salesforce Commerce Cloud or Salesforce Platform Technical Support do not support this project or its setup.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED.

For feature requests or bugs, please open a [GitHub issue](https://github.com/clavery/sfcc-cip-analytics-client/issues). 
