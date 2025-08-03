# Business Queries Reference

This document contains examples of business-specific query functions available in the CIP Analytics Client. These queries are designed to answer common business questions and are built on top of the low-level CIPClient.

## Common Setup

All business queries require a connected CIPClient instance:

```typescript
import { CIPClient } from 'sfcc-cip-analytics-client';

const client = new CIPClient(
  process.env.SFCC_CLIENT_ID!,
  process.env.SFCC_CLIENT_SECRET!,
  process.env.SFCC_CIP_INSTANCE!
);

await client.openConnection();

// Don't forget to close the connection when done
// await client.closeConnection();
```

## Technical Analytics

### queryOcapiRequests

**Business Question:** How are our APIs performing in terms of response times and request volumes?

**Primary Users:** Technical teams, Operations

**API Usage:**
```typescript
import { queryOcapiRequests } from 'sfcc-cip-analytics-client';

const query = queryOcapiRequests(
  client,
  { dateRange: { startDate: new Date('2024-01-01'), endDate: new Date('2024-01-31') } },
  100
);

for await (const batch of query) {
  console.log(`Processed ${batch.length} OCAPI requests`);
  // Each record has: request_date, api_name, api_resource, status_code, response_time, etc.
}
```

**CLI Usage:**
```bash
cip-query query --name ocapi-requests --from "2024-01-01" --to "2024-01-31"
```

## Sales Analytics

### querySalesAnalytics

**Business Question:** How is my business performing across revenue and orders?

**Primary Users:** Merchandising teams

**API Usage:**
```typescript
import { querySalesAnalytics } from 'sfcc-cip-analytics-client';

const query = querySalesAnalytics(
  client,
  {
    siteId: 'mysite',
    dateRange: { startDate: new Date('2024-01-01'), endDate: new Date('2024-01-31') }
  },
  100
);

for await (const batch of query) {
  console.log(`Processed ${batch.length} daily sales metrics`);
  // Each record has: date, std_revenue, orders, std_aov, units, aos, std_tax, std_shipping
}
```

**CLI Usage:**
```bash
cip-query query --name sales-analytics --param siteId=mysite --from "2024-01-01" --to "2024-01-31"
```

### querySalesSummary

**Business Question:** What are the detailed sales patterns by customer segments and device types?

**Primary Users:** Merchandising teams, Analytics teams

**API Usage:**
```typescript
import { querySalesSummary } from 'sfcc-cip-analytics-client';

const query = querySalesSummary(
  client,
  {
    dateRange: { startDate: new Date('2024-01-01'), endDate: new Date('2024-01-31') },
    siteId: 'mysite',
    deviceClassCode: 'mobile',
    registered: true
  },
  100
);

for await (const batch of query) {
  console.log(`Processed ${batch.length} sales summary records`);
  // Each record has: submit_date, site_id, registered, device_class_code, std_revenue, num_orders, etc.
}
```

**CLI Usage:**
```bash
cip-query query --name sales-summary --param siteId=mysite --param deviceClassCode=mobile --param registered=true --from "2024-01-01" --to "2024-01-31"
```

## Search Analytics

### querySearchQueryPerformance

**Business Question:** Which search terms drive revenue vs which represent missed opportunities?

**Primary Users:** Merchandising and UX teams

**API Usage:**
```typescript
import { querySearchQueryPerformance } from 'sfcc-cip-analytics-client';

// Query successful searches (has_results = true)
const successfulSearches = querySearchQueryPerformance(
  client,
  {
    siteId: 'mysite',
    dateRange: { startDate: new Date('2024-01-01'), endDate: new Date('2024-01-31') },
    hasResults: true
  },
  100
);

for await (const batch of successfulSearches) {
  console.log(`Processed ${batch.length} successful search queries`);
  // Each record has: query, converted_searches, orders, std_revenue, conversion_rate
}
```

**CLI Usage:**
```bash
cip-query query --name search-query-performance --param siteId=mysite --param hasResults=true --from "2024-01-01" --to "2024-01-31"
```


## Customer Analytics

### queryCustomerRegistrationTrends

**Business Question:** How effectively are we acquiring new customers and what drives registrations?

**Primary Users:** Marketing and CRM teams

**API Usage:**
```typescript
import { queryCustomerRegistrationTrends } from 'sfcc-cip-analytics-client';

const query = queryCustomerRegistrationTrends(
  client,
  {
    siteId: 'mysite',
    dateRange: { startDate: new Date('2024-01-01'), endDate: new Date('2024-01-31') }
  },
  100
);

for await (const batch of query) {
  console.log(`Processed ${batch.length} registration trend records`);
}
```

**CLI Usage:**
```bash
cip-query query --name customer-registration-trends --param siteId=mysite --from "2024-01-01" --to "2024-01-31"
```

## Payment Analytics

### queryPaymentMethodPerformance

**Business Question:** Which payment methods are most successful and preferred by customers?

**Primary Users:** Finance and Operations teams

**API Usage:**
```typescript
import { queryPaymentMethodPerformance } from 'sfcc-cip-analytics-client';

const query = queryPaymentMethodPerformance(
  client,
  {
    siteId: 'mysite',
    dateRange: { startDate: new Date('2024-01-01'), endDate: new Date('2024-01-31') }
  },
  100
);
```

**CLI Usage:**
```bash
cip-query query --name payment-method-performance --param siteId=mysite --from "2024-01-01" --to "2024-01-31"
```

## Traffic Analytics

### queryTopReferrers

**Business Question:** Which traffic sources are driving the most valuable customers?

**Primary Users:** Marketing and Analytics teams

**API Usage:**
```typescript
import { queryTopReferrers } from 'sfcc-cip-analytics-client';

const query = queryTopReferrers(
  client,
  {
    siteId: 'mysite',
    dateRange: { startDate: new Date('2024-01-01'), endDate: new Date('2024-01-31') },
    limit: 25
  },
  100
);
```

**CLI Usage:**
```bash
cip-query query --name top-referrers --param siteId=mysite --param limit=25 --from "2024-01-01" --to "2024-01-31"
```

## Product Analytics

### queryTopSellingProducts

**Business Question:** How do my products perform across different channels?

**Primary Users:** Buyers and Merchandising teams

**API Usage:**
```typescript
import { queryTopSellingProducts } from 'sfcc-cip-analytics-client';

const query = queryTopSellingProducts(
  client,
  {
    siteId: 'mysite',
    dateRange: { startDate: new Date('2024-01-01'), endDate: new Date('2024-01-31') }
  },
  100
);

for await (const batch of query) {
  console.log(`Processed ${batch.length} top selling products`);
  // Each record has: nproduct_id, product_display_name, units_sold, std_revenue, order_count, device_class_code, registered, nsite_id
}
```

**CLI Usage:**
```bash
cip-query query --name top-selling-products --param siteId=mysite --from "2024-01-01" --to "2024-01-31"
```

### queryProductCoPurchaseAnalysis

**Business Question:** Which products are frequently bought together?

**Primary Users:** Merchandising and Product teams

**API Usage:**
```typescript
import { queryProductCoPurchaseAnalysis } from 'sfcc-cip-analytics-client';

const query = queryProductCoPurchaseAnalysis(
  client,
  {
    siteId: 'mysite',
    dateRange: { startDate: new Date('2024-01-01'), endDate: new Date('2024-01-31') }
  },
  100
);

for await (const batch of query) {
  console.log(`Processed ${batch.length} product co-purchase records`);
  // Each record has: product_1_id, product_1_name, product_2_id, product_2_name, co_purchase_count, std_cobuy_revenue
}
```

**CLI Usage:**
```bash
cip-query query --name product-co-purchase-analysis --param siteId=mysite --from "2024-01-01" --to "2024-01-31"
```

## Promotion Analytics

### queryPromotionDiscountAnalysis

**Business Question:** Are my promotions driving incremental sales or just discounting existing sales?

**Primary Users:** Marketing and Merchandising teams

**API Usage:**
```typescript
import { queryPromotionDiscountAnalysis } from 'sfcc-cip-analytics-client';

const query = queryPromotionDiscountAnalysis(
  client,
  {
    dateRange: { startDate: new Date('2024-01-01'), endDate: new Date('2024-01-31') }
  },
  100
);

for await (const batch of query) {
  console.log(`Processed ${batch.length} promotion discount records`);
  // Each record has: submit_day, total_orders, promotion_class, std_total_discount, promotion_orders, avg_discount_per_order
}
```

**CLI Usage:**
```bash
cip-query query --name promotion-discount-analysis --from "2024-01-01" --to "2024-01-31"
```

## Additional Business Queries

All available business queries can be listed using:

```bash
cip-query query --list
```

This will show all available queries organized by category, along with their required and optional parameters. Each query follows the same patterns shown above with dedicated TypeScript functions and CLI commands.