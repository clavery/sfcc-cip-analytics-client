# Integration Tests for CIP Analytics

This directory contains integration tests that validate all business use case functions against the live CIP backend. These tests ensure that SQL queries are syntactically correct and return expected data structures.

## Overview

The integration tests execute all 13 refactored analytics functions:

- **Customer Analytics** (3 functions)
- **Product Analytics** (4 functions) 
- **Promotion Analytics** (3 functions)
- **Recommendation Analytics** (3 functions)

## Rate Limiting

The CIP backend limits to **10 queries per minute**. Our tests automatically handle this by:
- Tracking request timestamps in a sliding window
- Automatically waiting when rate limits are reached
- Showing progress indicators during wait periods

## Environment Setup

Set these environment variables before running tests:

```bash
export SFCC_CLIENT_ID=your_client_id
export SFCC_CLIENT_SECRET=your_client_secret  
export SFCC_CIP_INSTANCE=your_instance_url
export TEST_SITE_ID=your_site_id

# Optional: Control test behavior
export TEST_TIMEOUT=30000           # Test timeout in ms (default: 30s)
export ENABLE_SLOW_TESTS=true      # Enable longer running tests
```

## Running Tests

### All Integration Tests
```bash
npm run test:integration
```

### Individual Test Suites
```bash
npm run test:integration:customer       # Customer analytics only
npm run test:integration:product        # Product analytics only  
npm run test:integration:promotion      # Promotion analytics only
npm run test:integration:recommendation # Recommendation analytics only
```

### Manual Execution
```bash
# Run all tests
npx tsx tests/integration/run-all.ts

# Run specific test suite
npx tsx tests/integration/customer-analytics.test.ts
```

## Test Structure

Each test suite follows this pattern:

1. **Setup**: Connect to CIP backend
2. **Execute**: Run each function with test parameters
3. **Validate**: Check result structure against expected schema
4. **Report**: Generate summary with pass/fail status
5. **Cleanup**: Close connections

## Test Parameters

Tests use these default parameters:
- **Date Range**: 2024-01-01 to 2024-04-01 (fixed for consistent testing)
- **Site ID**: From TEST_SITE_ID environment variable
- **Batch Size**: 10 (for faster test execution)
- **Max Batches**: 3 (prevents excessive data in tests)

## Validation Rules

Each function has validation rules that check:
- **Required Fields**: Ensures expected columns are present
- **Data Types**: Validates numeric fields are actually numbers
- **Result Count**: Warns if no data returned

