# Integration Tests for CIP Analytics

This directory contains integration tests that validate all business use case functions against the live CIP backend. These tests ensure that SQL queries are syntactically correct and return expected data structures.

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
```

## Running Tests

### All Integration Tests
```bash
npm run test:integration
```
