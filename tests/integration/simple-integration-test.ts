import { CIPClient } from "../../src/cip-client";
import { globalRateLimiter } from "./rate-limiter";
import { getTestConfig, TEST_DATA } from "./test-config";
import {
  queryCustomerRegistrationTrends,
  queryPaymentMethodPerformance,
  queryTopSellingProducts,
  queryProductCoPurchaseAnalysis,
  queryPromotionDiscountAnalysis,
  querySalesAnalytics,
  querySalesSummary,
  querySearchQueryPerformance,
  queryTopReferrers,
  queryOcapiRequests,
  SiteSpecificQueryTemplateParams,
  QueryTemplateParams,
} from "../../src/data";

interface TestResult {
  functionName: string;
  success: boolean;
  error?: string;
  sql?: string;
  executionTime?: number;
}

interface SearchQueryPerformanceParams extends SiteSpecificQueryTemplateParams {
  hasResults: boolean;
}

async function testQueryFunction<T, P>(
  name: string,
  fn: {
    (
      client: CIPClient,
      params: P,
      batchSize?: number,
    ): AsyncGenerator<T[], void, unknown>;
    QUERY: (params: P) => { sql: string; parameters: unknown[] };
  },
  client: CIPClient,
  params: P,
  batchSize: number,
): Promise<TestResult> {
  const startTime = Date.now();
  let sql = "";

  try {
    // Get the SQL query for logging
    try {
      const queryResult = fn.QUERY(params);
      sql = queryResult.sql;
      console.log(`[${name}] SQL Query:`);
      console.log(sql.trim());
      console.log("");
    } catch (sqlError) {
      console.log(`[${name}] Could not extract SQL: ${sqlError}`);
    }

    // Execute the query function with small batch size
    const generator = fn(client, params, batchSize);
    const firstBatch = await generator.next();

    // We only need to verify the function can execute, not process all data
    if (!firstBatch.done) {
      console.log(
        `✅ [${name}] SUCCESS - Retrieved ${firstBatch.value.length} records`,
      );
      return {
        functionName: name,
        success: true,
        sql,
        executionTime: Date.now() - startTime,
      };
    } else {
      console.log(`✅ [${name}] SUCCESS - No data returned (empty result set)`);
      return {
        functionName: name,
        success: true,
        sql,
        executionTime: Date.now() - startTime,
      };
    }
  } catch (error) {
    console.log(`❌ [${name}] FAILED - ${error}`);
    return {
      functionName: name,
      success: false,
      error: String(error),
      sql,
      executionTime: Date.now() - startTime,
    };
  }
}

/**
 * Simplified integration test runner that tests all business object query functions
 */
async function runIntegrationTests(): Promise<TestResult[]> {
  const config = getTestConfig();
  const client = new CIPClient(
    config.clientId,
    config.clientSecret,
    config.instance,
  );

  // Connect to client
  await client.openConnection({});

  const results: TestResult[] = [];

  console.log(`Starting integration tests for 10 query functions...`);
  console.log(`Rate limit: max 10 requests per minute\n`);

  const baseParams = {
    siteId: config.defaultSiteId,
    dateRange: config.defaultDateRange,
  };
  const searchParams: SearchQueryPerformanceParams = {
    ...baseParams,
    hasResults: true,
  };

  // Test each function individually with proper typing
  await globalRateLimiter.waitForNextSlot();
  results.push(
    await testQueryFunction(
      "queryCustomerRegistrationTrends",
      queryCustomerRegistrationTrends,
      client,
      baseParams,
      TEST_DATA.batchSize,
    ),
  );
  console.log("---");

  await globalRateLimiter.waitForNextSlot();
  results.push(
    await testQueryFunction(
      "queryPaymentMethodPerformance",
      queryPaymentMethodPerformance,
      client,
      baseParams,
      TEST_DATA.batchSize,
    ),
  );
  console.log("---");

  await globalRateLimiter.waitForNextSlot();
  results.push(
    await testQueryFunction(
      "queryTopSellingProducts",
      queryTopSellingProducts,
      client,
      baseParams,
      TEST_DATA.batchSize,
    ),
  );
  console.log("---");

  await globalRateLimiter.waitForNextSlot();
  results.push(
    await testQueryFunction(
      "queryProductCoPurchaseAnalysis",
      queryProductCoPurchaseAnalysis,
      client,
      baseParams,
      TEST_DATA.batchSize,
    ),
  );
  console.log("---");

  await globalRateLimiter.waitForNextSlot();
  results.push(
    await testQueryFunction(
      "queryPromotionDiscountAnalysis",
      queryPromotionDiscountAnalysis,
      client,
      baseParams,
      TEST_DATA.batchSize,
    ),
  );
  console.log("---");

  await globalRateLimiter.waitForNextSlot();
  results.push(
    await testQueryFunction(
      "querySalesAnalytics",
      querySalesAnalytics,
      client,
      baseParams,
      TEST_DATA.batchSize,
    ),
  );
  console.log("---");

  await globalRateLimiter.waitForNextSlot();
  results.push(
    await testQueryFunction(
      "querySalesSummary",
      querySalesSummary,
      client,
      baseParams,
      TEST_DATA.batchSize,
    ),
  );
  console.log("---");

  await globalRateLimiter.waitForNextSlot();
  results.push(
    await testQueryFunction(
      "querySearchQueryPerformance",
      querySearchQueryPerformance,
      client,
      searchParams,
      TEST_DATA.batchSize,
    ),
  );
  console.log("---");

  await globalRateLimiter.waitForNextSlot();
  results.push(
    await testQueryFunction(
      "queryTopReferrers",
      queryTopReferrers,
      client,
      baseParams,
      TEST_DATA.batchSize,
    ),
  );
  console.log("---");

  await globalRateLimiter.waitForNextSlot();
  results.push(
    await testQueryFunction(
      "queryOcapiRequests",
      queryOcapiRequests,
      client,
      baseParams,
      TEST_DATA.batchSize,
    ),
  );
  console.log("---");

  // Summary
  const successful = results.filter((r) => r.success).length;
  const failed = results.filter((r) => !r.success).length;

  console.log("\n=== TEST SUMMARY ===");
  console.log(`Total tests: ${results.length}`);
  console.log(`Successful: ${successful}`);
  console.log(`Failed: ${failed}`);

  if (failed > 0) {
    console.log("\nFailed tests:");
    results
      .filter((r) => !r.success)
      .forEach((r) => {
        console.log(`- ${r.functionName}: ${r.error}`);
      });
  }

  await client.closeConnection();
  return results;
}

/**
 * Main integration test runner function with environment checks
 */
async function runAllIntegrationTests(): Promise<void> {
  console.log('🚀 CIP Analytics Integration Test Suite');
  console.log('='.repeat(80));
  console.log('Testing all business use case functions against live backend');
  console.log('Rate limited to 10 queries per minute');
  console.log();

  try {
    const results = await runIntegrationTests();
    const successful = results.filter(r => r.success).length;
    const failed = results.filter(r => !r.success).length;

    console.log('\n📊 FINAL TEST SUITE SUMMARY');
    console.log('='.repeat(80));
    console.log(`Total Tests: ${results.length}`);
    console.log(`Successful: ${successful} ✅`);
    console.log(`Failed: ${failed} ❌`);

    if (failed > 0) {
      console.log('\n❌ FAILED TESTS:');
      results.filter(r => !r.success).forEach(result => {
        console.log(`  • ${result.functionName}: ${result.error}`);
      });
      
      console.log('\n⚠️  Some tests failed. Check individual test outputs above for details.');
      process.exit(1);
    } else {
      console.log('\n🎉 ALL INTEGRATION TESTS COMPLETED SUCCESSFULLY');
      console.log('📋 All business use case functions validated against live backend');
    }
  } catch (error) {
    console.error('❌ Integration test suite failed:', error);
    process.exit(1);
  }
}

/**
 * Environment check function
 */
function checkEnvironment(): void {
  const requiredVars = [
    'SFCC_CLIENT_ID',
    'SFCC_CLIENT_SECRET', 
    'SFCC_CIP_INSTANCE',
    'TEST_SITE_ID'
  ];

  const missingVars = requiredVars.filter(varName => !process.env[varName]);
  
  if (missingVars.length > 0) {
    console.error('❌ Missing required environment variables:');
    missingVars.forEach(varName => console.error(`   - ${varName}`));
    console.error('\nPlease set these environment variables before running integration tests.');
    console.error('Example:');
    console.error('export SFCC_CLIENT_ID=your_client_id');
    console.error('export SFCC_CLIENT_SECRET=your_client_secret');
    console.error('export SFCC_CIP_INSTANCE=your_instance_url');
    console.error('export TEST_SITE_ID=your_site_id');
    process.exit(1);
  }
}

// Run tests if called directly
if (require.main === module) {
  checkEnvironment();
  runAllIntegrationTests().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

// Export for use in other test files
export { TestResult, runIntegrationTests, runAllIntegrationTests };

