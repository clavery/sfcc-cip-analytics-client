#!/usr/bin/env npx tsx

import { runCustomerAnalyticsTests } from './customer-analytics.test';
import { runProductAnalyticsTests } from './product-analytics.test';
import { runPromotionAnalyticsTests } from './promotion-analytics.test';
import { runRecommendationAnalyticsTests } from './recommendation-analytics.test';

/**
 * Main integration test runner that executes all test suites
 */
async function runAllIntegrationTests(): Promise<void> {
  console.log('🚀 CIP Analytics Integration Test Suite');
  console.log('=' .repeat(80));
  console.log('Testing all business use case functions against live backend');
  console.log('Rate limited to 10 queries per minute');
  console.log();

  const testSuites = [
    { name: 'Customer Analytics', icon: '📊', runner: runCustomerAnalyticsTests },
    { name: 'Product Analytics', icon: '🛍️', runner: runProductAnalyticsTests },
    { name: 'Promotion Analytics', icon: '🎯', runner: runPromotionAnalyticsTests },
    { name: 'Recommendation Analytics', icon: '🤖', runner: runRecommendationAnalyticsTests }
  ];

  const results: Array<{ name: string; success: boolean; error?: string }> = [];
  const startTime = Date.now();

  // Run all test suites, collecting results instead of exiting on failure
  for (const suite of testSuites) {
    console.log(`${suite.icon} Running ${suite.name} Tests...`);
    
    try {
      await suite.runner();
      console.log(`✅ ${suite.name} Tests Complete\n`);
      results.push({ name: suite.name, success: true });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.log(`❌ ${suite.name} Tests Failed: ${errorMessage}\n`);
      results.push({ name: suite.name, success: false, error: errorMessage });
    }
  }

  const duration = Date.now() - startTime;
  const successfulSuites = results.filter(r => r.success);
  const failedSuites = results.filter(r => !r.success);

  // Generate final summary
  console.log('📊 FINAL TEST SUITE SUMMARY');
  console.log('='.repeat(80));
  console.log(`Total Test Suites: ${results.length}`);
  console.log(`Successful: ${successfulSuites.length} ✅`);
  console.log(`Failed: ${failedSuites.length} ❌`);
  console.log(`⏱️  Total execution time: ${Math.round(duration / 1000)}s`);

  if (successfulSuites.length > 0) {
    console.log('\n✅ SUCCESSFUL TEST SUITES:');
    successfulSuites.forEach(result => {
      console.log(`  • ${result.name}`);
    });
  }

  if (failedSuites.length > 0) {
    console.log('\n❌ FAILED TEST SUITES:');
    failedSuites.forEach(result => {
      console.log(`  • ${result.name}: ${result.error}`);
    });
    
    console.log('\n⚠️  Some test suites failed. Check individual suite outputs above for details.');
    process.exit(1);
  } else {
    console.log('\n🎉 ALL INTEGRATION TEST SUITES COMPLETED SUCCESSFULLY');
    console.log('📋 All business use case functions validated against live backend');
  }
}

// Handle environment check
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

// Main execution
if (require.main === module) {
  checkEnvironment();
  runAllIntegrationTests().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

export { runAllIntegrationTests };