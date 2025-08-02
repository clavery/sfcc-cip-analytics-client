import { BaseIntegrationTest } from './base-test';
import { getTestConfig, TEST_DATA } from './test-config';
import {
  queryCustomerRegistrationTrends,
  queryTotalCustomerGrowth,
  queryRegistration
} from '../../src/data/aggregate/customer_registration_analytics';

/**
 * Integration tests for customer analytics functions
 */
class CustomerAnalyticsTest extends BaseIntegrationTest {
  
  async runAllTests(): Promise<void> {
    console.log('🔬 Starting Customer Analytics Integration Tests');
    console.log('='.repeat(60));
    
    await this.testCustomerRegistrationTrends();
    await this.testTotalCustomerGrowth();
    await this.testRegistrationRecords();
  }

  private async testCustomerRegistrationTrends(): Promise<void> {
    const params = {
      dateRange: this.config.defaultDateRange,
      siteId: this.config.defaultSiteId
    };

    await this.testFunction(
      queryCustomerRegistrationTrends,
      params,
      'CustomerRegistrationTrends',
      TEST_DATA.batchSize
    );
  }

  private async testTotalCustomerGrowth(): Promise<void> {
    const params = {
      dateRange: this.config.defaultDateRange,
      siteId: this.config.defaultSiteId
    };

    await this.testFunction(
      queryTotalCustomerGrowth,
      params,
      'CustomerListSnapshot',
      TEST_DATA.batchSize
    );
  }

  private async testRegistrationRecords(): Promise<void> {
    const params = {
      dateRange: this.config.defaultDateRange,
      siteId: this.config.defaultSiteId,
      deviceClassCode: TEST_DATA.deviceClassCode
    };

    await this.testFunction(
      queryRegistration,
      params,
      'RegistrationRecord',
      TEST_DATA.batchSize
    );
  }
}

/**
 * Run customer analytics integration tests
 */
export async function runCustomerAnalyticsTests(): Promise<void> {
  const testRunner = new CustomerAnalyticsTest();
  
  try {
    await testRunner.setup();
    await testRunner.runAllTests();
    testRunner.generateReport();
    
    if (!testRunner.allTestsPassed()) {
      throw new Error('Some customer analytics tests failed');
    }
  } catch (error) {
    if (error instanceof Error) {
      throw error; // Re-throw the error for the main runner to handle
    }
    throw new Error(`Customer analytics test suite failed: ${error}`);
  } finally {
    await testRunner.cleanup();
  }
}

// Run tests if called directly
if (require.main === module) {
  runCustomerAnalyticsTests().catch(error => {
    console.error('❌ Customer analytics test suite failed:', error);
    process.exit(1);
  });
}
