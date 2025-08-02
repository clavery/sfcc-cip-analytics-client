import { BaseIntegrationTest } from './base-test';
import { getTestConfig, TEST_DATA } from './test-config';
import {
  queryTopSellingProducts,
  queryProductCoPurchaseAnalysis,
  queryProductPerformanceByDimension,
  queryProductSalesSummary
} from '../../src/data/aggregate/product_analytics';

/**
 * Integration tests for product analytics functions
 */
class ProductAnalyticsTest extends BaseIntegrationTest {
  
  async runAllTests(): Promise<void> {
    console.log('🔬 Starting Product Analytics Integration Tests');
    console.log('='.repeat(60));
    
    await this.testTopSellingProducts();
    await this.testProductCoPurchaseAnalysis();
    await this.testProductPerformanceByDimension();
    await this.testProductSalesSummary();
  }

  private async testTopSellingProducts(): Promise<void> {
    const params = {
      dateRange: this.config.defaultDateRange,
      siteId: this.config.defaultSiteId
    };

    await this.testFunction(
      queryTopSellingProducts,
      params,
      'TopSellingProduct',
      TEST_DATA.batchSize
    );
  }

  private async testProductCoPurchaseAnalysis(): Promise<void> {
    const params = {
      dateRange: this.config.defaultDateRange,
      siteId: this.config.defaultSiteId,
      productId: TEST_DATA.productId
    };

    await this.testFunction(
      queryProductCoPurchaseAnalysis,
      params,
      'ProductCoPurchase',
      TEST_DATA.batchSize
    );
  }

  private async testProductPerformanceByDimension(): Promise<void> {
    const params = {
      dateRange: this.config.defaultDateRange,
      siteId: this.config.defaultSiteId,
      productId: TEST_DATA.productId,
      dimension: TEST_DATA.dimension
    };

    await this.testFunction(
      queryProductPerformanceByDimension,
      params,
      'ProductPerformanceByDimension',
      TEST_DATA.batchSize
    );
  }

  private async testProductSalesSummary(): Promise<void> {
    const params = {
      dateRange: this.config.defaultDateRange,
      siteId: this.config.defaultSiteId,
      productId: TEST_DATA.productId
    };

    await this.testFunction(
      queryProductSalesSummary,
      params,
      'ProductSalesSummaryRecord',
      TEST_DATA.batchSize
    );
  }
}

/**
 * Run product analytics integration tests
 */
export async function runProductAnalyticsTests(): Promise<void> {
  const testRunner = new ProductAnalyticsTest();
  
  try {
    await testRunner.setup();
    await testRunner.runAllTests();
    testRunner.generateReport();
    
    if (!testRunner.allTestsPassed()) {
      throw new Error('Some product analytics tests failed');
    }
  } catch (error) {
    if (error instanceof Error) {
      throw error; // Re-throw the error for the main runner to handle
    }
    throw new Error(`Product analytics test suite failed: ${error}`);
  } finally {
    await testRunner.cleanup();
  }
}

// Run tests if called directly
if (require.main === module) {
  runProductAnalyticsTests().catch(error => {
    console.error('❌ Product analytics test suite failed:', error);
    process.exit(1);
  });
}