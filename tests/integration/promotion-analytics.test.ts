import { BaseIntegrationTest } from './base-test';
import { getTestConfig, TEST_DATA } from './test-config';
import {
  queryPromotionDiscountAnalysis,
  queryPromotionPerformanceByType,
  queryPromotionSalesSummary
} from '../../src/data/aggregate/promotion_analytics';

/**
 * Integration tests for promotion analytics functions
 */
class PromotionAnalyticsTest extends BaseIntegrationTest {
  
  async runAllTests(): Promise<void> {
    console.log('🔬 Starting Promotion Analytics Integration Tests');
    console.log('='.repeat(60));
    
    await this.testPromotionDiscountAnalysis();
    await this.testPromotionPerformanceByType();
    await this.testPromotionSalesSummary();
  }

  private async testPromotionDiscountAnalysis(): Promise<void> {
    const params = {
      dateRange: this.config.defaultDateRange,
      siteId: this.config.defaultSiteId
    };

    await this.testFunction(
      queryPromotionDiscountAnalysis,
      params,
      'PromotionDiscountAnalysis',
      TEST_DATA.batchSize
    );
  }

  private async testPromotionPerformanceByType(): Promise<void> {
    const params = {
      dateRange: this.config.defaultDateRange,
      siteId: this.config.defaultSiteId,
      promotionClass: TEST_DATA.promotionClass
    };

    await this.testFunction(
      queryPromotionPerformanceByType,
      params,
      'PromotionPerformanceByType',
      TEST_DATA.batchSize
    );
  }

  private async testPromotionSalesSummary(): Promise<void> {
    const params = {
      dateRange: this.config.defaultDateRange,
      siteId: this.config.defaultSiteId,
      promotionId: TEST_DATA.promotionId
    };

    await this.testFunction(
      queryPromotionSalesSummary,
      params,
      'PromotionSalesSummaryRecord',
      TEST_DATA.batchSize
    );
  }
}

/**
 * Run promotion analytics integration tests
 */
export async function runPromotionAnalyticsTests(): Promise<void> {
  const testRunner = new PromotionAnalyticsTest();
  
  try {
    await testRunner.setup();
    await testRunner.runAllTests();
    testRunner.generateReport();
    
    if (!testRunner.allTestsPassed()) {
      throw new Error('Some promotion analytics tests failed');
    }
  } catch (error) {
    if (error instanceof Error) {
      throw error; // Re-throw the error for the main runner to handle
    }
    throw new Error(`Promotion analytics test suite failed: ${error}`);
  } finally {
    await testRunner.cleanup();
  }
}

// Run tests if called directly
if (require.main === module) {
  runPromotionAnalyticsTests().catch(error => {
    console.error('❌ Promotion analytics test suite failed:', error);
    process.exit(1);
  });
}