import { BaseIntegrationTest } from './base-test';
import { getTestConfig, TEST_DATA } from './test-config';
import {
  queryRecommendationPerformanceByAlgorithm,
  queryOverallRecommendationPerformance,
  queryRecommendationWidgetPlacement
} from '../../src/data/aggregate/recommendation_analytics';

/**
 * Integration tests for recommendation analytics functions
 */
class RecommendationAnalyticsTest extends BaseIntegrationTest {
  
  async runAllTests(): Promise<void> {
    console.log('🔬 Starting Recommendation Analytics Integration Tests');
    console.log('='.repeat(60));
    
    await this.testRecommendationPerformanceByAlgorithm();
    await this.testOverallRecommendationPerformance();
    await this.testRecommendationWidgetPlacement();
  }

  private async testRecommendationPerformanceByAlgorithm(): Promise<void> {
    const params = {
      dateRange: this.config.defaultDateRange,
      siteId: this.config.defaultSiteId,
      recommenderName: TEST_DATA.recommenderName
    };

    await this.testFunction(
      queryRecommendationPerformanceByAlgorithm,
      params,
      'RecommendationPerformance',
      TEST_DATA.batchSize
    );
  }

  private async testOverallRecommendationPerformance(): Promise<void> {
    const params = {
      dateRange: this.config.defaultDateRange,
      siteId: this.config.defaultSiteId
    };

    await this.testFunction(
      queryOverallRecommendationPerformance,
      params,
      'OverallRecommendationPerformance',
      TEST_DATA.batchSize
    );
  }

  private async testRecommendationWidgetPlacement(): Promise<void> {
    const params = {
      dateRange: this.config.defaultDateRange,
      siteId: this.config.defaultSiteId,
      recommenderName: TEST_DATA.recommenderName
    };

    await this.testFunction(
      queryRecommendationWidgetPlacement,
      params,
      'RecommendationWidgetPlacement',
      TEST_DATA.batchSize
    );
  }
}

/**
 * Run recommendation analytics integration tests
 */
export async function runRecommendationAnalyticsTests(): Promise<void> {
  const testRunner = new RecommendationAnalyticsTest();
  
  try {
    await testRunner.setup();
    await testRunner.runAllTests();
    testRunner.generateReport();
    
    if (!testRunner.allTestsPassed()) {
      throw new Error('Some recommendation analytics tests failed');
    }
  } catch (error) {
    if (error instanceof Error) {
      throw error; // Re-throw the error for the main runner to handle
    }
    throw new Error(`Recommendation analytics test suite failed: ${error}`);
  } finally {
    await testRunner.cleanup();
  }
}

// Run tests if called directly
if (require.main === module) {
  runRecommendationAnalyticsTests().catch(error => {
    console.error('❌ Recommendation analytics test suite failed:', error);
    process.exit(1);
  });
}