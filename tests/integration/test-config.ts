import { DateRange } from '../../src/data/types';

/**
 * Test configuration for integration tests
 */
export interface TestConfig {
  clientId: string;
  clientSecret: string;
  instance: string;
  defaultSiteId: string;
  defaultDateRange: DateRange;
  testTimeout: number;
  enableSlowTests: boolean;
}

/**
 * Get test configuration from environment variables
 */
export function getTestConfig(): TestConfig {
  const requiredEnvVars = {
    clientId: process.env.SFCC_CLIENT_ID,
    clientSecret: process.env.SFCC_CLIENT_SECRET,
    instance: process.env.SFCC_CIP_INSTANCE,
    defaultSiteId: process.env.TEST_SITE_ID || process.env.SFCC_DEFAULT_SITE_ID
  };

  // Check for missing required environment variables
  const missingVars = Object.entries(requiredEnvVars)
    .filter(([key, value]) => !value)
    .map(([key]) => key);

  if (missingVars.length > 0) {
    throw new Error(
      `Missing required environment variables for integration tests: ${missingVars.join(', ')}\n` +
      'Please set: SFCC_CLIENT_ID, SFCC_CLIENT_SECRET, SFCC_CIP_INSTANCE, TEST_SITE_ID'
    );
  }

  // Fixed date range for consistent testing
  const startDate = new Date('2024-01-01');
  const endDate = new Date('2024-04-01');

  return {
    clientId: requiredEnvVars.clientId!,
    clientSecret: requiredEnvVars.clientSecret!,
    instance: requiredEnvVars.instance!,
    defaultSiteId: requiredEnvVars.defaultSiteId!,
    defaultDateRange: { startDate, endDate },
    testTimeout: parseInt(process.env.TEST_TIMEOUT || '30000'), // 30 seconds default
    enableSlowTests: process.env.ENABLE_SLOW_TESTS === 'true'
  };
}

/**
 * Test data samples for different query types
 */
export const TEST_DATA = {
  siteId: 'test-site', // Will be overridden by config
  productId: 'test-product-123',
  deviceClassCode: 'desktop',
  recommenderName: 'einstein-recommendations',
  promotionId: 'test-promotion-123',
  promotionClass: 'order-discount',
  dimension: 'device' as const,
  batchSize: 10 // Small batch size for faster tests
};

/**
 * Expected result structure validation helpers
 */
export const VALIDATION_RULES = {
  // Customer Analytics
  CustomerRegistrationTrends: {
    requiredFields: ['date', 'new_registrations', 'device_class_code', 'nsite_id'],
    numericFields: ['new_registrations']
  },
  CustomerListSnapshot: {
    requiredFields: ['snapshot_date', 'total_customers'],
    numericFields: ['total_customers']
  },
  RegistrationRecord: {
    requiredFields: ['registration_date', 'site_id', 'device_class_code', 'num_registrations'],
    numericFields: ['site_id', 'num_registrations', 'locale_id', 'channel_id']
  },

  // Product Analytics
  TopSellingProduct: {
    requiredFields: ['nproduct_id', 'product_display_name', 'units_sold', 'std_revenue'],
    numericFields: ['units_sold', 'std_revenue', 'order_count']
  },
  ProductCoPurchase: {
    requiredFields: ['product_1_id', 'product_1_name', 'product_2_id', 'product_2_name'],
    numericFields: ['co_purchase_count', 'std_cobuy_revenue']
  },
  ProductPerformanceByDimension: {
    requiredFields: ['dimension_value', 'units_sold', 'std_revenue', 'order_count'],
    numericFields: ['units_sold', 'std_revenue', 'order_count', 'avg_unit_price']
  },
  ProductSalesSummaryRecord: {
    requiredFields: ['submit_date', 'site_id', 'product_id', 'device_class_code'],
    numericFields: ['site_id', 'product_id', 'num_orders', 'num_units', 'std_revenue']
  },

  // Promotion Analytics
  PromotionDiscountAnalysis: {
    requiredFields: ['submit_day', 'total_orders', 'promotion_class'],
    numericFields: ['total_orders', 'promotion_orders', 'std_total_discount', 'avg_discount_per_order']
  },
  PromotionPerformanceByType: {
    requiredFields: ['promotion_class', 'promotion_name', 'total_orders'],
    numericFields: ['total_orders', 'total_units', 'std_revenue', 'std_total_discount', 'avg_discount_percentage']
  },
  PromotionSalesSummaryRecord: {
    requiredFields: ['submit_date', 'site_id', 'promotion_id', 'device_class_code'],
    numericFields: ['site_id', 'promotion_id', 'num_orders', 'num_units', 'std_revenue']
  },

  // Recommendation Analytics
  RecommendationPerformance: {
    requiredFields: ['nsite_id', 'recommender_name', 'recommender_views_count'],
    numericFields: ['recommender_views_count', 'product_views_count', 'clicks_count', 'add_to_cart_count', 'product_purchased_count', 'order_count']
  },
  OverallRecommendationPerformance: {
    requiredFields: ['recommender_name', 'total_revenue', 'total_orders'],
    numericFields: ['total_revenue', 'total_orders', 'total_clicks', 'total_views', 'overall_ctr', 'overall_conversion_rate', 'avg_order_value']
  },
  RecommendationWidgetPlacement: {
    requiredFields: ['widget_location', 'widget_type', 'total_views'],
    numericFields: ['total_views', 'total_clicks', 'total_revenue', 'location_ctr', 'revenue_per_view']
  }
};