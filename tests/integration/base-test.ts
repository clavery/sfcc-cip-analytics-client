import { CIPClient } from '../../src/cip-client';
import { EnhancedQueryFunction, QueryTemplateParams } from '../../src/data/helpers';
import { globalRateLimiter } from './rate-limiter';
import { getTestConfig, TestConfig, VALIDATION_RULES } from './test-config';

/**
 * Test result interface
 */
export interface TestResult {
  functionName: string;
  passed: boolean;
  error?: string;
  executionTime: number;
  rowCount: number;
  sqlQuery?: string;
  validationErrors?: string[];
}

/**
 * Base integration test class
 */
export class BaseIntegrationTest {
  protected client: CIPClient;
  protected config: TestConfig;
  protected results: TestResult[] = [];

  constructor() {
    this.config = getTestConfig();
    this.client = new CIPClient(
      this.config.clientId,
      this.config.clientSecret,
      this.config.instance
    );
  }

  /**
   * Setup test environment
   */
  async setup(): Promise<void> {
    try {
      await this.client.openConnection({});
      console.log('✅ Connected to CIP backend');
    } catch (error) {
      throw new Error(`Failed to connect to CIP backend: ${error}`);
    }
  }

  /**
   * Cleanup test environment
   */
  async cleanup(): Promise<void> {
    try {
      await this.client.closeConnection();
      console.log('✅ Disconnected from CIP backend');
    } catch (error) {
      console.warn(`Warning: Failed to cleanup connection: ${error}`);
    }
  }

  /**
   * Test a business use case function
   */
  async testFunction<TResult, TParams extends QueryTemplateParams>(
    queryFunction: EnhancedQueryFunction<TResult, TParams>,
    params: TParams,
    validationRule?: keyof typeof VALIDATION_RULES,
    batchSize: number = 10
  ): Promise<TestResult> {
    const functionName = queryFunction.metadata.name;
    const startTime = Date.now();
    
    console.log(`\n🧪 Testing ${functionName}...`);

    let sqlQuery: string | undefined;
    
    try {
      // Rate limiting
      await globalRateLimiter.waitForNextSlot();
      
      // Get the SQL query for debugging
      try {
        const { sql } = queryFunction.QUERY(params);
        sqlQuery = sql.replace(/\\s+/g, ' ').trim();
      } catch (error) {
        sqlQuery = `Error generating SQL: ${error}`;
      }

      // Execute the query
      const allResults: TResult[] = [];
      let batchCount = 0;
      const maxBatches = 3; // Limit batches to avoid too much data in tests

      for await (const batch of queryFunction(this.client, params, batchSize)) {
        allResults.push(...batch);
        batchCount++;
        
        // Limit batches for testing purposes
        if (batchCount >= maxBatches) {
          console.log(`  📊 Limiting to ${maxBatches} batches for test performance`);
          break;
        }
      }

      const executionTime = Date.now() - startTime;
      const rowCount = allResults.length;

      console.log(`  ✅ Success: ${rowCount} rows in ${executionTime}ms`);

      // Validate results if validation rule provided
      const validationErrors = validationRule ? 
        this.validateResults(allResults, validationRule) : 
        undefined;

      if (validationErrors && validationErrors.length > 0) {
        console.log(`  ⚠️  Validation warnings: ${validationErrors.join(', ')}`);
      }

      const result: TestResult = {
        functionName,
        passed: true,
        executionTime,
        rowCount,
        sqlQuery,
        validationErrors
      };

      this.results.push(result);
      return result;

    } catch (error) {
      const executionTime = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      console.log(`  ❌ Failed: ${errorMessage}`);

      const result: TestResult = {
        functionName,
        passed: false,
        error: errorMessage,
        executionTime,
        rowCount: 0,
        sqlQuery: sqlQuery
      };

      this.results.push(result);
      return result;
    }
  }

  /**
   * Validate query results against expected structure
   */
  private validateResults<T>(results: T[], ruleName: keyof typeof VALIDATION_RULES): string[] {
    const rule = VALIDATION_RULES[ruleName];
    const errors: string[] = [];

    if (results.length === 0) {
      errors.push('No results returned');
      return errors;
    }

    const sampleRow = results[0] as any;

    // Check required fields
    for (const field of rule.requiredFields) {
      if (!(field in sampleRow)) {
        errors.push(`Missing required field: ${field}`);
      }
    }

    // Check numeric fields
    for (const field of rule.numericFields || []) {
      if (field in sampleRow && typeof sampleRow[field] !== 'number') {
        errors.push(`Field ${field} should be numeric but is ${typeof sampleRow[field]}`);
      }
    }

    return errors;
  }

  /**
   * Generate test summary report
   */
  generateReport(): void {
    const totalTests = this.results.length;
    const passedTests = this.results.filter(r => r.passed).length;
    const failedTests = totalTests - passedTests;

    console.log('\n📊 INTEGRATION TEST SUMMARY');
    console.log('='.repeat(50));
    console.log(`Total Tests: ${totalTests}`);
    console.log(`Passed: ${passedTests} ✅`);
    console.log(`Failed: ${failedTests} ❌`);
    console.log(`Success Rate: ${((passedTests / totalTests) * 100).toFixed(1)}%`);

    if (failedTests > 0) {
      console.log('\n❌ FAILED TESTS:');
      this.results
        .filter(r => !r.passed)
        .forEach(result => {
          console.log(`  • ${result.functionName}: ${result.error}`);
          if (result.sqlQuery) {
            console.log(`    SQL: ${result.sqlQuery}`);
          }
        });
    }

    if (passedTests > 0) {
      console.log('\n✅ SUCCESSFUL TESTS:');
      this.results
        .filter(r => r.passed)
        .forEach(result => {
          console.log(`  • ${result.functionName}: ${result.rowCount} rows in ${result.executionTime}ms`);
          if (result.validationErrors && result.validationErrors.length > 0) {
            console.log(`    Warnings: ${result.validationErrors.join(', ')}`);
          }
        });
    }

    console.log('\n🕐 RATE LIMITING STATUS:');
    console.log(`Current requests in window: ${globalRateLimiter.getCurrentRequestCount()}/10`);
  }

  /**
   * Get all test results
   */
  getResults(): TestResult[] {
    return [...this.results];
  }

  /**
   * Check if all tests passed
   */
  allTestsPassed(): boolean {
    return this.results.every(r => r.passed);
  }
}
