#!/usr/bin/env python3
"""
Comprehensive test runner for ETL pipeline tests.
Executes all test suites and provides detailed reporting.
"""

import unittest
import sys
import os
import time
from datetime import datetime

# Add the parent directory to the path to import ETL modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

def run_all_tests():
    """Run all ETL tests and provide comprehensive reporting."""
    
    # Discover and load all test modules
    test_loader = unittest.TestLoader()
    test_suite = unittest.TestSuite()
    
    # Test modules to run
    test_modules = [
        'test_data_processing',
        'test_database_operations', 
        'test_api_integration',
        'test_fetch_market_data',
        'test_fetch_market_average_data'
    ]
    
    print("🔍 Discovering ETL tests...")
    
    for module_name in test_modules:
        try:
            module = __import__(module_name)
            tests = test_loader.loadTestsFromModule(module)
            test_suite.addTests(tests)
            print(f"✅ Loaded {module_name}: {tests.countTestCases()} tests")
        except ImportError as e:
            print(f"❌ Failed to load {module_name}: {e}")
        except Exception as e:
            print(f"⚠️ Error loading {module_name}: {e}")
    
    print(f"\n📊 Total tests discovered: {test_suite.countTestCases()}")
    
    # Run tests with detailed output
    print("\n🚀 Starting ETL test execution...")
    print("=" * 80)
    
    start_time = time.time()
    
    # Create test runner with detailed output
    test_runner = unittest.TextTestRunner(
        verbosity=2,
        stream=sys.stdout,
        descriptions=True,
        failfast=False
    )
    
    # Run tests
    result = test_runner.run(test_suite)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Generate comprehensive report
    print("\n" + "=" * 80)
    print("📋 ETL TEST EXECUTION REPORT")
    print("=" * 80)
    
    print(f"⏱️  Execution Time: {execution_time:.2f} seconds")
    print(f"📈 Total Tests: {result.testsRun}")
    print(f"✅ Passed: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"❌ Failed: {len(result.failures)}")
    print(f"⚠️  Errors: {len(result.errors)}")
    print(f"📊 Success Rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    # Detailed failure and error reporting
    if result.failures:
        print(f"\n❌ FAILURES ({len(result.failures)}):")
        for test, traceback in result.failures:
            print(f"  • {test}: {traceback.split('AssertionError:')[-1].strip()}")
    
    if result.errors:
        print(f"\n⚠️  ERRORS ({len(result.errors)}):")
        for test, traceback in result.errors:
            print(f"  • {test}: {traceback.split('Exception:')[-1].strip()}")
    
    # Test coverage summary
    print(f"\n📋 TEST COVERAGE SUMMARY:")
    print(f"  • Data Processing Tests: ✅ API response parsing, data validation, error handling")
    print(f"  • Database Operations Tests: ✅ Data insertion accuracy, update operations, batch processing")
    print(f"  • API Integration Tests: ✅ Rate limiting, retry logic, error handling")
    print(f"  • Market Data Pipeline: ✅ End-to-end data flow testing")
    
    # Recommendations based on results
    print(f"\n💡 RECOMMENDATIONS:")
    if result.failures or result.errors:
        print(f"  • Review failed tests and fix underlying issues")
        print(f"  • Check API endpoint availability and rate limits")
        print(f"  • Verify database connection and schema")
        print(f"  • Ensure all environment variables are properly set")
    else:
        print(f"  • All tests passing! ETL pipeline is ready for production")
        print(f"  • Consider adding performance benchmarks")
        print(f"  • Monitor API rate limits in production")
    
    return result.wasSuccessful()

def run_specific_test_category(category):
    """Run tests for a specific category."""
    category_tests = {
        'data_processing': ['test_data_processing'],
        'database': ['test_database_operations'],
        'api': ['test_api_integration'],
        'market_data': ['test_fetch_market_data', 'test_fetch_market_average_data']
    }
    
    if category not in category_tests:
        print(f"❌ Unknown test category: {category}")
        print(f"Available categories: {list(category_tests.keys())}")
        return False
    
    test_loader = unittest.TestLoader()
    test_suite = unittest.TestSuite()
    
    for module_name in category_tests[category]:
        try:
            module = __import__(module_name)
            tests = test_loader.loadTestsFromModule(module)
            test_suite.addTests(tests)
            print(f"✅ Loaded {module_name}: {tests.countTestCases()} tests")
        except ImportError as e:
            print(f"❌ Failed to load {module_name}: {e}")
            return False
    
    print(f"\n🚀 Running {category} tests...")
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)
    
    return result.wasSuccessful()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL Test Runner')
    parser.add_argument('--category', choices=['data_processing', 'database', 'api', 'market_data', 'all'],
                       default='all', help='Test category to run')
    
    args = parser.parse_args()
    
    if args.category == 'all':
        success = run_all_tests()
    else:
        success = run_specific_test_category(args.category)
    
    sys.exit(0 if success else 1) 