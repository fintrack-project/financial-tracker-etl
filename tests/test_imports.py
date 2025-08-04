import unittest
import sys
import os

class TestImports(unittest.TestCase):
    """Test that all required modules can be imported successfully."""
    
    def test_etl_main_import(self):
        """Test that etl.main can be imported."""
        try:
            from etl.main import publish_kafka_messages, ProducerKafkaTopics
            self.assertTrue(True, "Successfully imported from etl.main")
        except Exception as e:
            self.fail(f"Failed to import from etl.main: {e}")
    
    def test_etl_fetch_utils_import(self):
        """Test that etl.fetch_utils can be imported."""
        try:
            from etl.fetch_utils import fetch_data, process_data, fetch_and_insert_data
            self.assertTrue(True, "Successfully imported from etl.fetch_utils")
        except Exception as e:
            self.fail(f"Failed to import from etl.fetch_utils: {e}")
    
    def test_etl_jobs_import(self):
        """Test that etl.jobs modules can be imported."""
        try:
            from etl.jobs.fetch_market_data.fetch_market_data import run, get_assets_needing_update
            self.assertTrue(True, "Successfully imported from etl.jobs.fetch_market_data")
        except Exception as e:
            self.fail(f"Failed to import from etl.jobs.fetch_market_data: {e}")
    
    def test_etl_utils_import(self):
        """Test that etl.utils can be imported."""
        try:
            from etl.utils import log_message, get_db_connection
            self.assertTrue(True, "Successfully imported from etl.utils")
        except Exception as e:
            self.fail(f"Failed to import from etl.utils: {e}")

if __name__ == "__main__":
    unittest.main() 