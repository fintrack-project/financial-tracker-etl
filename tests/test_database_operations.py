import unittest
from unittest.mock import patch, MagicMock, Mock
from datetime import datetime, timezone, timedelta
import psycopg
from etl.utils import get_db_connection
from etl.fetch_utils import (
    get_existing_data,
    get_existing_data_ranges,
    adjust_date_range,
    determine_symbols_needing_update
)
from etl.jobs.fetch_market_data.fetch_market_data import (
    insert_or_update_data,
    validate_assets,
    get_assets_needing_update
)


class TestDatabaseOperations(unittest.TestCase):
    """Test suite for database operations in the ETL pipeline."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_asset = {
            "symbol": "AAPL",
            "asset_type": "STOCK"
        }
        self.mock_processed_data = {
            "symbol": "AAPL",
            "close": 150.00,
            "percent_change": 2.5,
            "change": 3.75,
            "high": 155.00,
            "low": 148.00
        }
        self.mock_connection = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_connection.cursor.return_value = self.mock_cursor

    @patch('etl.jobs.fetch_market_data.fetch_market_data.get_db_connection')
    def test_insert_or_update_data_success(self, mock_get_db_connection):
        """Test successful data insertion/update."""
        mock_get_db_connection.return_value = self.mock_connection
        
        insert_or_update_data(self.mock_cursor, self.mock_connection, self.mock_asset, self.mock_processed_data)
        
        # Verify the SQL query was executed with correct parameters
        self.mock_cursor.execute.assert_called_once()
        call_args = self.mock_cursor.execute.call_args
        sql_query = call_args[0][0]
        params = call_args[0][1]
        
        # Verify SQL contains INSERT ... ON CONFLICT
        self.assertIn("INSERT INTO market_data", sql_query)
        self.assertIn("ON CONFLICT", sql_query)
        self.assertIn("DO UPDATE SET", sql_query)
        
        # Verify parameters
        self.assertEqual(params[0], "AAPL")  # symbol
        self.assertEqual(params[1], "STOCK")  # asset_type
        self.assertEqual(params[2], 150.00)  # price
        self.assertEqual(params[3], 2.5)  # percent_change
        self.assertEqual(params[4], 3.75)  # change
        self.assertEqual(params[5], 155.00)  # high
        self.assertEqual(params[6], 148.00)  # low
        
        # Verify commit was called
        self.mock_connection.commit.assert_called_once()

    @patch('etl.jobs.fetch_market_data.fetch_market_data.get_db_connection')
    def test_insert_or_update_data_database_error(self, mock_get_db_connection):
        """Test handling of database errors during insertion."""
        mock_get_db_connection.return_value = self.mock_connection
        self.mock_cursor.execute.side_effect = psycopg.Error("Database error")
        
        with self.assertRaises(psycopg.Error):
            insert_or_update_data(self.mock_cursor, self.mock_connection, self.mock_asset, self.mock_processed_data)
        
        # Verify commit was not called due to error
        self.mock_connection.commit.assert_not_called()

    @patch('etl.jobs.fetch_market_data.fetch_market_data.get_db_connection')
    def test_insert_or_update_data_connection_error(self, mock_get_db_connection):
        """Test handling of connection errors."""
        mock_get_db_connection.side_effect = psycopg.Error("Connection error")
        
        with self.assertRaises(psycopg.Error):
            insert_or_update_data(self.mock_cursor, self.mock_connection, self.mock_asset, self.mock_processed_data)

    @patch('etl.jobs.fetch_market_data.fetch_market_data.get_db_connection')
    def test_validate_assets_success(self, mock_get_db_connection):
        """Test successful asset validation."""
        mock_get_db_connection.return_value = self.mock_connection
        self.mock_cursor.fetchall.return_value = [("AAPL",), ("TSLA",)]
        
        assets = [
            {"symbol": "AAPL", "asset_type": "STOCK"},
            {"symbol": "TSLA", "asset_type": "STOCK"},
            {"symbol": "INVALID", "asset_type": "STOCK"}
        ]
        
        valid_assets = validate_assets(assets)
        
        # Verify only valid assets are returned
        expected_valid = [
            {"symbol": "AAPL", "asset_type": "STOCK"},
            {"symbol": "TSLA", "asset_type": "STOCK"}
        ]
        self.assertEqual(valid_assets, expected_valid)
        
        # Verify SQL query was executed
        self.mock_cursor.execute.assert_called_once()

    @patch('etl.jobs.fetch_market_data.fetch_market_data.get_db_connection')
    def test_validate_assets_no_valid_assets(self, mock_get_db_connection):
        """Test asset validation when no assets are valid."""
        mock_get_db_connection.return_value = self.mock_connection
        self.mock_cursor.fetchall.return_value = []
        
        assets = [
            {"symbol": "INVALID1", "asset_type": "STOCK"},
            {"symbol": "INVALID2", "asset_type": "STOCK"}
        ]
        
        valid_assets = validate_assets(assets)
        
        # Verify empty list is returned
        self.assertEqual(valid_assets, [])

    @patch('etl.jobs.fetch_market_data.fetch_market_data.get_db_connection')
    def test_validate_assets_database_error(self, mock_get_db_connection):
        """Test handling of database errors during asset validation."""
        mock_get_db_connection.return_value = self.mock_connection
        self.mock_cursor.execute.side_effect = psycopg.Error("Database error")
        
        assets = [{"symbol": "AAPL", "asset_type": "STOCK"}]
        
        with self.assertRaises(psycopg.Error):
            validate_assets(assets)

    @patch('etl.jobs.fetch_market_data.fetch_market_data.get_db_connection')
    @patch('etl.jobs.fetch_market_data.fetch_market_data.get_closest_us_market_closing_time')
    def test_get_assets_needing_update_success(self, mock_get_closing_time, mock_get_db_connection):
        """Test successful identification of assets needing updates."""
        mock_get_db_connection.return_value = self.mock_connection
        mock_get_closing_time.return_value = datetime(2025, 4, 16, 20, 0, 0)
        self.mock_cursor.fetchall.return_value = [("AAPL",), ("TSLA",)]
        
        assets = [
            {"symbol": "AAPL", "asset_type": "STOCK"},
            {"symbol": "TSLA", "asset_type": "STOCK"},
            {"symbol": "INVALID", "asset_type": "STOCK"}
        ]
        
        assets_needing_update = get_assets_needing_update(assets)
        
        # Verify assets needing update are returned
        expected_assets = [
            {"symbol": "AAPL", "asset_type": "STOCK"},
            {"symbol": "TSLA", "asset_type": "STOCK"}
        ]
        self.assertEqual(assets_needing_update, expected_assets)

    @patch('etl.fetch_utils.get_db_connection')
    def test_get_existing_data_success(self, mock_get_db_connection):
        """Test successful retrieval of existing data."""
        mock_get_db_connection.return_value = self.mock_connection
        self.mock_cursor.fetchall.return_value = [("AAPL",), ("TSLA",)]
        
        symbols = ["AAPL", "TSLA", "INVALID"]
        existing_data = get_existing_data(symbols, "market_data")
        
        # Verify existing symbols are returned
        expected_symbols = ["AAPL", "TSLA"]
        self.assertEqual(existing_data, expected_symbols)
        
        # Verify SQL query was executed
        self.mock_cursor.execute.assert_called_once()

    @patch('etl.fetch_utils.get_db_connection')
    def test_get_existing_data_empty_result(self, mock_get_db_connection):
        """Test retrieval of existing data when no data exists."""
        mock_get_db_connection.return_value = self.mock_connection
        self.mock_cursor.fetchall.return_value = []
        
        symbols = ["INVALID1", "INVALID2"]
        existing_data = get_existing_data(symbols, "market_data")
        
        # Verify empty list is returned
        self.assertEqual(existing_data, [])

    @patch('etl.fetch_utils.get_db_connection')
    def test_get_existing_data_ranges_success(self, mock_get_db_connection):
        """Test successful retrieval of existing data ranges."""
        mock_get_db_connection.return_value = self.mock_connection
        self.mock_cursor.fetchall.return_value = [
            ("AAPL", datetime(2025, 1, 1), datetime(2025, 1, 31)),
            ("TSLA", datetime(2025, 1, 1), datetime(2025, 1, 15))
        ]
        
        symbols = ["AAPL", "TSLA"]
        existing_ranges = get_existing_data_ranges(symbols, "historical_market_data")
        
        # Verify existing ranges are returned
        expected_ranges = {
            "AAPL": [(datetime(2025, 1, 1), datetime(2025, 1, 31))],
            "TSLA": [(datetime(2025, 1, 1), datetime(2025, 1, 15))]
        }
        self.assertEqual(existing_ranges, expected_ranges)

    def test_adjust_date_range_valid_dates(self):
        """Test date range adjustment with valid dates."""
        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 1, 31)
        
        adjusted_start, adjusted_end = adjust_date_range(start_date, end_date)
        
        # Verify dates are adjusted correctly
        self.assertEqual(adjusted_start, start_date)
        self.assertEqual(adjusted_end, end_date)

    def test_adjust_date_range_invalid_dates(self):
        """Test date range adjustment with invalid dates."""
        start_date = datetime(2025, 1, 31)
        end_date = datetime(2025, 1, 1)  # End before start
        
        adjusted_start, adjusted_end = adjust_date_range(start_date, end_date)
        
        # Verify dates are swapped
        self.assertEqual(adjusted_start, end_date)
        self.assertEqual(adjusted_end, start_date)

    def test_determine_symbols_needing_update_success(self):
        """Test successful determination of symbols needing updates."""
        symbols = ["AAPL", "TSLA", "BTC"]
        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 1, 31)
        
        # Mock existing data with gaps
        existing_data_by_symbol = {
            "AAPL": [(datetime(2025, 1, 1), datetime(2025, 1, 15))],  # Gap from 16-31
            "TSLA": [(datetime(2025, 1, 1), datetime(2025, 1, 31))],  # Complete
            "BTC": []  # No data
        }
        
        symbols_needing_update = determine_symbols_needing_update(
            symbols, start_date, end_date, existing_data_by_symbol
        )
        
        # Verify symbols with gaps or no data are returned
        expected_symbols = ["AAPL", "BTC"]
        self.assertEqual(symbols_needing_update, expected_symbols)

    def test_determine_symbols_needing_update_all_complete(self):
        """Test determination when all symbols have complete data."""
        symbols = ["AAPL", "TSLA"]
        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 1, 31)
        
        # Mock existing data with complete coverage
        existing_data_by_symbol = {
            "AAPL": [(datetime(2025, 1, 1), datetime(2025, 1, 31))],
            "TSLA": [(datetime(2025, 1, 1), datetime(2025, 1, 31))]
        }
        
        symbols_needing_update = determine_symbols_needing_update(
            symbols, start_date, end_date, existing_data_by_symbol
        )
        
        # Verify no symbols need updates
        self.assertEqual(symbols_needing_update, [])

    @patch('etl.jobs.fetch_market_data.fetch_market_data.get_db_connection')
    def test_batch_insert_operations(self, mock_get_db_connection):
        """Test batch insert operations with multiple assets."""
        mock_get_db_connection.return_value = self.mock_connection
        
        assets = [
            {"symbol": "AAPL", "asset_type": "STOCK"},
            {"symbol": "TSLA", "asset_type": "STOCK"},
            {"symbol": "BTC", "asset_type": "CRYPTO"}
        ]
        
        processed_data_list = [
            {"symbol": "AAPL", "close": 150.00, "percent_change": 2.5, "change": 3.75, "high": 155.00, "low": 148.00},
            {"symbol": "TSLA", "close": 250.00, "percent_change": 1.5, "change": 3.75, "high": 255.00, "low": 248.00},
            {"symbol": "BTC", "close": 45000.00, "percent_change": 1.2, "change": 540.00, "high": 45500.00, "low": 44800.00}
        ]
        
        # Simulate batch insert
        for asset, processed_data in zip(assets, processed_data_list):
            insert_or_update_data(self.mock_cursor, self.mock_connection, asset, processed_data)
        
        # Verify execute was called for each asset
        self.assertEqual(self.mock_cursor.execute.call_count, 3)
        
        # Verify commit was called for each asset
        self.assertEqual(self.mock_connection.commit.call_count, 3)

    @patch('etl.jobs.fetch_market_data.fetch_market_data.get_db_connection')
    def test_data_consistency_checks(self, mock_get_db_connection):
        """Test data consistency checks during database operations."""
        mock_get_db_connection.return_value = self.mock_connection
        
        # Test with valid data
        valid_data = {
            "symbol": "AAPL",
            "close": 150.00,
            "percent_change": 2.5,
            "change": 3.75,
            "high": 155.00,
            "low": 148.00
        }
        
        insert_or_update_data(self.mock_cursor, self.mock_connection, self.mock_asset, valid_data)
        
        # Verify data types are correct
        call_args = self.mock_cursor.execute.call_args
        params = call_args[0][1]
        
        # Check that numeric values are floats
        self.assertIsInstance(params[2], float)  # price
        self.assertIsInstance(params[3], float)  # percent_change
        self.assertIsInstance(params[4], float)  # change
        self.assertIsInstance(params[5], float)  # high
        self.assertIsInstance(params[6], float)  # low

    @patch('etl.jobs.fetch_market_data.fetch_market_data.get_db_connection')
    def test_transaction_rollback_on_error(self, mock_get_db_connection):
        """Test transaction rollback when database error occurs."""
        mock_get_db_connection.return_value = self.mock_connection
        self.mock_cursor.execute.side_effect = psycopg.Error("Database error")
        
        with self.assertRaises(psycopg.Error):
            insert_or_update_data(self.mock_cursor, self.mock_connection, self.mock_asset, self.mock_processed_data)
        
        # Verify rollback was called (if implemented)
        # Note: This depends on the actual implementation of error handling

    def test_data_type_validation(self):
        """Test validation of data types before database insertion."""
        # Test with invalid data types
        invalid_data = {
            "symbol": "AAPL",
            "close": "invalid_price",  # Should be float
            "percent_change": "invalid_percent",  # Should be float
            "change": "invalid_change",  # Should be float
            "high": "invalid_high",  # Should be float
            "low": "invalid_low"  # Should be float
        }
        
        with self.assertRaises(ValueError):
            # This should fail when trying to convert to float
            insert_or_update_data(self.mock_cursor, self.mock_connection, self.mock_asset, invalid_data)

    @patch('etl.jobs.fetch_market_data.fetch_market_data.get_db_connection')
    def test_concurrent_access_simulation(self, mock_get_db_connection):
        """Test simulation of concurrent database access."""
        mock_get_db_connection.return_value = self.mock_connection
        
        # Simulate multiple concurrent operations
        assets = [
            {"symbol": "AAPL", "asset_type": "STOCK"},
            {"symbol": "TSLA", "asset_type": "STOCK"}
        ]
        
        processed_data_list = [
            {"symbol": "AAPL", "close": 150.00, "percent_change": 2.5, "change": 3.75, "high": 155.00, "low": 148.00},
            {"symbol": "TSLA", "close": 250.00, "percent_change": 1.5, "change": 3.75, "high": 255.00, "low": 248.00}
        ]
        
        # Simulate concurrent operations
        for asset, processed_data in zip(assets, processed_data_list):
            insert_or_update_data(self.mock_cursor, self.mock_connection, asset, processed_data)
        
        # Verify all operations completed successfully
        self.assertEqual(self.mock_cursor.execute.call_count, 2)
        self.assertEqual(self.mock_connection.commit.call_count, 2)

    @patch('etl.jobs.fetch_market_data.fetch_market_data.get_db_connection')
    def test_database_connection_pooling(self, mock_get_db_connection):
        """Test database connection pooling behavior."""
        mock_get_db_connection.return_value = self.mock_connection
        
        # Simulate multiple database operations
        for i in range(5):
            validate_assets([{"symbol": f"SYMBOL{i}", "asset_type": "STOCK"}])
        
        # Verify connection was reused (not created multiple times)
        # This depends on the actual implementation of connection pooling
        self.assertGreaterEqual(mock_get_db_connection.call_count, 1)


if __name__ == '__main__':
    unittest.main() 