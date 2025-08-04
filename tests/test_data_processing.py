import unittest
from unittest.mock import patch, MagicMock, Mock
from datetime import datetime, timezone
import json
import requests
import logging

# Mock logging to avoid permission issues
@patch('etl.utils.log_message')
@patch('etl.fetch_utils.log_message')
class TestDataProcessing(unittest.TestCase):
    """Test suite for data processing functionality in the ETL pipeline."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_asset = {
            "symbol": "AAPL",
            "asset_type": "STOCK"
        }
        self.mock_crypto_asset = {
            "symbol": "BTC",
            "asset_type": "CRYPTO"
        }
        self.mock_forex_asset = {
            "symbol": "EUR/USD",
            "asset_type": "FOREX"
        }
        self.required_fields = ["symbol", "close", "percent_change", "change", "high", "low"]

    def test_fetch_data_stock_success(self, mock_log_fetch, mock_log_utils):
        """Test successful stock data fetching."""
        mock_stock_data = {
            "symbol": "AAPL",
            "close": 150.00,
            "percent_change": 2.5,
            "change": 3.75,
            "high": 155.00,
            "low": 148.00
        }
        
        with patch('etl.utils.get_realtime_stock_data', return_value=mock_stock_data):
            from etl.fetch_utils import fetch_data
            from etl.utils import get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data
            
            result = fetch_data(self.mock_asset, get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data)
            
        self.assertEqual(result, mock_stock_data)

    def test_fetch_data_crypto_success(self, mock_log_fetch, mock_log_utils):
        """Test successful crypto data fetching."""
        mock_crypto_data = {
            "symbol": "BTC",
            "close": 45000.00,
            "percent_change": 1.2,
            "change": 540.00,
            "high": 45500.00,
            "low": 44800.00
        }
        
        with patch('etl.utils.get_realtime_crypto_data', return_value=mock_crypto_data):
            from etl.fetch_utils import fetch_data
            from etl.utils import get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data
            
            result = fetch_data(self.mock_crypto_asset, get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data)
            
        self.assertEqual(result, mock_crypto_data)

    def test_fetch_data_forex_success(self, mock_log_fetch, mock_log_utils):
        """Test successful forex data fetching."""
        mock_forex_data = {
            "symbol": "EUR/USD",
            "close": 1.0850,
            "percent_change": -0.5,
            "change": -0.0054,
            "high": 1.0900,
            "low": 1.0800
        }
        
        with patch('etl.utils.get_realtime_forex_data', return_value=mock_forex_data):
            from etl.fetch_utils import fetch_data
            from etl.utils import get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data
            
            result = fetch_data(self.mock_forex_asset, get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data)
            
        self.assertEqual(result, mock_forex_data)

    def test_fetch_data_unsupported_asset_type(self, mock_log_fetch, mock_log_utils):
        """Test handling of unsupported asset types."""
        unsupported_asset = {"symbol": "TEST", "asset_type": "COMMODITY"}
        
        from etl.fetch_utils import fetch_data
        from etl.utils import get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data
        
        result = fetch_data(unsupported_asset, get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data)
        
        self.assertIsNone(result)

    def test_fetch_data_api_exception(self, mock_log_fetch, mock_log_utils):
        """Test handling of API exceptions during data fetching."""
        with patch('etl.utils.get_realtime_stock_data', side_effect=Exception("API Error")):
            from etl.fetch_utils import fetch_data
            from etl.utils import get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data
            
            with self.assertRaises(Exception):
                fetch_data(self.mock_asset, get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data)

    def test_process_data_valid_response(self, mock_log_fetch, mock_log_utils):
        """Test successful data processing with valid API response."""
        api_response = {
            "symbol": "AAPL",
            "close": 150.00,
            "percent_change": 2.5,
            "change": 3.75,
            "high": 155.00,
            "low": 148.00
        }
        
        from etl.fetch_utils import process_data
        result = process_data(api_response, self.required_fields)
        
        self.assertEqual(result, api_response)

    def test_process_data_missing_field(self, mock_log_fetch, mock_log_utils):
        """Test data processing with missing required field."""
        incomplete_response = {
            "symbol": "AAPL",
            "close": 150.00,
            "percent_change": 2.5,
            "change": 3.75,
            "high": 155.00
            # Missing "low" field
        }
        
        from etl.fetch_utils import process_data
        
        with self.assertRaises(ValueError) as context:
            process_data(incomplete_response, self.required_fields)
        
        self.assertIn("Missing or invalid field 'low'", str(context.exception))

    def test_process_data_null_field(self, mock_log_fetch, mock_log_utils):
        """Test data processing with null field."""
        response_with_null = {
            "symbol": "AAPL",
            "close": 150.00,
            "percent_change": None,  # Null field
            "change": 3.75,
            "high": 155.00,
            "low": 148.00
        }
        
        from etl.fetch_utils import process_data
        
        with self.assertRaises(ValueError) as context:
            process_data(response_with_null, self.required_fields)
        
        self.assertIn("Missing or invalid field 'percent_change'", str(context.exception))

    def test_process_data_empty_response(self, mock_log_fetch, mock_log_utils):
        """Test data processing with empty response."""
        empty_response = {}
        
        from etl.fetch_utils import process_data
        
        with self.assertRaises(ValueError) as context:
            process_data(empty_response, self.required_fields)
        
        self.assertIn("Missing or invalid field 'symbol'", str(context.exception))

    @patch('etl.fetch_utils.get_db_connection')
    def test_fetch_and_insert_data_success(self, mock_get_db_connection, mock_log_fetch, mock_log_utils):
        """Test successful fetch and insert data process."""
        # Mock database connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock API responses
        mock_stock_data = {
            "symbol": "AAPL",
            "close": 150.00,
            "percent_change": 2.5,
            "change": 3.75,
            "high": 155.00,
            "low": 148.00
        }
        
        assets = [self.mock_asset]
        
        with patch('etl.fetch_utils.fetch_data', return_value=mock_stock_data):
            with patch('etl.fetch_utils.process_data', return_value=mock_stock_data):
                with patch('etl.fetch_utils.insert_or_update_data') as mock_insert:
                    from etl.fetch_utils import fetch_and_insert_data
                    from etl.utils import get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data
                    
                    fetch_and_insert_data(
                        assets,
                        self.required_fields,
                        mock_insert,
                        get_realtime_stock_data,
                        get_realtime_crypto_data,
                        get_realtime_forex_data
                    )
                    
                    # Verify insert_or_update_data was called
                    mock_insert.assert_called_once()

    @patch('etl.fetch_utils.get_db_connection')
    def test_fetch_and_insert_data_rate_limit_handling(self, mock_get_db_connection, mock_log_fetch, mock_log_utils):
        """Test handling of rate limit errors with retry logic."""
        # Mock database connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock rate limit error
        rate_limit_error = Exception("429 Rate limit exceeded")
        
        assets = [self.mock_asset]
        
        with patch('etl.fetch_utils.fetch_data', side_effect=rate_limit_error):
            with patch('etl.fetch_utils.time.sleep') as mock_sleep:  # Mock sleep to speed up test
                from etl.fetch_utils import fetch_and_insert_data
                from etl.utils import get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data
                
                fetch_and_insert_data(
                    assets,
                    self.required_fields,
                    Mock(),
                    get_realtime_stock_data,
                    get_realtime_crypto_data,
                    get_realtime_forex_data,
                    max_retries=2,
                    retry_delay=1
                )
                
                # Verify retry logic was triggered
                mock_sleep.assert_called()

    @patch('etl.fetch_utils.get_db_connection')
    def test_fetch_and_insert_data_retry_success(self, mock_get_db_connection, mock_log_fetch, mock_log_utils):
        """Test successful retry after initial failure."""
        # Mock database connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock API responses - first call fails, second succeeds
        mock_stock_data = {
            "symbol": "AAPL",
            "close": 150.00,
            "percent_change": 2.5,
            "change": 3.75,
            "high": 155.00,
            "low": 148.00
        }
        
        assets = [self.mock_asset]
        
        with patch('etl.fetch_utils.fetch_data', side_effect=[Exception("Temporary error"), mock_stock_data]):
            with patch('etl.fetch_utils.process_data', return_value=mock_stock_data):
                with patch('etl.fetch_utils.insert_or_update_data') as mock_insert:
                    with patch('etl.fetch_utils.time.sleep'):  # Mock sleep
                        from etl.fetch_utils import fetch_and_insert_data
                        from etl.utils import get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data
                        
                        fetch_and_insert_data(
                            assets,
                            self.required_fields,
                            mock_insert,
                            get_realtime_stock_data,
                            get_realtime_crypto_data,
                            get_realtime_forex_data,
                            max_retries=3,
                            retry_delay=1
                        )
                        
                        # Verify insert_or_update_data was called after retry
                        mock_insert.assert_called_once()

    def test_handle_api_error_rate_limit(self, mock_log_fetch, mock_log_utils):
        """Test handling of rate limit errors."""
        rate_limit_error = requests.exceptions.HTTPError("429 Rate limit exceeded")
        
        with patch('etl.fetch_utils.time.sleep') as mock_sleep:
            from etl.fetch_utils import handle_api_error
            result = handle_api_error(rate_limit_error, "AAPL", 1, 3, 60)
            
        self.assertTrue(result)  # Should return True to indicate retry
        mock_sleep.assert_called_with(60)

    def test_handle_api_error_max_retries_exceeded(self, mock_log_fetch, mock_log_utils):
        """Test handling when max retries are exceeded."""
        api_error = Exception("API Error")
        
        from etl.fetch_utils import handle_api_error
        result = handle_api_error(api_error, "AAPL", 3, 3, 60)
        
        self.assertFalse(result)  # Should return False when max retries exceeded

    def test_handle_api_error_non_retryable(self, mock_log_fetch, mock_log_utils):
        """Test handling of non-retryable errors."""
        non_retryable_error = Exception("Invalid API key")
        
        from etl.fetch_utils import handle_api_error
        result = handle_api_error(non_retryable_error, "AAPL", 1, 3, 60)
        
        self.assertFalse(result)  # Should return False for non-retryable errors

    @patch('etl.utils.requests.get')
    def test_quote_market_index_data_success(self, mock_get, mock_log_fetch, mock_log_utils):
        """Test successful market index data quoting."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "quoteResponse": {
                "result": [
                    {
                        "symbol": "AAPL",
                        "regularMarketPrice": 150.00,
                        "regularMarketChangePercent": 2.5
                    }
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        with patch.dict('os.environ', {
            'RAPIDAPI_HOST': 'test-host',
            'RAPIDAPI_MARKET_GET_QUOTES': 'test-endpoint',
            'RAPIDAPI_KEY': 'test-key'
        }):
            from etl.utils import quote_market_index_data
            result = quote_market_index_data(["AAPL"])
            
        self.assertIsNotNone(result)
        mock_get.assert_called_once()

    @patch('etl.utils.requests.get')
    def test_quote_market_index_data_missing_env_vars(self, mock_get, mock_log_fetch, mock_log_utils):
        """Test handling of missing environment variables."""
        with patch.dict('os.environ', {}, clear=True):
            from etl.utils import quote_market_index_data
            with self.assertRaises(ValueError) as context:
                quote_market_index_data(["AAPL"])
            
            self.assertIn("Missing required environment variables", str(context.exception))

    @patch('etl.utils.requests.get')
    def test_quote_market_index_data_api_error(self, mock_get, mock_log_fetch, mock_log_utils):
        """Test handling of API errors in market index data quoting."""
        mock_get.side_effect = requests.exceptions.RequestException("API Error")
        
        with patch.dict('os.environ', {
            'RAPIDAPI_HOST': 'test-host',
            'RAPIDAPI_MARKET_GET_QUOTES': 'test-endpoint',
            'RAPIDAPI_KEY': 'test-key'
        }):
            from etl.utils import quote_market_index_data
            with self.assertRaises(requests.exceptions.RequestException):
                quote_market_index_data(["AAPL"])

    def test_data_validation_edge_cases(self, mock_log_fetch, mock_log_utils):
        """Test data validation with various edge cases."""
        from etl.fetch_utils import process_data
        
        # Test with zero values
        zero_data = {
            "symbol": "AAPL",
            "close": 0.0,
            "percent_change": 0.0,
            "change": 0.0,
            "high": 0.0,
            "low": 0.0
        }
        result = process_data(zero_data, self.required_fields)
        self.assertEqual(result, zero_data)
        
        # Test with negative values
        negative_data = {
            "symbol": "AAPL",
            "close": -150.00,
            "percent_change": -2.5,
            "change": -3.75,
            "high": -155.00,
            "low": -148.00
        }
        result = process_data(negative_data, self.required_fields)
        self.assertEqual(result, negative_data)
        
        # Test with very large numbers
        large_data = {
            "symbol": "AAPL",
            "close": 999999.99,
            "percent_change": 999.99,
            "change": 999999.99,
            "high": 999999.99,
            "low": 999999.99
        }
        result = process_data(large_data, self.required_fields)
        self.assertEqual(result, large_data)

    def test_batch_processing_simulation(self, mock_log_fetch, mock_log_utils):
        """Test batch processing simulation with multiple assets."""
        assets = [
            {"symbol": "AAPL", "asset_type": "STOCK"},
            {"symbol": "TSLA", "asset_type": "STOCK"},
            {"symbol": "BTC", "asset_type": "CRYPTO"},
            {"symbol": "ETH", "asset_type": "CRYPTO"}
        ]
        
        mock_responses = [
            {"symbol": "AAPL", "close": 150.00, "percent_change": 2.5, "change": 3.75, "high": 155.00, "low": 148.00},
            {"symbol": "TSLA", "close": 250.00, "percent_change": 1.5, "change": 3.75, "high": 255.00, "low": 248.00},
            {"symbol": "BTC", "close": 45000.00, "percent_change": 1.2, "change": 540.00, "high": 45500.00, "low": 44800.00},
            {"symbol": "ETH", "close": 3000.00, "percent_change": 0.8, "change": 24.00, "high": 3050.00, "low": 2980.00}
        ]
        
        with patch('etl.fetch_utils.fetch_data', side_effect=mock_responses):
            with patch('etl.fetch_utils.process_data', side_effect=mock_responses):
                with patch('etl.fetch_utils.insert_or_update_data') as mock_insert:
                    with patch('etl.fetch_utils.get_db_connection'):
                        from etl.fetch_utils import fetch_and_insert_data
                        from etl.utils import get_realtime_stock_data, get_realtime_crypto_data, get_realtime_forex_data
                        
                        fetch_and_insert_data(
                            assets,
                            self.required_fields,
                            mock_insert,
                            get_realtime_stock_data,
                            get_realtime_crypto_data,
                            get_realtime_forex_data
                        )
                        
                        # Verify insert_or_update_data was called for each asset
                        self.assertEqual(mock_insert.call_count, 4)


if __name__ == '__main__':
    unittest.main() 