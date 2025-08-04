import unittest
from unittest.mock import patch, MagicMock, Mock
import requests
import time
from datetime import datetime, timezone
from etl.utils import (
    get_realtime_stock_data,
    get_realtime_crypto_data,
    get_realtime_forex_data,
    quote_market_index_data,
    get_historical_stock_data,
    get_historical_crypto_data,
    get_historical_fx_data
)
from etl.fetch_utils import (
    fetch_data,
    process_data,
    handle_api_error
)


class TestAPIIntegration(unittest.TestCase):
    """Test suite for API integration functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_asset = {
            "symbol": "AAPL",
            "asset_type": "STOCK"
        }
        self.required_fields = ["symbol", "close", "percent_change", "change", "high", "low"]

    @patch('etl.utils.requests.get')
    def test_get_realtime_stock_data_success(self, mock_get):
        """Test successful real-time stock data retrieval."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "quoteResponse": {
                "result": [
                    {
                        "symbol": "AAPL",
                        "regularMarketPrice": 150.00,
                        "regularMarketChangePercent": 2.5,
                        "regularMarketChange": 3.75,
                        "regularMarketDayHigh": 155.00,
                        "regularMarketDayLow": 148.00
                    }
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_realtime_stock_data("AAPL")
        
        self.assertIsNotNone(result)
        self.assertEqual(result["symbol"], "AAPL")
        self.assertEqual(result["close"], 150.00)
        mock_get.assert_called_once()

    @patch('etl.utils.requests.get')
    def test_get_realtime_stock_data_api_error(self, mock_get):
        """Test handling of API errors in stock data retrieval."""
        mock_get.side_effect = requests.exceptions.RequestException("API Error")
        
        with self.assertRaises(requests.exceptions.RequestException):
            get_realtime_stock_data("AAPL")

    @patch('etl.utils.requests.get')
    def test_get_realtime_stock_data_rate_limit(self, mock_get):
        """Test handling of rate limit errors in stock data retrieval."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("429 Rate limit exceeded")
        mock_get.return_value = mock_response
        
        with self.assertRaises(requests.exceptions.HTTPError):
            get_realtime_stock_data("AAPL")

    @patch('etl.utils.requests.get')
    def test_get_realtime_crypto_data_success(self, mock_get):
        """Test successful real-time crypto data retrieval."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": {
                "BTC": {
                    "quote": {
                        "USD": {
                            "price": 45000.00,
                            "percent_change_24h": 1.2,
                            "volume_24h": 25000000000,
                            "market_cap": 850000000000
                        }
                    }
                }
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_realtime_crypto_data("BTC")
        
        self.assertIsNotNone(result)
        self.assertEqual(result["symbol"], "BTC")
        self.assertEqual(result["close"], 45000.00)
        mock_get.assert_called_once()

    @patch('etl.utils.requests.get')
    def test_get_realtime_forex_data_success(self, mock_get):
        """Test successful real-time forex data retrieval."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "Realtime Currency Exchange Rate": {
                "5. Exchange Rate": "1.0850",
                "8. Bid Price": "1.0845",
                "9. Ask Price": "1.0855"
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_realtime_forex_data("EUR", "USD")
        
        self.assertIsNotNone(result)
        self.assertEqual(result["symbol"], "EUR/USD")
        self.assertEqual(result["close"], 1.0850)
        mock_get.assert_called_once()

    @patch('etl.utils.requests.get')
    def test_quote_market_index_data_success(self, mock_get):
        """Test successful market index data quoting."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "quoteResponse": {
                "result": [
                    {
                        "symbol": "AAPL",
                        "regularMarketPrice": 150.00,
                        "regularMarketChangePercent": 2.5,
                        "regularMarketChange": 3.75,
                        "regularMarketDayHigh": 155.00,
                        "regularMarketDayLow": 148.00
                    },
                    {
                        "symbol": "TSLA",
                        "regularMarketPrice": 250.00,
                        "regularMarketChangePercent": 1.5,
                        "regularMarketChange": 3.75,
                        "regularMarketDayHigh": 255.00,
                        "regularMarketDayLow": 248.00
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
            result = quote_market_index_data(["AAPL", "TSLA"])
            
        self.assertIsNotNone(result)
        self.assertEqual(len(result["quoteResponse"]["result"]), 2)
        mock_get.assert_called_once()

    @patch('etl.utils.requests.get')
    def test_quote_market_index_data_missing_env_vars(self, mock_get):
        """Test handling of missing environment variables."""
        with patch.dict('os.environ', {}, clear=True):
            with self.assertRaises(ValueError) as context:
                quote_market_index_data(["AAPL"])
            
            self.assertIn("Missing required environment variables", str(context.exception))

    @patch('etl.utils.requests.get')
    def test_get_historical_stock_data_success(self, mock_get):
        """Test successful historical stock data retrieval."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1640995200, 1641081600],
                        "indicators": {
                            "quote": [
                                {
                                    "close": [150.00, 152.00],
                                    "high": [155.00, 157.00],
                                    "low": [148.00, 150.00],
                                    "volume": [1000000, 1100000]
                                }
                            ]
                        }
                    }
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_historical_stock_data("AAPL", "2025-01-01", "2025-01-02")
        
        self.assertIsNotNone(result)
        self.assertIn("chart", result)
        mock_get.assert_called_once()

    @patch('etl.utils.requests.get')
    def test_get_historical_crypto_data_success(self, mock_get):
        """Test successful historical crypto data retrieval."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "Data": {
                "Data": [
                    {
                        "time": 1640995200,
                        "close": 45000.00,
                        "high": 45500.00,
                        "low": 44800.00,
                        "volumefrom": 1000
                    }
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_historical_crypto_data("BTC", "USD", "2025-01-01", "2025-01-02")
        
        self.assertIsNotNone(result)
        self.assertIn("Data", result)
        mock_get.assert_called_once()

    def test_rate_limiting_simulation(self):
        """Test rate limiting behavior simulation."""
        # Simulate rate limiting with delays
        start_time = time.time()
        
        with patch('etl.utils.requests.get') as mock_get:
            mock_get.side_effect = [
                requests.exceptions.HTTPError("429 Rate limit exceeded"),
                MagicMock(json=lambda: {"data": "success"})
            ]
            
            # First call should fail with rate limit
            with self.assertRaises(requests.exceptions.HTTPError):
                get_realtime_stock_data("AAPL")
            
            # Second call should succeed after delay
            with patch('time.sleep'):  # Mock sleep to speed up test
                result = get_realtime_stock_data("AAPL")
                self.assertIsNotNone(result)

    def test_retry_logic_simulation(self):
        """Test retry logic simulation."""
        with patch('etl.utils.requests.get') as mock_get:
            # Simulate 3 failures followed by success
            mock_get.side_effect = [
                requests.exceptions.RequestException("Temporary error"),
                requests.exceptions.RequestException("Temporary error"),
                requests.exceptions.RequestException("Temporary error"),
                MagicMock(json=lambda: {"data": "success"})
            ]
            
            # Should succeed after retries
            with patch('time.sleep'):  # Mock sleep
                result = get_realtime_stock_data("AAPL")
                self.assertIsNotNone(result)

    def test_api_error_handling_rate_limit(self):
        """Test API error handling for rate limit errors."""
        rate_limit_error = requests.exceptions.HTTPError("429 Rate limit exceeded")
        
        with patch('etl.fetch_utils.time.sleep') as mock_sleep:
            result = handle_api_error(rate_limit_error, "AAPL", 1, 3, 60)
            
        self.assertTrue(result)  # Should return True to indicate retry
        mock_sleep.assert_called_with(60)

    def test_api_error_handling_max_retries(self):
        """Test API error handling when max retries exceeded."""
        api_error = requests.exceptions.RequestException("API Error")
        
        result = handle_api_error(api_error, "AAPL", 3, 3, 60)
        
        self.assertFalse(result)  # Should return False when max retries exceeded

    def test_api_error_handling_non_retryable(self):
        """Test API error handling for non-retryable errors."""
        non_retryable_error = requests.exceptions.HTTPError("401 Unauthorized")
        
        result = handle_api_error(non_retryable_error, "AAPL", 1, 3, 60)
        
        self.assertFalse(result)  # Should return False for non-retryable errors

    @patch('etl.utils.requests.get')
    def test_api_response_validation_success(self, mock_get):
        """Test successful API response validation."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "quoteResponse": {
                "result": [
                    {
                        "symbol": "AAPL",
                        "regularMarketPrice": 150.00,
                        "regularMarketChangePercent": 2.5,
                        "regularMarketChange": 3.75,
                        "regularMarketDayHigh": 155.00,
                        "regularMarketDayLow": 148.00
                    }
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_realtime_stock_data("AAPL")
        
        # Validate response structure
        self.assertIn("symbol", result)
        self.assertIn("close", result)
        self.assertIn("percent_change", result)
        self.assertIn("change", result)
        self.assertIn("high", result)
        self.assertIn("low", result)

    @patch('etl.utils.requests.get')
    def test_api_response_validation_missing_data(self, mock_get):
        """Test API response validation with missing data."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "quoteResponse": {
                "result": []  # Empty result
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_realtime_stock_data("AAPL")
        
        # Should handle missing data gracefully
        self.assertIsNone(result)

    @patch('etl.utils.requests.get')
    def test_api_response_validation_invalid_structure(self, mock_get):
        """Test API response validation with invalid structure."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "invalid": "structure"  # Invalid response structure
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_realtime_stock_data("AAPL")
        
        # Should handle invalid structure gracefully
        self.assertIsNone(result)

    def test_concurrent_api_requests_simulation(self):
        """Test simulation of concurrent API requests."""
        with patch('etl.utils.requests.get') as mock_get:
            mock_get.return_value = MagicMock(
                json=lambda: {"data": "success"},
                raise_for_status=lambda: None
            )
            
            # Simulate concurrent requests
            results = []
            for i in range(5):
                result = get_realtime_stock_data(f"SYMBOL{i}")
                results.append(result)
            
            # Verify all requests completed
            self.assertEqual(len(results), 5)
            self.assertEqual(mock_get.call_count, 5)

    @patch('etl.utils.requests.get')
    def test_api_timeout_handling(self, mock_get):
        """Test handling of API timeouts."""
        mock_get.side_effect = requests.exceptions.Timeout("Request timeout")
        
        with self.assertRaises(requests.exceptions.Timeout):
            get_realtime_stock_data("AAPL")

    @patch('etl.utils.requests.get')
    def test_api_connection_error_handling(self, mock_get):
        """Test handling of API connection errors."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection error")
        
        with self.assertRaises(requests.exceptions.ConnectionError):
            get_realtime_stock_data("AAPL")

    def test_data_transformation_accuracy(self):
        """Test accuracy of data transformation from API response."""
        # Mock API response
        api_response = {
            "quoteResponse": {
                "result": [
                    {
                        "symbol": "AAPL",
                        "regularMarketPrice": 150.00,
                        "regularMarketChangePercent": 2.5,
                        "regularMarketChange": 3.75,
                        "regularMarketDayHigh": 155.00,
                        "regularMarketDayLow": 148.00
                    }
                ]
            }
        }
        
        # Simulate transformation
        with patch('etl.utils.requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = api_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            result = get_realtime_stock_data("AAPL")
            
            # Verify transformation accuracy
            self.assertEqual(result["symbol"], "AAPL")
            self.assertEqual(result["close"], 150.00)
            self.assertEqual(result["percent_change"], 2.5)
            self.assertEqual(result["change"], 3.75)
            self.assertEqual(result["high"], 155.00)
            self.assertEqual(result["low"], 148.00)

    def test_api_response_consistency(self):
        """Test consistency of API responses across multiple calls."""
        with patch('etl.utils.requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "quoteResponse": {
                    "result": [
                        {
                            "symbol": "AAPL",
                            "regularMarketPrice": 150.00,
                            "regularMarketChangePercent": 2.5,
                            "regularMarketChange": 3.75,
                            "regularMarketDayHigh": 155.00,
                            "regularMarketDayLow": 148.00
                        }
                    ]
                }
            }
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            # Make multiple calls
            results = []
            for _ in range(3):
                result = get_realtime_stock_data("AAPL")
                results.append(result)
            
            # Verify consistency
            for result in results:
                self.assertEqual(result["symbol"], "AAPL")
                self.assertEqual(result["close"], 150.00)


if __name__ == '__main__':
    unittest.main() 