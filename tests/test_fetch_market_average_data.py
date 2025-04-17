import unittest
import datetime
from unittest.mock import patch, MagicMock

# Mock load_env_variables at the module level
with patch("etl.utils.load_env_variables", return_value={"RAPIDAPI_URL": "https://dummy-url.com", "RAPIDAPI_KEY": "dummy-key"}):
    from etl.jobs.fetch_market_average_data.fetch_market_average_data import run

class TestFetchMarketAverageData(unittest.TestCase):

    @patch("etl.jobs.fetch_market_average_data.fetch_market_average_data.get_existing_market_average_data")
    @patch("etl.jobs.fetch_market_average_data.fetch_market_average_data.publish_market_average_data_update_complete")
    @patch("etl.jobs.fetch_market_average_data.fetch_market_average_data.get_closest_us_market_closing_time", return_value=datetime.datetime(2025, 4, 16, 20, 0, 0))
    def test_run_with_existing_data(self, mock_closing_time, mock_publish, mock_get_existing):
        # Mock existing data
        mock_get_existing.return_value = [
            {"symbol": "^GSPC", "price": 4120.5},
            {"symbol": "^NDX", "price": 13450.75},
        ]

        # Call the run function
        run({"symbols": ["^GSPC", "^NDX"]})  # Updated to use "symbols"

        # Assertions
        mock_get_existing.assert_called_once_with(["^GSPC", "^NDX"], mock_closing_time.return_value)
        mock_publish.assert_called_once_with(mock_get_existing.return_value)

if __name__ == "__main__":
    unittest.main()