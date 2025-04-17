import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from etl.jobs.fetch_market_data.fetch_market_data import (
    validate_symbols,
    get_symbols_needing_update,
    update_asset_prices_in_db,
    run
)

class TestFetchMarketData(unittest.TestCase):

    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_db_connection")
    def test_validate_symbols(self, mock_get_db_connection):
        # Mock the database connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
    
        # Mock the query result
        mock_cursor.fetchall.return_value = [("AAPL",), ("TSLA",)]
    
        # Call the function
        symbols = ["AAPL", "TSLA", "INVALID"]
        valid_symbols = validate_symbols(symbols)
    
        # Assertions
        self.assertEqual(valid_symbols, ["AAPL", "TSLA"])
        mock_cursor.execute.assert_called_once_with(
            """
            SELECT DISTINCT symbol
            FROM holdings
            WHERE symbol = ANY(%s)
            """.strip(), (symbols,)
        )

    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_db_connection")
    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_closest_us_market_closing_time", return_value=datetime(2025, 4, 16, 20, 0, 0))
    def test_get_symbols_needing_update(self, mock_get_closing_time, mock_get_db_connection):
        # Mock the database connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Mock the query result
        mock_cursor.fetchall.return_value = [("AAPL",), ("TSLA",)]

        # Call the function
        symbols = ["AAPL", "TSLA", "INVALID"]
        symbols_needing_update = get_symbols_needing_update(symbols)

        # Assertions
        self.assertEqual(symbols_needing_update, ["AAPL", "TSLA"])
        mock_cursor.execute.assert_called_once()

    @patch("etl.jobs.fetch_market_data.fetch_market_data.publish_price_update_complete")
    def test_run_invalid_input(self, mock_publish):
        # Call the run function with invalid input
        run("INVALID_INPUT")

        # Assertions
        mock_publish.assert_not_called()

    @patch("etl.jobs.fetch_market_data.fetch_market_data.validate_symbols", return_value=[])
    @patch("etl.jobs.fetch_market_data.fetch_market_data.publish_price_update_complete")
    def test_run_no_valid_symbols(self, mock_publish, mock_validate):
        # Call the run function
        run({"symbols": ["INVALID"]})
    
        # Assertions
        mock_validate.assert_called_once_with(["INVALID"])
        mock_publish.assert_not_called()

    @patch("etl.jobs.fetch_market_data.fetch_market_data.validate_symbols", return_value=["AAPL", "TSLA"])
    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_symbols_needing_update", return_value=["AAPL", "TSLA"])
    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_db_connection")
    @patch("etl.jobs.fetch_market_data.fetch_market_data.quote_market_data")
    @patch("etl.jobs.fetch_market_data.fetch_market_data.update_asset_prices_in_db")
    @patch("etl.jobs.fetch_market_data.fetch_market_data.publish_price_update_complete")
    def test_run_symbols_needing_update(self, mock_publish, mock_update_db, mock_quote, mock_get_db_connection, mock_get_symbols, mock_validate):
        # Mock the database connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Mock the quote_market_data function
        mock_quote.return_value = [
            {"symbol": "AAPL", "regularMarketPrice": 150.0},
            {"symbol": "TSLA", "regularMarketPrice": 700.0},
        ]

        # Call the run function
        run({"symbols": ["AAPL", "TSLA"]})

        # Assertions
        mock_validate.assert_called_once_with(["AAPL", "TSLA"])
        mock_get_symbols.assert_called_once_with(["AAPL", "TSLA"])
        mock_quote.assert_called_once_with(["AAPL", "TSLA"])
        mock_update_db.assert_called_once_with(mock_quote.return_value)
        mock_publish.assert_called_once_with(
            ["AAPL", "TSLA"], ["AAPL", "TSLA"]
        )

    @patch("etl.jobs.fetch_market_data.fetch_market_data.validate_symbols", return_value=["AAPL", "TSLA"])
    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_db_connection")
    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_symbols_needing_update", return_value=[])
    @patch("etl.jobs.fetch_market_data.fetch_market_data.publish_price_update_complete")
    def test_run_no_symbols_needing_update(self, mock_publish, mock_get_symbols, mock_get_db_connection, mock_validate):
        # Mock the database connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
    
        # Call the run function
        run({"symbols": ["AAPL", "TSLA"]})
    
        # Assertions
        mock_validate.assert_called_once_with(["AAPL", "TSLA"])
        mock_get_symbols.assert_called_once_with(["AAPL", "TSLA"])
        mock_publish.assert_called_once_with(["AAPL", "TSLA"], [])

if __name__ == "__main__":
    unittest.main()