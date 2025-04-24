import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from etl.jobs.fetch_market_data.fetch_market_data import (
    validate_assets,
    get_assets_needing_update,
    fetch_and_insert_data,
    run
)

class TestFetchMarketData(unittest.TestCase):

    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_db_connection")
    def test_validate_assets(self, mock_get_db_connection):
        # Mock the database connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Mock the query result
        mock_cursor.fetchall.return_value = [("AAPL",), ("TSLA",)]

        # Call the function
        assets = [{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}, {"symbol": "INVALID", "asset_type": "STOCK"}]
        valid_assets = validate_assets(assets)

        # Assertions
        self.assertEqual(valid_assets, [{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
        mock_cursor.execute.assert_called_once_with(
            """
            SELECT DISTINCT symbol
            FROM holdings
            WHERE symbol = ANY(%s)
            """.strip(), (["AAPL", "TSLA", "INVALID"],)
        )

    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_db_connection")
    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_closest_us_market_closing_time", return_value=datetime(2025, 4, 16, 20, 0, 0))
    def test_get_assets_needing_update(self, mock_get_closing_time, mock_get_db_connection):
        # Mock the database connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Mock the query result
        mock_cursor.fetchall.return_value = [("AAPL",), ("TSLA",)]

        # Call the function
        assets = [{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}, {"symbol": "INVALID", "asset_type": "STOCK"}]
        assets_needing_update = get_assets_needing_update(assets)

        # Assertions
        self.assertEqual(assets_needing_update, [{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
        mock_cursor.execute.assert_called_once()

    @patch("etl.jobs.fetch_market_data.fetch_market_data.fetch_and_insert_data")
    @patch("etl.jobs.fetch_market_data.fetch_market_data.publish_price_update_complete")
    def test_run_invalid_input(self, mock_publish, mock_fetch_and_insert):
        # Call the run function with invalid input
        run("INVALID_INPUT")

        # Assertions
        mock_publish.assert_not_called()
        mock_fetch_and_insert.assert_not_called()

    @patch("etl.jobs.fetch_market_data.fetch_market_data.validate_assets", return_value=[])
    @patch("etl.jobs.fetch_market_data.fetch_market_data.publish_price_update_complete")
    @patch("etl.jobs.fetch_market_data.fetch_market_data.fetch_and_insert_data")
    def test_run_no_valid_assets(self, mock_fetch_and_insert, mock_publish, mock_validate):
        # Call the run function
        run({"assets": [{"symbol": "INVALID", "asset_type": "STOCK"}]})

        # Assertions
        mock_validate.assert_called_once_with([{"symbol": "INVALID", "asset_type": "STOCK"}])
        mock_publish.assert_not_called()
        mock_fetch_and_insert.assert_not_called()

    @patch("etl.jobs.fetch_market_data.fetch_market_data.validate_assets", return_value=[{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_assets_needing_update", return_value=[{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
    @patch("etl.jobs.fetch_market_data.fetch_market_data.fetch_and_insert_data")
    @patch("etl.jobs.fetch_market_data.fetch_market_data.publish_price_update_complete")
    def test_run_assets_needing_update(self, mock_publish, mock_fetch_and_insert, mock_get_assets, mock_validate):
        # Call the run function
        run({"assets": [{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}]})

        # Assertions
        mock_validate.assert_called_once_with([{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
        mock_get_assets.assert_called_once_with([{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
        mock_fetch_and_insert.assert_called_once_with([{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
        mock_publish.assert_called_once_with(
            [{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}],
            [{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}]
        )

    @patch("etl.jobs.fetch_market_data.fetch_market_data.validate_assets", return_value=[{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_assets_needing_update", return_value=[])
    @patch("etl.jobs.fetch_market_data.fetch_market_data.publish_price_update_complete")
    @patch("etl.jobs.fetch_market_data.fetch_market_data.fetch_and_insert_data")
    def test_run_no_assets_needing_update(self, mock_fetch_and_insert, mock_publish, mock_get_assets, mock_validate):
        # Call the run function
        run({"assets": [{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}]})

        # Assertions
        mock_validate.assert_called_once_with([{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
        mock_get_assets.assert_called_once_with([{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
        mock_fetch_and_insert.assert_not_called()
        mock_publish.assert_called_once_with(
            [{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}],
            []
        )

if __name__ == "__main__":
    unittest.main()