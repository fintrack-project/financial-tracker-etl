import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from etl.jobs.fetch_market_data.fetch_market_data import (
    get_assets_needing_update,
    insert_or_update_data,
    run
)
from etl.fetch_utils import (
    fetch_data,
    process_data
)
from tests.test_utils.mock_responses import get_mock_api_response

class TestFetchMarketData(unittest.TestCase):

    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_db_connection")
    def test_get_assets_needing_update(self, mock_get_db_connection):
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

    @patch("etl.fetch_utils.fetch_and_insert_data")
    @patch("etl.main.publish_kafka_messages")
    def test_run_invalid_input(self, mock_publish, mock_fetch_and_insert):
        # Call the run function with invalid input
        run("INVALID_INPUT")

        # Assertions
        mock_publish.assert_not_called()
        mock_fetch_and_insert.assert_not_called()

    @patch("etl.main.publish_kafka_messages")
    @patch("etl.fetch_utils.fetch_and_insert_data")
    def test_run_no_valid_assets(self, mock_fetch_and_insert, mock_publish):
        # Call the run function with assets that don't need updates
        run({"assets": [{"symbol": "INVALID", "asset_type": "STOCK"}]})

        # Assertions
        mock_publish.assert_not_called()
        mock_fetch_and_insert.assert_not_called()

    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_assets_needing_update", return_value=[{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
    @patch("etl.fetch_utils.fetch_and_insert_data")
    @patch("etl.main.publish_kafka_messages")
    def test_run_assets_needing_update(self, mock_publish, mock_fetch_and_insert, mock_get_assets):
        # Call the run function
        run({"assets": [{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}]})

        # Assertions
        mock_get_assets.assert_called_once_with([{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
        mock_fetch_and_insert.assert_called_once_with([{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
        mock_publish.assert_called_once()

    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_assets_needing_update", return_value=[])
    @patch("etl.main.publish_kafka_messages")
    @patch("etl.fetch_utils.fetch_and_insert_data")
    def test_run_no_assets_needing_update(self, mock_fetch_and_insert, mock_publish, mock_get_assets):
        # Call the run function
        run({"assets": [{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}]})

        # Assertions
        mock_get_assets.assert_called_once_with([{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TSLA", "asset_type": "STOCK"}])
        mock_fetch_and_insert.assert_not_called()
        mock_publish.assert_called_once()

    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_realtime_stock_data")
    def test_fetch_data(self, mock_get_realtime_stock_data):
        # Mock the API response
        mock_get_realtime_stock_data.side_effect = get_mock_api_response

        # Test fetching data for a valid stock
        asset = {"symbol": "AAPL", "asset_type": "STOCK"}
        data = fetch_data(asset)
        self.assertEqual(data["symbol"], "AAPL")
        self.assertEqual(data["close"], "204.60001")

        # Test fetching data for an unsupported asset type
        asset = {"symbol": "BTC", "asset_type": "UNKNOWN"}
        data = fetch_data(asset)
        self.assertIsNone(data)

    def test_process_data(self):
        # Mock API response
        data = get_mock_api_response("AAPL")
        symbol = "AAPL"

        # Test processing valid data
        timestamp, price, percent_change = process_data(data, symbol)
        self.assertEqual(timestamp, 1745415000)
        self.assertEqual(price, 204.60001)
        self.assertEqual(percent_change, 2.43316)

        # Test processing invalid data (missing field)
        data.pop("close")
        with self.assertRaises(ValueError) as context:
            process_data(data, symbol)
        self.assertIn("Missing or invalid field 'close'", str(context.exception))

    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_db_connection")
    def test_insert_or_update_data(self, mock_get_db_connection):
        # Mock the database connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
    
        # Test inserting or updating data
        insert_or_update_data(
            cursor=mock_cursor,
            connection=mock_connection,
            symbol="AAPL",
            asset_type="STOCK",
            price=204.60001,
            percent_change=2.43316,
            timestamp=1745415000
        )
    
        # Normalize the SQL query string
        expected_query = """
            INSERT INTO market_data (symbol, asset_type, price, percent_change, updated_at)
            VALUES (%s, %s, %s, %s, to_timestamp(%s))
            ON CONFLICT (symbol)
            DO UPDATE SET
                price = EXCLUDED.price,
                percent_change = EXCLUDED.percent_change,
                updated_at = EXCLUDED.updated_at,
                asset_type = EXCLUDED.asset_type
        """.strip().replace("\n", " ").replace("  ", " ")
    
        actual_query = mock_cursor.execute.call_args[0][0].strip().replace("\n", " ").replace("  ", " ")
    
        # Assert that the normalized queries match
        self.assertEqual(expected_query, actual_query)
    
        # Assert the parameters
        mock_cursor.execute.assert_called_once_with(
            mock_cursor.execute.call_args[0][0],  # Use the actual query from the mock
            ("AAPL", "STOCK", 204.60001, 2.43316, 1745415000)
        )
        mock_connection.commit.assert_called_once()

    @patch("etl.fetch_utils.fetch_data")
    @patch("etl.fetch_utils.process_data")
    @patch("etl.jobs.fetch_market_data.fetch_market_data.insert_or_update_data")
    @patch("etl.jobs.fetch_market_data.fetch_market_data.get_db_connection")
    def test_fetch_and_insert_data(self, mock_get_db_connection, mock_insert_or_update_data, mock_process_data, mock_fetch_data):
        # Mock the database connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_db_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
    
        # Mock fetch_data and process_data
        mock_fetch_data.side_effect = lambda asset: get_mock_api_response(asset["symbol"])
        mock_process_data.side_effect = lambda data, symbol: (
            data["timestamp"], float(data["close"]), float(data["percent_change"])
        )
    
        # Test fetch_and_insert_data
        assets = [{"symbol": "AAPL", "asset_type": "STOCK"}, {"symbol": "TQQQ", "asset_type": "STOCK"}]
        from etl.fetch_utils import fetch_and_insert_data
        fetch_and_insert_data(assets)
    
        # Assertions
        mock_fetch_data.assert_any_call({"symbol": "AAPL", "asset_type": "STOCK"})
        mock_fetch_data.assert_any_call({"symbol": "TQQQ", "asset_type": "STOCK"})
        self.assertEqual(mock_fetch_data.call_count, 2)
    
        mock_process_data.assert_any_call(get_mock_api_response("AAPL"), "AAPL")
        mock_process_data.assert_any_call(get_mock_api_response("TQQQ"), "TQQQ")
        self.assertEqual(mock_process_data.call_count, 2)
    
        # Check the calls to insert_or_update_data
        mock_insert_or_update_data.assert_any_call(
            mock_cursor,
            mock_connection,
            "AAPL",
            "STOCK",
            float(204.60001),
            float(2.43316),
            int(1745415000)
        )
        mock_insert_or_update_data.assert_any_call(
            mock_cursor,
            mock_connection,
            "TQQQ",
            "STOCK",
            float(48.049999),
            float(6.63559),
            int(1745415000)
        )
        self.assertEqual(mock_insert_or_update_data.call_count, 2)

if __name__ == "__main__":
    unittest.main()