import unittest
from etl.jobs.fetch_live_market_average_data.fetch_live_market_average_data import process_market_data

class TestFetchLiveMarketAverageData(unittest.TestCase):
    def test_process_market_data(self):
        raw_data = [
            {"symbol": "SPY", "regularMarketPrice": 400.5, "regularMarketChangePercent": -0.25},
            {"symbol": "QQQ", "regularMarketPrice": 300.2, "regularMarketChangePercent": -0.15}
        ]
        expected_output = [
            {"symbol": "SPY", "price": 400.5, "percent_change": -0.25},
            {"symbol": "QQQ", "price": 300.2, "percent_change": -0.15}
        ]
        self.assertEqual(process_market_data(raw_data), expected_output)

if __name__ == "__main__":
    unittest.main()