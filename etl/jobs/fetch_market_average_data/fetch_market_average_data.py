import os
from datetime import datetime
from dotenv import load_dotenv
from etl.utils import get_db_connection, log_message, load_env_variables, fetch_market_data

# Load environment variables from .env file
env_vars = load_env_variables()

# Constants
SP500_SYMBOL = "^GSPC"
NASDAQ100_SYMBOL = "^NDX"
US_MARKET_CLOSE_TIME = "16:00"  # 4:00 PM ET^

def process_market_data(data):
    """
    Extract price and percent dropped for each symbol.
    """
    processed_data = []
    for symbol_data in data:
        symbol = symbol_data.get("symbol")
        price = symbol_data.get("regularMarketPrice")
        price_change = symbol_data.get("regularMarketChange")
        percent_change = symbol_data.get("regularMarketChangePercent")
        price_high = symbol_data.get("regularMarketDayHigh")
        price_low = symbol_data.get("regularMarketDayLow")
        processed_data.append({
            "symbol": symbol,
            "price": price,
            "price_change": price_change,
            "percent_change": percent_change,
            "price_high": price_high,
            "price_low": price_low
        })
    return processed_data

def save_market_data_to_db(data):
    """
    Save the processed market data into the database.
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    for record in data:
        cursor.execute("""
            INSERT INTO market_average_data (symbol, price, price_change, percent_change, price_high, price_low, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            record['symbol'],
            record['price'],
            record['price_change'],
            record['percent_change'],
            record['price_high'],
            record['price_low'],
            datetime.now()
        ))
    connection.commit()
    cursor.close()
    connection.close()

def run():
    print("Running fetch_market_average_data job...")
    """
    Main function to fetch, process, and save market data.
    """
    # Step 1: Fetch market data
    symbols = [SP500_SYMBOL, NASDAQ100_SYMBOL]
    raw_data = fetch_market_data(symbols)

    # Step 2: Process market data
    processed_data = process_market_data(raw_data)

    # Step 3: Save data to the database
    save_market_data_to_db(processed_data)

    log_message("Market data fetched and saved successfully.")

if __name__ == "__main__":
    run()