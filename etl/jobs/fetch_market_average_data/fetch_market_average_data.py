import requests
import os
from datetime import datetime
from dotenv import load_dotenv
from etl.utils import get_db_connection, log_message, load_env_variables

# Load environment variables from .env file
env_vars = load_env_variables()

# Constants
SP500_SYMBOL = "^GSPC"
NASDAQ100_SYMBOL = "^NDX"
US_MARKET_CLOSE_TIME = "16:00"  # 4:00 PM ET^

def fetch_market_data(symbols):
    """
    Fetch market data for the given symbols from Yahoo Finance via RapidAPI.
    """
    api_url = env_vars.get("RAPIDAPI_URL")
    api_key = env_vars.get("RAPIDAPI_KEY")

    if not api_url or not api_key:
        raise ValueError("API_URL or API_KEY is not set. Check your .env file.")

    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "apidojo-yahoo-finance-v1.p.rapidapi.com"
    }
    params = {"symbols": ",".join(symbols)}
    response = requests.get(api_url, headers=headers, params=params)
    response.raise_for_status()

    return response.json()

def process_market_data(data):
    """
    Extract price and percent dropped for each symbol.
    """
    processed_data = []
    # Navigate to the "result" array in the response
    results = data.get("quoteResponse", {}).get("result", [])
    for symbol_data in results:
        symbol = symbol_data.get("symbol")
        price = symbol_data.get("regularMarketPrice")
        percent_change = symbol_data.get("regularMarketChangePercent")
        processed_data.append({
            "symbol": symbol,
            "price": price,
            "percent_change": percent_change
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
            INSERT INTO market_data (symbol, price, percent_change, timestamp)
            VALUES (%s, %s, %s, %s)
        """, (record['symbol'], record['price'], record['percent_change'], datetime.now()))
    connection.commit()
    cursor.close()
    connection.close()

def run():
    print("Running fetch_live_market_average_data job...")
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