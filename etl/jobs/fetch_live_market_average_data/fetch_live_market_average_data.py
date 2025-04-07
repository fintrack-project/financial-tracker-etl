import requests
import json
from datetime import datetime
from etl.utils import get_db_connection, log_message

# Constants
SPY_SYMBOL = "SPY"
QQQ_SYMBOL = "QQQ"
US_MARKET_CLOSE_TIME = "16:00"  # 4:00 PM ET

def fetch_market_data(api_url, api_key, symbols):
    """
    Fetch market data for the given symbols from Yahoo Finance via RapidAPI.
    """
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "yahoo-finance15.p.rapidapi.com"
    }
    params = {"symbol": ",".join(symbols)}
    response = requests.get(api_url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def process_market_data(data):
    """
    Extract price and percent dropped for each symbol.
    """
    processed_data = []
    for symbol_data in data:
        symbol = symbol_data.get("symbol")
        price = symbol_data.get("regularMarketPrice")
        percent_change = symbol_data.get("regularMarketChangePercent")
        processed_data.append({
            "symbol": symbol,
            "price": price,
            "percent_change": percent_change
        })
    return processed_data

def save_market_data_to_db(data, db_config):
    """
    Save the processed market data into the database.
    """
    connection = get_db_connection(db_config)
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
    """
    Main function to fetch, process, and save market data.
    """
    # Load API and DB configuration
    with open('config/api_config.json') as f:
        api_config = json.load(f)
    with open('config/db_config.json') as f:
        db_config = json.load(f)

    # Step 1: Fetch market data
    symbols = [SPY_SYMBOL, QQQ_SYMBOL]
    raw_data = fetch_market_data(api_config['api_url'], api_config['api_key'], symbols)

    # Step 2: Process market data
    processed_data = process_market_data(raw_data)

    # Step 3: Save data to the database
    save_market_data_to_db(processed_data, db_config)

    log_message("Market data fetched and saved successfully.")