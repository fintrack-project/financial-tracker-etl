import psycopg2
import sys
import logging
import requests
import json
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import pytz
from urllib.parse import quote

# Load environment variables from .env file
load_dotenv()

def get_db_connection(db_config=None):
    """
    Establish a connection to the database.
    """
    if not db_config:
        db_config = {
            "dbname": os.getenv("DATABASE_NAME"),
            "user": os.getenv("DATABASE_USER"),
            "password": os.getenv("DATABASE_PASSWORD"),
            "host": os.getenv("DATABASE_HOST"),
            "port": os.getenv("DATABASE_PORT"),
        }
    return psycopg2.connect(**db_config)

def log_message(message):
    log_dir = "logs"
    log_file = os.path.join(log_dir, "etl.log")

    # Ensure the logs directory exists
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.info(message)

def load_env_variables(env_file=".env"):
    """
    Load environment variables from a .env file.
    """
    env_vars = {}
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                # Ignore comments and empty lines
                line = line.strip()
                if line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    env_vars[key.strip()] = value.strip()
    else:
        raise FileNotFoundError(f"{env_file} file not found.")
    return env_vars

def quote_market_data(symbols, region="US"):
    """
    Quote market data for the given symbols from Yahoo Finance via RapidAPI.
    """
    log_message("Quoting market data from external API...")
    api_host = os.getenv("RAPIDAPI_HOST")
    api_market_get_quotes = os.getenv("RAPIDAPI_MARKET_GET_QUOTES")
    api_key = os.getenv("RAPIDAPI_KEY")

    if not api_host or not api_key or not api_market_get_quotes:
        log_message("API_HOST or API_KEY is not set. Check your .env file.")
        raise ValueError("API_HOST or API_KEY is not set. Check your .env file.")

    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": api_host
    }
    params = {
        "symbols": ",".join([quote(symbol) for symbol in symbols]),
        "region": region
    }

    api_url = f"https://{api_host}/{api_market_get_quotes}"

    try:
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        # Validate response structure
        if "quoteResponse" not in data or "result" not in data["quoteResponse"]:
            raise ValueError("Invalid API response format")

        log_message(f"Successfully fetched market data for {len(data['quoteResponse']['result'])} symbols.")
        return data["quoteResponse"]["result"]

    except requests.exceptions.RequestException as e:
        log_message(f"Error quoting market data: {e}")
        raise

def get_historical_stock_data(symbol, start_date=None, end_date=None):
    """
    Fetch historical stock data for the given symbol and date range using Alpha Vantage API.
    """
    log_message(f"Fetching historical stock data for symbol: {symbol}, start_date: {start_date}, end_date: {end_date}...")
    api_host = os.getenv("ALPHA_VANTAGE_API_HOST")
    api_query = os.getenv("ALPHA_VANTAGE_API_QUERY")
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

    if not api_host or not api_query or not api_key:
        raise ValueError("ALPHA_VANTAGE_API_HOST, ALPHA_VANTAGE_API_QUERY, or ALPHA_VANTAGE_API_KEY is not set. Check your .env file.")

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": api_key
    }

    api_url = f"https://{api_host}{api_query}"

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Validate response structure
        if "Time Series (Daily)" not in data:
            raise ValueError("Invalid API response format for stock data.")

        # Filter data by date range
        time_series = data["Time Series (Daily)"]
        filtered_data = {
            date: values
            for date, values in time_series.items()
            if (not start_date or date >= start_date) and (not end_date or date <= end_date)
        }

        log_message(f"Successfully fetched historical stock data for symbol: {symbol}.")
        return filtered_data

    except requests.exceptions.RequestException as e:
        log_message(f"Error fetching historical stock data for symbol {symbol}: {e}")
        raise


def get_historical_crypto_data(symbol, market="USD", start_date=None, end_date=None):
    """
    Fetch historical cryptocurrency data for the given symbol, market, and date range using Alpha Vantage API.
    """
    log_message(f"Fetching historical cryptocurrency data for symbol: {symbol}, market: {market}, start_date: {start_date}, end_date: {end_date}...")
    api_host = os.getenv("ALPHA_VANTAGE_API_HOST")
    api_query = os.getenv("ALPHA_VANTAGE_API_QUERY")
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

    if not api_host or not api_query or not api_key:
        raise ValueError("ALPHA_VANTAGE_API_HOST, ALPHA_VANTAGE_API_QUERY, or ALPHA_VANTAGE_API_KEY is not set. Check your .env file.")

    params = {
        "function": "CRYPTO_DAILY",
        "symbol": symbol,
        "market": market,
        "apikey": api_key
    }

    api_url = f"https://{api_host}{api_query}"

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Validate response structure
        if "Time Series (Digital Currency Daily)" not in data:
            raise ValueError("Invalid API response format for cryptocurrency data.")

        # Filter data by date range
        time_series = data["Time Series (Digital Currency Daily)"]
        filtered_data = {
            date: values
            for date, values in time_series.items()
            if (not start_date or date >= start_date) and (not end_date or date <= end_date)
        }

        log_message(f"Successfully fetched historical cryptocurrency data for symbol: {symbol}.")
        return filtered_data

    except requests.exceptions.RequestException as e:
        log_message(f"Error fetching historical cryptocurrency data for symbol {symbol}: {e}")
        raise


def get_historical_fx_data(from_symbol, to_symbol, start_date=None, end_date=None):
    """
    Fetch historical forex data for the given currency pair and date range using Alpha Vantage API.
    """
    log_message(f"Fetching historical forex data for pair: {from_symbol}/{to_symbol}, start_date: {start_date}, end_date: {end_date}...")
    api_host = os.getenv("ALPHA_VANTAGE_API_HOST")
    api_query = os.getenv("ALPHA_VANTAGE_API_QUERY")
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

    if not api_host or not api_query or not api_key:
        raise ValueError("ALPHA_VANTAGE_API_HOST, ALPHA_VANTAGE_API_QUERY, or ALPHA_VANTAGE_API_KEY is not set. Check your .env file.")

    params = {
        "function": "FX_DAILY",
        "from_symbol": from_symbol,
        "to_symbol": to_symbol,
        "apikey": api_key
    }

    api_url = f"https://{api_host}{api_query}"

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Validate response structure
        if "Time Series FX (Daily)" not in data:
            raise ValueError("Invalid API response format for forex data.")

        # Filter data by date range
        time_series = data["Time Series FX (Daily)"]
        filtered_data = {
            date: values
            for date, values in time_series.items()
            if (not start_date or date >= start_date) and (not end_date or date <= end_date)
        }

        log_message(f"Successfully fetched historical forex data for pair: {from_symbol}/{to_symbol}.")
        return filtered_data

    except requests.exceptions.RequestException as e:
        log_message(f"Error fetching historical forex data for pair {from_symbol}/{to_symbol}: {e}")
        raise

def get_closest_us_market_closing_time():
    """
    Calculate the most recent US market closing time (4:00 PM EDT).
    """
    eastern = pytz.timezone("US/Eastern")
    utc = pytz.utc
    now = datetime.now(eastern)

    if now.hour < 16:  # Before today's market closing time
        # Use yesterday's closing time
        closest_closing_time = (now - timedelta(days=1)).replace(hour=16, minute=0, second=0, microsecond=0)
    else:  # After today's market closing time
        # Use today's closing time
        closest_closing_time = now.replace(hour=16, minute=0, second=0, microsecond=0)

    return closest_closing_time.astimezone(utc)