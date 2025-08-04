import psycopg
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
            "options": f"-c search_path={os.getenv('DATABASE_SCHEMA_NAME', 'financial_tracker')}"
        }
    log_message(f"Attempting to connect to database with config: {db_config}")
    try:
        conn = psycopg.connect(**db_config)
        log_message("Successfully connected to database")
        return conn
    except Exception as e:
        log_message(f"Error connecting to database: {str(e)}")
        raise

def log_message(message):
    """
    Log a message to both file and stdout.
    """
    # Use local log file for tests, system log file for production
    if os.environ.get('TESTING') or os.environ.get('CI'):
        log_file = "etl_test.log"
    else:
        log_file = "/var/log/etl.log"

    # Configure logging to both file and stdout
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
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

def quote_market_index_data(symbols, region="US"):
    """
    Quote market data for the given symbols from Yahoo Finance via RapidAPI.
    """
    log_message("Quoting market data from external API...")
    api_host = os.getenv("RAPIDAPI_HOST")
    api_market_get_quotes = os.getenv("RAPIDAPI_MARKET_GET_QUOTES")
    api_key = os.getenv("RAPIDAPI_KEY")

    # Check each environment variable individually
    missing_vars = []
    if not api_host:
        missing_vars.append("RAPIDAPI_HOST")
    if not api_market_get_quotes:
        missing_vars.append("RAPIDAPI_MARKET_GET_QUOTES")
    if not api_key:
        missing_vars.append("RAPIDAPI_KEY")

    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}. Please check your .env file."
        log_message(error_msg)
        raise ValueError(error_msg)

    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": api_host
    }
    params = {
        "symbols": ",".join([quote(symbol) for symbol in symbols]),
        "region": region
    }

    api_url = f"https://{api_host}/{api_market_get_quotes}"
    log_message(f"Making API request to: {api_url}")
    log_message(f"Request parameters: {params}")

    try:
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        # Validate response structure
        if "quoteResponse" not in data:
            error_msg = "Invalid API response format: missing 'quoteResponse' field"
            log_message(error_msg)
            raise ValueError(error_msg)
        
        if "result" not in data["quoteResponse"]:
            error_msg = "Invalid API response format: missing 'result' field in 'quoteResponse'"
            log_message(error_msg)
            raise ValueError(error_msg)

        result = data["quoteResponse"]["result"]
        log_message(f"Successfully fetched market data for {len(result)} symbols.")
        log_message(f"API response: {result}")
        return result

    except requests.exceptions.RequestException as e:
        error_msg = f"Error quoting market data: {str(e)}"
        log_message(error_msg)
        raise ValueError(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error while quoting market data: {str(e)}"
        log_message(error_msg)
        raise ValueError(error_msg)

def get_realtime_stock_data(symbol):
    """
    Fetch real-time stock data for the given symbol using Twelve Data API.
    """
    log_message(f"Fetching real-time stock data for symbol: {symbol}...")
    api_host = os.getenv("TWELVE_DATA_API_HOST")
    api_quote = os.getenv("TWELVE_DATA_API_QUOTE")
    api_key = os.getenv("TWELVE_DATA_API_KEY")

    if not api_host or not api_quote or not api_key:
        raise ValueError("TWELVE_DATA_API_HOST, TWELVE_DATA_API_QUOTE, or TWELVE_DATA_API_KEY is not set. Check your .env file.")

    params = {
        "symbol": symbol,
        "apikey": api_key,
        "format": "JSON"
    }

    api_url = f"https://{api_host}{api_quote}"

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Validate response structure
        if "close" not in data:
            raise ValueError(f"Invalid API response format, there is not \"close\" for stock data: {data}")

        log_message(f"Successfully fetched real-time stock data for symbol: {symbol}.")
        return data

    except requests.exceptions.RequestException as e:
        log_message(f"Error fetching real-time stock data for symbol {symbol}: {e}")
        raise

def get_realtime_crypto_data(symbol, market="USD"):
    """
    Fetch real-time cryptocurrency data for the given symbol and market using Twelve Data API.
    """
    log_message(f"Fetching real-time cryptocurrency data for symbol: {symbol}, market: {market}...")
    api_host = os.getenv("TWELVE_DATA_API_HOST")
    api_quote = os.getenv("TWELVE_DATA_API_QUOTE")
    api_key = os.getenv("TWELVE_DATA_API_KEY")

    if not api_host or not api_quote or not api_key:
        raise ValueError("TWELVE_DATA_API_HOST, TWELVE_DATA_API_QUOTE, or TWELVE_DATA_API_KEY is not set. Check your .env file.")

    params = {
        "symbol": f"{symbol}/{market}",
        "apikey": api_key,
        "format": "JSON"
    }

    api_url = f"https://{api_host}{api_quote}"

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Validate response structure
        if "close" not in data:
            raise ValueError(f"Invalid API response format, there is not \"close\" for cryptocurrency data: {data}")

        log_message(f"Successfully fetched real-time cryptocurrency data for symbol: {symbol}.")
        return data

    except requests.exceptions.RequestException as e:
        log_message(f"Error fetching real-time cryptocurrency data for symbol {symbol}: {e}")
        raise

def get_realtime_forex_data(from_symbol, to_symbol):
    """
    Fetch real-time forex data for the given currency pair using Twelve Data API.
    """
    log_message(f"Fetching real-time forex data for pair: {from_symbol}/{to_symbol}...")
    api_host = os.getenv("TWELVE_DATA_API_HOST")
    api_quote = os.getenv("TWELVE_DATA_API_QUOTE")
    api_key = os.getenv("TWELVE_DATA_API_KEY")

    if not api_host or not api_quote or not api_key:
        raise ValueError("TWELVE_DATA_API_HOST, TWELVE_DATA_API_QUOTE, or TWELVE_DATA_API_KEY is not set. Check your .env file.")

    params = {
        "symbol": f"{from_symbol}/{to_symbol}",
        "apikey": api_key,
        "format": "JSON"
    }

    api_url = f"https://{api_host}{api_quote}"

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Validate response structure
        if "close" not in data:
            raise ValueError(f"Invalid API response format, there is not \"close\" for forex data: {data}")

        log_message(f"Successfully fetched real-time forex data for pair: {from_symbol}/{to_symbol}.")
        return data

    except requests.exceptions.RequestException as e:
        log_message(f"Error fetching real-time forex data for pair {from_symbol}/{to_symbol}: {e}")
        raise

def get_historical_stock_data(symbol, start_date=None, end_date=None):
    """
    Fetch historical stock data for the given symbol and date range using Twelve Data API.
    """
    log_message(f"Fetching historical stock data for symbol: {symbol}, start_date: {start_date}, end_date: {end_date}...")
    api_host = os.getenv("TWELVE_DATA_API_HOST")
    api_timeseries = os.getenv("TWELVE_DATA_API_TIMESERIES")
    api_key = os.getenv("TWELVE_DATA_API_KEY")

    if not api_host or not api_timeseries or not api_key:
        raise ValueError("TWELVE_DATA_API_HOST, TWELVE_DATA_API_TIMESERIES, or TWELVE_DATA_API_KEY is not set. Check your .env file.")

    params = {
        "symbol": symbol,
        "interval": "1month",  # Monthly data
        "start_date": start_date,
        "end_date": end_date,
        "apikey": api_key,
        "format": "JSON"
    }

    api_url = f"https://{api_host}{api_timeseries}"

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Validate response structure
        if "values" not in data:
            raise ValueError(f"Invalid API response format for stock data: {data}")

        log_message(f"Successfully fetched historical stock data for symbol: {symbol}.")
        return data["values"]

    except requests.exceptions.RequestException as e:
        log_message(f"Error fetching historical stock data for symbol {symbol}: {e}")
        raise


def get_historical_crypto_data(symbol, market="USD", start_date=None, end_date=None):
    """
    Fetch historical cryptocurrency data for the given symbol, market, and date range using Twelve Data API.
    """
    log_message(f"Fetching historical cryptocurrency data for symbol: {symbol}, market: {market}, start_date: {start_date}, end_date: {end_date}...")
    api_host = os.getenv("TWELVE_DATA_API_HOST")
    api_timeseries = os.getenv("TWELVE_DATA_API_TIMESERIES")
    api_key = os.getenv("TWELVE_DATA_API_KEY")

    if not api_host or not api_timeseries or not api_key:
        raise ValueError("TWELVE_DATA_API_HOST, TWELVE_DATA_API_TIMESERIES, or TWELVE_DATA_API_KEY is not set. Check your .env file.")

    params = {
        "symbol": f"{symbol}/{market}",
        "interval": "1month",  # Monthly data
        "start_date": start_date,
        "end_date": end_date,
        "apikey": api_key,
        "format": "JSON"
    }

    api_url = f"https://{api_host}{api_timeseries}"

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Validate response structure
        if "values" not in data:
            raise ValueError(f"Invalid API response format for cryptocurrency data: {data}")

        log_message(f"Successfully fetched historical cryptocurrency data for symbol: {symbol}.")
        return data["values"]

    except requests.exceptions.RequestException as e:
        log_message(f"Error fetching historical cryptocurrency data for symbol {symbol}: {e}")
        raise


def get_historical_fx_data(from_symbol, to_symbol, start_date=None, end_date=None):
    """
    Fetch historical forex data for the given currency pair and date range using Twelve Data API.
    """
    log_message(f"Fetching historical forex data for pair: {from_symbol}/{to_symbol}, start_date: {start_date}, end_date: {end_date}...")
    api_host = os.getenv("TWELVE_DATA_API_HOST")
    api_timeseries = os.getenv("TWELVE_DATA_API_TIMESERIES")
    api_key = os.getenv("TWELVE_DATA_API_KEY")

    if not api_host or not api_timeseries or not api_key:
        raise ValueError("TWELVE_DATA_API_HOST, TWELVE_DATA_API_TIMESERIES, or TWELVE_DATA_API_KEY is not set. Check your .env file.")

    params = {
        "symbol": f"{from_symbol}/{to_symbol}",
        "interval": "1month",  # Monthly data
        "start_date": start_date,
        "end_date": end_date,
        "apikey": api_key,
        "format": "JSON"
    }

    api_url = f"https://{api_host}{api_timeseries}"

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Validate response structure
        if "values" not in data:
            raise ValueError(f"Invalid API response format for forex data: {data}")

        log_message(f"Successfully fetched historical forex data for pair: {from_symbol}/{to_symbol}.")
        return data["values"]

    except requests.exceptions.RequestException as e:
        log_message(f"Error fetching historical forex data for pair {from_symbol}/{to_symbol}: {e}")
        raise