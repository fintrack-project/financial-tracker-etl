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

def quote_market_index_data(symbols, region="US"):
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
        if "price" not in data:
            raise ValueError(f"Invalid API response format for stock data: {data}")

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
        if "price" not in data:
            raise ValueError(f"Invalid API response format for cryptocurrency data: {data}")

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
        if "price" not in data:
            raise ValueError(f"Invalid API response format for forex data: {data}")

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

def get_closest_us_market_closing_time(reference_date=None):
    """
    Calculate the most recent US market closing time (4:00 PM EDT) relative to a given date.
    If no date is provided, use the current time.
    
    Args:
        reference_date (datetime, optional): The reference date to calculate the closest market closing time. Defaults to the current time in US Eastern timezone.
    
    Returns:
        datetime: The closest US market closing time in UTC.
    """
    eastern = pytz.timezone("US/Eastern")
    utc = pytz.utc

    # Use the provided reference date or default to the current time
    if reference_date is None:
        reference_date = datetime.now(eastern)
    else:
        # Ensure the reference_date is in the US Eastern timezone
        reference_date = reference_date.astimezone(eastern)

    if reference_date.hour < 16:  # Before today's market closing time
        # Use yesterday's closing time
        closest_closing_time = (reference_date - timedelta(days=1)).replace(hour=16, minute=0, second=0, microsecond=0)
    else:  # After today's market closing time
        # Use today's closing time
        closest_closing_time = reference_date.replace(hour=16, minute=0, second=0, microsecond=0)

    return closest_closing_time.astimezone(utc)