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
    """
    Log a message to the etl.log file.
    """
    logging.basicConfig(
        filename="logs/etl.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.info(message)

import os

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

def quote_market_data(symbols):
    """
    Quote market data for the given symbols from Yahoo Finance via RapidAPI.
    """
    log_message("Quoting market data from external API...")
    api_url = os.getenv("RAPIDAPI_URL")
    api_key = os.getenv("RAPIDAPI_KEY")

    if not api_url or not api_key:
        raise ValueError("API_URL or API_KEY is not set. Check your .env file.")

    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "apidojo-yahoo-finance-v1.p.rapidapi.com"
    }
    params = {"symbols": ",".join([quote(symbol) for symbol in symbols])}

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