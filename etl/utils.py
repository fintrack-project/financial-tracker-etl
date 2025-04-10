import psycopg2
import logging
from dotenv import load_dotenv
import os

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