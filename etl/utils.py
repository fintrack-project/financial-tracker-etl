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
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
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