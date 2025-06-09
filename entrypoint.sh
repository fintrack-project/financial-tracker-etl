#!/bin/bash
set -e

# Create log directory if it doesn't exist
mkdir -p /var/log

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a /var/log/entrypoint.log
}

echo "[$(date)] Checking environment variables..."

# Check if required environment variables are set
if [ -z "$KAFKA_BROKER" ]; then
    echo "Error: KAFKA_BROKER environment variable is not set"
    exit 1
fi

if [ -z "$DATABASE_HOST" ]; then
    echo "Error: DATABASE_HOST environment variable is not set"
    exit 1
fi

if [ -z "$DATABASE_PORT" ]; then
    echo "Error: DATABASE_PORT environment variable is not set"
    exit 1
fi

if [ -z "$DATABASE_NAME" ]; then
    echo "Error: DATABASE_NAME environment variable is not set"
    exit 1
fi

if [ -z "$DATABASE_USER" ]; then
    echo "Error: DATABASE_USER environment variable is not set"
    exit 1
fi

if [ -z "$DATABASE_PASSWORD" ]; then
    echo "Error: DATABASE_PASSWORD environment variable is not set"
    exit 1
fi

# Wait for database to be ready using Python
log "Waiting for database to be ready..."
python3 << EOF
import os
import time
import psycopg
from psycopg import OperationalError

def wait_for_db():
    while True:
        try:
            conn = psycopg.connect(
                host=os.getenv('DATABASE_HOST'),
                port=os.getenv('DATABASE_PORT'),
                dbname=os.getenv('DATABASE_NAME'),
                user=os.getenv('DATABASE_USER'),
                password=os.getenv('DATABASE_PASSWORD')
            )
            conn.close()
            print("Database is ready")
            break
        except OperationalError:
            print("Database is unavailable - sleeping")
            time.sleep(1)

wait_for_db()
EOF
log "Database is ready"

# Wait for Kafka to be ready
log "Waiting for Kafka to be ready..."
until nc -z $(echo $KAFKA_BROKER | cut -d: -f1) $(echo $KAFKA_BROKER | cut -d: -f2); do
    log "Kafka is unavailable - sleeping"
    sleep 1
done
log "Kafka is ready"

# Start supervisor
log "Starting supervisor..."
exec /usr/bin/supervisord -n -c /etc/supervisor/conf.d/supervisord.conf 