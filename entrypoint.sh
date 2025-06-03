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

# Wait for database to be ready
log "Waiting for database to be ready..."
until PGPASSWORD=$DATABASE_PASSWORD psql -h "$DATABASE_HOST" -p "$DATABASE_PORT" -U "$DATABASE_USER" -d "$DATABASE_NAME" -c '\q'; do
    log "Database is unavailable - sleeping"
    sleep 1
done
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