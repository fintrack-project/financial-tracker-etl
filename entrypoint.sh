#!/bin/bash
set -e

# Create log directory if it doesn't exist
mkdir -p /var/log

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a /var/log/entrypoint.log
}

# Check if required environment variables are set
log "Checking environment variables..."
required_vars=(
    "KAFKA_BROKER"
    "DATABASE_HOST"
    "DATABASE_PORT"
    "DATABASE_NAME"
    "DATABASE_USER"
    "DATABASE_PASSWORD"
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        log "ERROR: Required environment variable $var is not set"
        exit 1
    fi
done

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