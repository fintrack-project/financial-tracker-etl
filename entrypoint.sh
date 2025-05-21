#!/bin/bash
set -e

# Start cron
service cron start

# Start supervisor (if needed for other processes)
supervisord -c /etc/supervisor/conf.d/supervisord.conf &

# Start Kafka consumer (main ETL process)
python3 -m etl.main consume 