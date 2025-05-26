#!/bin/bash
set -e

# Start supervisor (which will manage both cron and Kafka consumer)
exec supervisord -n -c /etc/supervisor/conf.d/supervisord.conf 