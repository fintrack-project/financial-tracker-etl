FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    cron \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project directory into the container
COPY . /app

# Copy crontab file
COPY crontab /etc/cron.d/etl-cron
RUN chmod 0644 /etc/cron.d/etl-cron

# Create log directory
RUN mkdir -p /var/log

# Copy supervisord configuration
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Create an entrypoint script
RUN echo '#!/bin/bash\n\
# Start cron\n\
service cron start\n\
\n\
# Start Kafka consumer\n\
python3 -m etl.main consume\n\
' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Set the entrypoint script as the default command
ENTRYPOINT ["/app/entrypoint.sh"]