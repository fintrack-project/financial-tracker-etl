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

# Add the cron job
RUN echo "0 0 1 * * python3 /app/etl/jobs/produce_holdings_monthly_request.py >> /var/log/cron.log 2>&1" > /etc/cron.d/etl-cron

# Give execution rights to the cron job
RUN chmod 0644 /etc/cron.d/etl-cron

# Apply the cron job
RUN crontab /etc/cron.d/etl-cron

# Create a log file for cron
RUN touch /var/log/cron.log

# Copy the supervisord configuration file
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Start supervisord to manage both cron and Kafka consumer
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]