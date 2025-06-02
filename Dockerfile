# Stage 1: Build dependencies
FROM python:3.13.3-slim-bullseye as builder

WORKDIR /app

# Install system dependencies for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Stage 2: Runtime image
FROM python:3.13.3-slim-bullseye

WORKDIR /app

# Install runtime system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    supervisor \
    netcat-openbsd \
    postgresql-client \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# Copy project files
COPY . /app

# Set up supervisor configuration
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Set up logging configuration
COPY logging.conf /app/logging.conf

# Create log directory and set permissions
RUN mkdir -p /var/log && \
    # Convert line endings and set permissions
    dos2unix /app/entrypoint.sh && \
    chmod 755 /app/entrypoint.sh && \
    chown root:root /app/entrypoint.sh && \
    # Set ownership for app directory
    chown -R root:root /app && \
    chown -R root:root /var/log

ENTRYPOINT ["/app/entrypoint.sh"]