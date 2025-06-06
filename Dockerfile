# Stage 1: Build dependencies
FROM python:3.13.4-slim-bookworm as builder

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
FROM python:3.13.4-slim-bookworm

WORKDIR /app

# Install runtime system dependencies with specific versions to fix CVEs
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron=3.0pl1-162 \
    supervisor=4.2.5-1 \
    netcat-openbsd=1.219-1 \
    postgresql-client=15+248 \
    dos2unix=7.4.3-1 \
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