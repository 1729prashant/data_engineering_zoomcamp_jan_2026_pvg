# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    dbt-duckdb \
    duckdb \
    requests==2.31.0

# Install DuckDB CLI
RUN wget https://github.com/duckdb/duckdb/releases/download/v1.4.4/duckdb_cli-linux-arm64.zip \
    && unzip duckdb_cli-linux-arm64.zip -d /usr/local/bin \
    && rm duckdb_cli-linux-arm64.zip \
    && chmod +x /usr/local/bin/duckdb

    
# Create dbt profile directory
RUN mkdir -p /root/.dbt

# Copy entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Set environment variables
ENV DBT_PROFILES_DIR=/root/.dbt

# Default entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["bash"]