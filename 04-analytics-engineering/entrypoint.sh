#!/bin/bash
set -e

# Marker files to track what's been loaded
DATA_MARKER="/app/taxi_rides_ny/.data_loaded"
SEED_MARKER="/app/taxi_rides_ny/.seeds_loaded"

# Check if data needs to be loaded
if [ ! -f "$DATA_MARKER" ]; then
    echo "🚀 First run detected - loading data into DuckDB..."
    
    # Check if data directory exists and has parquet files
    if [ -d "/app/taxi_rides_ny/data/yellow" ] && [ -d "/app/taxi_rides_ny/data/green" ]; then
        echo "📊 Found local data files, loading into DuckDB..."
        pwd
        ls -latr
        python /app/ingest_data.py
        
        # Create marker file to prevent re-loading on subsequent runs
        touch "$DATA_MARKER"
        echo "✅ Data loaded successfully!"
    else
        echo "⚠️  Warning: No data found in /app/taxi_rides_ny/data/"
        echo "Please run download_data.py on your local machine first."
    fi
else
    echo "✓ Data already loaded (skipping initial load)"
fi

# Check if seeds need to be loaded
if [ ! -f "$SEED_MARKER" ]; then
    # First run - load seeds
    if [ -d "seeds" ] && [ "$(ls -A seeds/*.csv 2>/dev/null)" ]; then
        echo "🌱 Loading seed data..."
        dbt seed
        touch "$SEED_MARKER"
        echo "✅ Seeds loaded successfully!"
    fi
else
    echo "✓ Seeds already loaded (skipping)"
fi

# Execute the command passed to docker-compose run
exec "$@"