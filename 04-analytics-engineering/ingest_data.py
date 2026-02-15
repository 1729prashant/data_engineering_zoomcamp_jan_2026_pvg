"""
Load Parquet data into DuckDB.

This script runs INSIDE the Docker container on first startup.
It assumes Parquet files already exist in the data/ directory.
"""

import duckdb
from pathlib import Path
import sys

def load_data_to_duckdb():
    """Load parquet files into DuckDB tables."""
    print("\n📊 Loading data into DuckDB...")
    
    # Connect to DuckDB
    db_path = "/app/taxi_rides_ny/taxi_rides_ny.duckdb"
    con = duckdb.connect(db_path)
    
    # Create schema
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")
    print("✓ Created prod schema")

    # Load data for each taxi type
    for taxi_type in ["yellow", "green", "fhv"]:
        parquet_path = f"/app/taxi_rides_ny/data/{taxi_type}/*.parquet"
        
        # Check if parquet files exist
        data_dir = Path(f"/app/taxi_rides_ny/data/{taxi_type}")
        if not data_dir.exists() or not list(data_dir.glob("*.parquet")):
            print(f"⚠️  No parquet files found for {taxi_type} taxi")
            continue
            
        print(f"📥 Loading {taxi_type} taxi data from {parquet_path}...")
        
        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE prod.{taxi_type}_tripdata AS
                SELECT * FROM read_parquet('{parquet_path}', union_by_name=true)
            """)
            
            # Get row count
            result = con.execute(f"SELECT COUNT(*) FROM prod.{taxi_type}_tripdata").fetchone()
            print(f"✅ Loaded {result[0]:,} rows into prod.{taxi_type}_tripdata")
        except Exception as e:
            print(f"❌ Error loading {taxi_type} data: {e}")
            con.close()
            sys.exit(1)

    con.close()
    print("\n🎉 Data loading complete!")
    print(f"DuckDB database: {db_path}")

if __name__ == "__main__":
    load_data_to_duckdb()