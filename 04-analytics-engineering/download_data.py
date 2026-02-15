"""
Download and convert NYC taxi data to Parquet format.

This script runs on your LOCAL MACHINE (not in Docker).
It downloads CSV files and converts them to Parquet for efficient storage.

Usage:
    python download_data.py
"""

import duckdb
import requests
from pathlib import Path

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

def download_and_convert_files(taxi_type):
    """Download CSV.gz files and convert them to Parquet format."""
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in [2019, 2020]:
        for month in range(1, 13):
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            if parquet_filepath.exists():
                print(f"✓ Skipping {parquet_filename} (already exists)")
                continue

            # Download CSV.gz file

            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            
            csv_gz_filepath = data_dir / csv_gz_filename
            print(f"⬇ Downloading {csv_gz_filename}...")

            try:

                response = requests.get(f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", stream=True)

                response.raise_for_status()

                with open(csv_gz_filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                print(f"🔄 Converting {csv_gz_filename} to Parquet...")
                con = duckdb.connect()
                con.execute(f"""
                    COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
                    TO '{parquet_filepath}' (FORMAT PARQUET)
                """)
                con.close()

                # Remove the CSV.gz file to save space
                csv_gz_filepath.unlink()
                print(f"✅ Completed {parquet_filename}")
            
            except Exception as e:
                print(f"❌ Error processing {csv_gz_filename}: {e}")
                if csv_gz_filepath.exists():
                    csv_gz_filepath.unlink()
                continue


def update_gitignore():
    """Add data directory to .gitignore."""
    gitignore_path = Path(".gitignore")

    # Read existing content or start with empty string
    content = gitignore_path.read_text() if gitignore_path.exists() else ""

    # Add data/ if not already present
    if 'data/' not in content:
        with open(gitignore_path, 'a') as f:
            f.write('\n# Data directory\ndata/\n' if content else '# Data directory\ndata/\n')
        print("✓ Updated .gitignore")

if __name__ == "__main__":
    print("🚀 NYC Taxi Data Download & Conversion (Local)")
    print("=" * 50)
    print("This will download data to your LOCAL machine")
    print("Docker will load it into DuckDB on first run")
    print("=" * 50)
    print()
    
    # Update .gitignore to exclude data directory
    update_gitignore()

    # Download and convert files
    for taxi_type in ["yellow", "green", "fhv"]:
        print(f"\n📦 Processing {taxi_type.upper()} taxi data...")
        download_and_convert_files(taxi_type)

    print("\n" + "=" * 50)
    print("🎉 Download and conversion complete!")
    print("=" * 50)
    print("\nNext steps:")
    print("1. Build Docker image: docker-compose build")
    print("2. Run container: docker-compose run --rm dbt bash")
    print("   (Data will be loaded into DuckDB automatically on first run)")