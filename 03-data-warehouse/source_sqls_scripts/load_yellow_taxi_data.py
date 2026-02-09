# Import required libraries for file operations, HTTP requests, threading, and Google Cloud Storage
import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

# Configuration: Get the Google Cloud Storage bucket name from environment variable
# This value is set by sourcing env.sh which fetches the bucket name from gsutil
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
if not BUCKET_NAME:
    print("Error: GCP_BUCKET_NAME environment variable not set.")
    print("Please run: source env.sh")
    sys.exit(1)

# Authentication: Load GCS credentials from a service account JSON file
# The credentials file path is retrieved from the GCP_CREDENTIALS environment variable
# This value is set by sourcing env.sh
CREDENTIALS_FILE = os.getenv("GCP_CREDENTIALS")
if not CREDENTIALS_FILE:
    print("Error: GCP_CREDENTIALS environment variable not set.")
    print("Please run: source env.sh")
    sys.exit(1)

# Verify that the credentials file exists
if not os.path.exists(CREDENTIALS_FILE):
    print(f"Error: Credentials file not found at: {CREDENTIALS_FILE}")
    print("Please update the GCP_CREDENTIALS path in env.sh")
    sys.exit(1)


client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
# If commented initialize client with the following
# client = storage.Client(project='zoomcamp-mod3-datawarehouse')

# Data source configuration: Base URL for NYC taxi trip data and months to download (Jan-Jun 2024)
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]

# Local storage configuration: Directory for temporary file downloads
DOWNLOAD_DIR = "."

# Upload configuration: Set chunk size to 8MB for streaming uploads to GCS
CHUNK_SIZE = 8 * 1024 * 1024

# Ensure the download directory exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Initialize the GCS bucket object
bucket = client.bucket(BUCKET_NAME)

# Function to download a single parquet file for a given month
def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")
    
    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

# Function to create a GCS bucket or verify it exists and is accessible
def create_bucket(bucket_name):
    try:
        # Get bucket details
        bucket = client.get_bucket(bucket_name)
        
        # Check if the bucket belongs to the current project
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        
        if bucket_name in project_bucket_ids:
            print(
                f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding..."
            )
        else:
            print(
                f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
            )
            sys.exit(1)
    
    except NotFound:
        # If the bucket doesn't exist, create it
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    
    except Forbidden:
        # If the request is forbidden, it means the bucket exists but you don't have access to see details
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name."
        )
        sys.exit(1)

# Function to verify that a file was successfully uploaded to GCS
def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

# Function to upload a local file to GCS with retry logic and verification
def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE
    
    # Ensure bucket exists before uploading
    create_bucket(BUCKET_NAME)
    
    # Retry loop for upload attempts
    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            
            # Verify the upload was successful
            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")
        
        # Wait before retrying
        time.sleep(5)
    
    # All retry attempts exhausted
    print(f"Giving up on {file_path} after {max_retries} attempts.")

# Main execution block: orchestrate downloading and uploading files in parallel
if __name__ == "__main__":
    # Ensure the GCS bucket exists before starting
    create_bucket(BUCKET_NAME)
    
    # Download all files in parallel using 4 worker threads
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))
    
    # Upload all successfully downloaded files in parallel using 4 worker threads
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values
    
    print("All files processed and verified.")