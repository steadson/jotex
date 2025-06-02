import requests
import os
import sys
from pathlib import Path
import pandas as pd
import json
from urllib.parse import urlencode, parse_qs, urlparse
from io import BytesIO
from dotenv import load_dotenv
import time
import hashlib
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.access_auth import get_ms_access_token
from modules.logger import setup_logging

load_dotenv()

setup_logging('download_excel')

# Directory for storing downloaded files and cache
DOWNLOAD_DIR = os.path.join(os.getcwd(), "data/downloads")
CACHE_DIR = os.path.join(os.getcwd(), "data/cache")

# Ensure directories exist
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

def get_file_metadata(access_token, drive_id, item_id):
    """
    Get file metadata to check if it has been modified.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to get file metadata: {response.status_code} - {response.text}")
    
    metadata = response.json()
    return metadata

def get_cached_file_info(file_name):
    """
    Get information about previously cached file.
    """
    cache_info_path = os.path.join(CACHE_DIR, f"{file_name}.info.json")
    
    if not os.path.exists(cache_info_path):
        return None
    
    with open(cache_info_path, 'r') as f:
        return json.load(f)

def save_cached_file_info(file_name, metadata):
    """
    Save cache information for a file.
    """
    cache_info = {
        "last_modified": metadata.get("lastModifiedDateTime", ""),
        "etag": metadata.get("eTag", ""),
        "size": metadata.get("size", 0),
        "timestamp": time.time()
    }
    
    cache_info_path = os.path.join(CACHE_DIR, f"{file_name}.info.json")
    
    with open(cache_info_path, 'w') as f:
        json.dump(cache_info, f)
    
    return cache_info

def identify_new_rows(new_df, file_name):
    """
    Compare new data with previously cached data to identify new rows.
    Return only the new rows and update the cached data.
    """
    cached_data_path = os.path.join(CACHE_DIR, f"{file_name}.cached.csv")
    
    # If no cached data exists, all rows are new
    if not os.path.exists(cached_data_path):
        logging.warning(f"No cached data found for {file_name}. All rows are considered new.")
        # Save current data as cache
        new_df.to_csv(cached_data_path, index=False)
        return new_df, len(new_df)
    
    # Load cached data
    cached_df = pd.read_csv(cached_data_path)
    
    # Try to determine a unique identifier or combination of columns
    # This is a simplified approach - you might need to adjust based on your Excel structure
    # First, try to find primary key columns that might exist in your data
    potential_key_columns = ['ID', 'Id', 'id', 'UUID', 'Key', 'TransactionId', 'RecordID']
    key_columns = [col for col in potential_key_columns if col in new_df.columns]
    
    if not key_columns:
        # If no obvious ID column exists, use all columns to identify duplicates
        # Exclude time-based columns which might change between exports
        exclude_cols = [col for col in new_df.columns if any(
            time_word in col.lower() for time_word in ['time', 'date', 'updated', 'created', 'timestamp']
        )]
        key_columns = [col for col in new_df.columns if col not in exclude_cols]
    
    # If we have too many columns, try to generate a hash
    if len(key_columns) > 5:
        logging.info("Many columns found - creating hash of row values to identify unique rows")
        # Create hash for each row in both dataframes based on string values
        new_df['row_hash'] = new_df.astype(str).apply(
            lambda row: hashlib.md5(''.join(row).encode()).hexdigest(), axis=1
        )
        cached_df['row_hash'] = cached_df.astype(str).apply(
            lambda row: hashlib.md5(''.join(row).encode()).hexdigest(), axis=1
        )
        
        # Find new rows by comparing hashes
        new_rows_df = new_df[~new_df['row_hash'].isin(cached_df['row_hash'])]
        new_rows_count = len(new_rows_df)
        
        # Drop the temporary hash column
        new_df = new_df.drop('row_hash', axis=1)
        new_rows_df = new_rows_df.drop('row_hash', axis=1)
    else:
        # Use the identified key columns to find new rows
        logging.info(f"Using columns {key_columns} to identify unique rows")
        
        # If dataframes are empty or have no matching columns, handle the edge case
        if len(key_columns) == 0 or cached_df.empty or new_df.empty:
            # In this case, consider everything as new (worst case)
            new_rows_df = new_df
            new_rows_count = len(new_df)
        else:
            # Generate a multi-index merge key for each dataframe to compare
            new_df_keys = new_df[key_columns].astype(str).agg('-'.join, axis=1)
            cached_df_keys = cached_df[key_columns].astype(str).agg('-'.join, axis=1)
            
            # Find rows in new_df that don't exist in cached_df
            new_rows_df = new_df[~new_df_keys.isin(cached_df_keys)]
            new_rows_count = len(new_rows_df)
    
    # Update cache with new data
    new_df.to_csv(cached_data_path, index=False)
    
    return new_rows_df, new_rows_count

def download_specific_file(access_token, drive_id, item_id, file_name, sheet_name=None):
    """
    Download a specific file using its drive ID and item ID.
    Compare with cached data to only return new rows.
    
    Parameters:
    - access_token: Microsoft Graph API access token
    - drive_id: OneDrive/SharePoint drive ID
    - item_id: File item ID
    - file_name: Name to save the file as
    - sheet_name: Specific Excel sheet to read (optional)
    """
    # Get file metadata to check if modified
    metadata = get_file_metadata(access_token, drive_id, item_id)
    
    # Add sheet_name to metadata if provided
    if sheet_name:
        metadata["sheet_name"] = sheet_name
        
    cached_info = get_cached_file_info(file_name)
    
    # Check if file has been modified since last download
    if cached_info and cached_info.get("etag") == metadata.get("eTag"):
        logging.info(f"File {file_name} has not changed since last download.")
        return None, 0
    
    # Download the file content
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/octet-stream"
    }
    
    file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    
    logging.info('-' * 30)
    logging.info(f"Downloading {file_name} from URL: {file_url}")
    response = requests.get(file_url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to download {file_name}: {response.status_code} - {response.text}")
    
    # Load the Excel data
    excel_data = BytesIO(response.content)
    
    # Check if a specific sheet name is provided in the config
    sheet_name = metadata.get("sheet_name", 0)  # Default to first sheet (0) if not specified
    
    # Get all available sheets for logging
    excel_file = pd.ExcelFile(excel_data)
    available_sheets = excel_file.sheet_names
    logging.info(f"Available sheets in {file_name}: {available_sheets}")
    
    # Read the specified sheet
    if sheet_name in available_sheets:
        logging.info(f"Reading sheet: {sheet_name}")
        df = pd.read_excel(excel_data, sheet_name=sheet_name)
    else:
        logging.warning(f"Sheet '{sheet_name}' not found. Using first sheet: {available_sheets[0]}")
        df = pd.read_excel(excel_data, sheet_name=0)  # Use first sheet as fallback
    
    # Identify new rows only
    new_rows_df, new_rows_count = identify_new_rows(df, file_name)

    if new_rows_count == 0:
        logging.warning(f"No new rows found in {file_name}. Skipping download and cache update.")
        return None, 0

    # Save the full file locally only if new rows exist
    output_file = os.path.join(DOWNLOAD_DIR, file_name)
    with open(output_file, 'wb') as f:
        f.write(response.content)

    logging.info(f"Full file saved as '{output_file}'")

    
    # Save only the new rows to a separate file
    if new_rows_count > 0:
        # Ensure subfolder for new_rows exists
        new_rows_dir = os.path.join(DOWNLOAD_DIR, "new_rows")
        os.makedirs(new_rows_dir, exist_ok=True)

        # Save new rows to this subfolder
        new_rows_file = os.path.join(new_rows_dir, f"{file_name.replace('.xlsx', '.csv')}")
        new_rows_df.to_csv(new_rows_file, index=False)
        logging.info(f"Found {new_rows_count} new rows. Saved to '{new_rows_file}'")
        logging.info('-' * 30)
    else:
        logging.info(f"No new rows found in {file_name}")
    
    # Update cached file info
    save_cached_file_info(file_name, metadata)
    
    return new_rows_df, new_rows_count

if __name__ == "__main__":
    try:
        # Load configuration from JSON file
        config_file = "config/files_config.json"
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
                files_to_download = config["files_to_download"]
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            logging.warning(f"Error loading configuration from {config_file}: {e}")
            logging.warning("Please ensure the JSON file exists and has the correct format.")
            exit(1)
        
        # Get access token (will try to use saved token first, only opening browser if needed)
        access_token = get_ms_access_token()
        
        # Track total new rows across all files
        total_new_rows = 0
        
        # Download each file
        for file_info in files_to_download:
            # Extract sheet_name if provided in config
            sheet_name = file_info.get("sheet_name", None)
            
            _, new_rows_count = download_specific_file(
                access_token,
                file_info["drive_id"],
                file_info["item_id"],
                file_info["name"],
                sheet_name
            )
            total_new_rows += new_rows_count
            
        logging.info(f"\nProcess completed! Downloaded {total_new_rows} new rows across all files.")
        
    except Exception as e:
        logging.error(f"Error: {e}")