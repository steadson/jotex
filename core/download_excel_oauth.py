import requests
import os
import pandas as pd
import json
import webbrowser
from urllib.parse import urlencode, parse_qs, urlparse
from io import BytesIO
from dotenv import load_dotenv
import time
import hashlib

load_dotenv()

CLIENT_ID = os.getenv('STOCK_SHAREPOINT_EXCEL_FINANCE_CLIENT_ID')
CLIENT_SECRET = os.getenv('STOCK_SHAREPOINT_EXCEL_FINANCE_CLIENT_SECRET')
TENANT_ID = os.getenv('STOCK_SHAREPOINT_EXCEL_FINANCE_TENANT_ID')

REDIRECT_URI = os.getenv('REDIRECT_URI', 'http://localhost:8000')
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
TOKEN_FILE = "ms_token.json"

# For delegated permissions
DELEGATED_SCOPE = "Files.Read Files.Read.All Sites.Read.All offline_access"

# Directory for storing downloaded files and cache
DOWNLOAD_DIR = os.path.join(os.getcwd(), "data/downloads")
CACHE_DIR = os.path.join(os.getcwd(), "data/cache")


# Ensure directories exist
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

def get_auth_code():
    """
    Open browser for user to authenticate and get authorization code.
    """
    auth_url = f"{AUTHORITY}/oauth2/v2.0/authorize"
    auth_params = {
        'client_id': CLIENT_ID,
        'response_type': 'code',
        'redirect_uri': REDIRECT_URI,
        'scope': DELEGATED_SCOPE,
        'response_mode': 'query'
    }
    
    full_auth_url = f"{auth_url}?{urlencode(auth_params)}"
    print(f"Opening browser for authentication. Please authorize and copy the code from the redirected URL.")
    webbrowser.open(full_auth_url)
    
    # Ask user to input the full redirect URL
    redirect_url = input("Please paste the entire redirected URL: ")
    
    # Parse the URL to extract the code
    parsed_url = urlparse(redirect_url)
    query_params = parse_qs(parsed_url.query)
    auth_code = query_params.get('code', [None])[0]
    
    if not auth_code:
        raise Exception("No authorization code found in the URL")
    
    return auth_code

def get_token_from_auth_code(auth_code):
    """
    Exchange authorization code for token and save it with refresh token.
    """
    token_url = f"{AUTHORITY}/oauth2/v2.0/token"
    token_data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scope': DELEGATED_SCOPE,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code',
        'code': auth_code
    }

    response = requests.post(token_url, data=token_data)
    token_info = response.json()

    if 'access_token' not in token_info or 'refresh_token' not in token_info:
        raise Exception(f"Failed to get tokens: {token_info}")
    
    # Add expiry time for easier checks later
    token_info['expires_at'] = time.time() + token_info.get('expires_in', 3600)
    
    # Save token info to file
    with open(TOKEN_FILE, 'w') as f:
        json.dump(token_info, f)
    
    print("Successfully obtained and saved access token!")
    return token_info['access_token']

def refresh_saved_token():
    """
    Use refresh token to get a new access token.
    """
    try:
        # Check if we have a saved token
        if not os.path.exists(TOKEN_FILE):
            return None
        
        with open(TOKEN_FILE, 'r') as f:
            token_info = json.load(f)
        
        # Check if refresh token exists and access token not expired
        if 'refresh_token' not in token_info:
            return None
            
        # If token is still valid, return it
        if 'expires_at' in token_info and token_info['expires_at'] > time.time() + 300:
            print("Using existing access token!")
            return token_info['access_token']
        
        # Otherwise refresh it
        token_url = f"{AUTHORITY}/oauth2/v2.0/token"
        token_data = {
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'scope': DELEGATED_SCOPE,
            'grant_type': 'refresh_token',
            'refresh_token': token_info['refresh_token']
        }

        response = requests.post(token_url, data=token_data)
        new_token_info = response.json()

        if 'access_token' not in new_token_info or 'refresh_token' not in new_token_info:
            return None
            
        # Add expiry time
        new_token_info['expires_at'] = time.time() + new_token_info.get('expires_in', 3600)
        
        # Save updated token info
        with open(TOKEN_FILE, 'w') as f:
            json.dump(new_token_info, f)
        
        print("Successfully refreshed access token!")
        return new_token_info['access_token']
    except Exception as e:
        print(f"Error refreshing token: {e}")
        return None

def get_access_token():
    """
    Get an access token - first try to use/refresh a saved token,
    if that fails, go through the auth code flow.
    """
    # Try to use a saved refresh token
    access_token = refresh_saved_token()
    if access_token:
        return access_token
        
    # If that fails, go through the full auth flow
    print("No valid saved token found. Need to authenticate through the browser...")
    auth_code = get_auth_code()
    return get_token_from_auth_code(auth_code)

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
        print(f"No cached data found for {file_name}. All rows are considered new.")
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
        print("Many columns found - creating hash of row values to identify unique rows")
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
        print(f"Using columns {key_columns} to identify unique rows")
        
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
        print(f"File {file_name} has not changed since last download.")
        return None, 0
    
    # Download the file content
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/octet-stream"
    }
    
    file_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
    
    print('-' * 30)
    print(f"Downloading {file_name} from URL: {file_url}")
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
    print(f"Available sheets in {file_name}: {available_sheets}")
    
    # Read the specified sheet
    if sheet_name in available_sheets:
        print(f"Reading sheet: {sheet_name}")
        df = pd.read_excel(excel_data, sheet_name=sheet_name)
    else:
        print(f"Sheet '{sheet_name}' not found. Using first sheet: {available_sheets[0]}")
        df = pd.read_excel(excel_data, sheet_name=0)  # Use first sheet as fallback
    
    # Save the full file locally
    output_file = os.path.join(DOWNLOAD_DIR, file_name)
    with open(output_file, 'wb') as f:
        f.write(response.content)
        
    print(f"Full file saved as '{output_file}'")
    
    # Identify new rows only
    new_rows_df, new_rows_count = identify_new_rows(df, file_name)
    
    # Save only the new rows to a separate file
    if new_rows_count > 0:
        new_rows_file = os.path.join(DOWNLOAD_DIR, f"new_rows_{file_name.replace('.xlsx', '.csv')}")
        new_rows_df.to_csv(new_rows_file, index=False)
        print(f"Found {new_rows_count} new rows. Saved to '{new_rows_file}'")
    else:
        print(f"No new rows found in {file_name}")
    
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
            print(f"Error loading configuration from {config_file}: {e}")
            print("Please ensure the JSON file exists and has the correct format.")
            exit(1)
        
        # Get access token (will try to use saved token first, only opening browser if needed)
        access_token = get_access_token()
        
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
            
        print(f"\nProcess completed! Downloaded {total_new_rows} new rows across all files.")
        
    except Exception as e:
        print(f"Error: {e}")