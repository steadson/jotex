import os
import sys
import logging
import pandas as pd
import requests
import time
from datetime import datetime
import requests
import json
import webbrowser
from urllib.parse import urlencode, parse_qs, urlparse
from io import BytesIO
import time
import hashlib
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_bc_auth_token():
    tenant_id = os.getenv('TENANT_ID')
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    if not all([tenant_id, client_id, client_secret]):
        logging.error("Missing OAuth credentials in .env file")
        return None

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://api.businesscentral.dynamics.com/.default'
    }

    try:
        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        return response.json().get('access_token')
    except requests.exceptions.RequestException as e:
        logging.error(f"Error obtaining access token: {e}")
        return None
    

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

def get_ms_access_token():
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
