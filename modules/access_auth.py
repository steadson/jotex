import os
import time
import json
import logging
import requests
import webbrowser
from urllib.parse import urlencode, parse_qs, urlparse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ==========================
# Business Central Auth (BC)
# ==========================
def get_bc_auth_token():
    """
    Retrieve an access token for Business Central using client credentials.
    """
    tenant_id = os.getenv('TENANT_ID')
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')

    if not all([tenant_id, client_id, client_secret]):
        logging.error("Missing BC OAuth credentials in .env file")
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
    except requests.RequestException as e:
        logging.error(f"Error obtaining BC token: {e}")
        return None


# =====================
# Microsoft Auth (MS)
# =====================
MS_CLIENT_ID = os.getenv('STOCK_SHAREPOINT_EXCEL_FINANCE_CLIENT_ID')
MS_CLIENT_SECRET = os.getenv('STOCK_SHAREPOINT_EXCEL_FINANCE_CLIENT_SECRET')
MS_TENANT_ID = os.getenv('STOCK_SHAREPOINT_EXCEL_FINANCE_TENANT_ID')
MS_REDIRECT_URI = os.getenv('REDIRECT_URI', 'http://localhost:8000')

MS_AUTHORITY = f"https://login.microsoftonline.com/{MS_TENANT_ID}"
MS_TOKEN_FILE = "ms_token.json"
MS_SCOPE = "Files.Read Files.Read.All Sites.Read.All offline_access"


def get_ms_auth_code():
    """
    Open browser for user to authenticate and return the authorization code.
    """
    auth_url = f"{MS_AUTHORITY}/oauth2/v2.0/authorize"
    auth_params = {
        'client_id': MS_CLIENT_ID,
        'response_type': 'code',
        'redirect_uri': MS_REDIRECT_URI,
        'scope': MS_SCOPE,
        'response_mode': 'query'
    }

    full_url = f"{auth_url}?{urlencode(auth_params)}"
    webbrowser.open(full_url)
    redirect_response = input("Paste the full redirect URL: ")

    parsed_url = urlparse(redirect_response)
    code = parse_qs(parsed_url.query).get('code', [None])[0]

    if not code:
        raise Exception("Authorization code not found in the URL")

    return code


def get_ms_token_from_code(code):
    """
    Exchange authorization code for access and refresh tokens.
    """
    token_url = f"{MS_AUTHORITY}/oauth2/v2.0/token"
    data = {
        'client_id': MS_CLIENT_ID,
        'client_secret': MS_CLIENT_SECRET,
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': MS_REDIRECT_URI,
        'scope': MS_SCOPE
    }

    response = requests.post(token_url, data=data)
    token_info = response.json()

    if 'access_token' not in token_info or 'refresh_token' not in token_info:
        raise Exception(f"Failed to get tokens: {token_info}")

    token_info['expires_at'] = time.time() + token_info.get('expires_in', 3600)

    with open(MS_TOKEN_FILE, 'w') as f:
        json.dump(token_info, f)

    print("Access token saved.")
    return token_info['access_token']


def refresh_ms_token():
    """
    Refresh the saved MS access token using the refresh token.
    """
    if not os.path.exists(MS_TOKEN_FILE):
        return None

    with open(MS_TOKEN_FILE, 'r') as f:
        token_info = json.load(f)

    if 'refresh_token' not in token_info:
        return None

    # If token still valid, reuse it
    if token_info.get('expires_at', 0) > time.time() + 300:
        print("Using existing access token.")
        return token_info['access_token']

    # Otherwise, refresh
    token_url = f"{MS_AUTHORITY}/oauth2/v2.0/token"
    data = {
        'client_id': MS_CLIENT_ID,
        'client_secret': MS_CLIENT_SECRET,
        'grant_type': 'refresh_token',
        'refresh_token': token_info['refresh_token'],
        'scope': MS_SCOPE
    }

    response = requests.post(token_url, data=data)
    new_token_info = response.json()

    if 'access_token' not in new_token_info or 'refresh_token' not in new_token_info:
        return None

    new_token_info['expires_at'] = time.time() + new_token_info.get('expires_in', 3600)

    with open(MS_TOKEN_FILE, 'w') as f:
        json.dump(new_token_info, f)

    print("Access token refreshed.")
    return new_token_info['access_token']


def get_ms_access_token():
    """
    Retrieve a Microsoft access token. Try refresh first, then full auth flow if needed.
    """
    token = refresh_ms_token()
    if token:
        return token

    print("No valid token found. Launching browser for authentication.")
    auth_code = get_ms_auth_code()
    return get_ms_token_from_code(auth_code)
