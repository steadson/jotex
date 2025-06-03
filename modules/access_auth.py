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
class BusinessCentralAuth:
    def __init__(self, tenant_id, client_id, client_secret):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    def get_access_token(self):
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        token_data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
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

class MicrosoftAuth:
    def __init__(self, client_id, client_secret, tenant_id, redirect_uri, token_file, scope):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.redirect_uri = redirect_uri
        self.token_file = token_file
        self.scope = scope
        self.authority = f"https://login.microsoftonline.com/{tenant_id}"

    def _get_auth_code(self):
        auth_url = f"{self.authority}/oauth2/v2.0/authorize"
        auth_params = {
            'client_id': self.client_id,
            'response_type': 'code',
            'redirect_uri': self.redirect_uri,
            'scope': self.scope,
            'response_mode': 'query'
        }
        full_url = f"{auth_url}?{urlencode(auth_params)}"
        webbrowser.open(full_url)
        redirect_response = input("Paste the full redirect URL: ")

        parsed_url = urlparse(redirect_response)
        return parse_qs(parsed_url.query).get('code', [None])[0]

    def _get_token_from_code(self, code):
        token_url = f"{self.authority}/oauth2/v2.0/token"
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': self.redirect_uri,
            'scope': self.scope
        }

        response = requests.post(token_url, data=data)
        token_info = response.json()

        if 'access_token' in token_info and 'refresh_token' in token_info:
            token_info['expires_at'] = time.time() + token_info.get('expires_in', 3600)
            with open(self.token_file, 'w') as f:
                json.dump(token_info, f)
            return token_info['access_token']
        raise Exception(f"Failed to obtain tokens: {token_info}")

    def _refresh_token(self):
        if not os.path.exists(self.token_file):
            return None

        with open(self.token_file, 'r') as f:
            token_info = json.load(f)

        if token_info.get('expires_at', 0) > time.time() + 300:
            return token_info['access_token']

        token_url = f"{self.authority}/oauth2/v2.0/token"
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'refresh_token',
            'refresh_token': token_info.get('refresh_token'),
            'scope': self.scope
        }

        response = requests.post(token_url, data=data)
        new_token_info = response.json()

        if 'access_token' in new_token_info and 'refresh_token' in new_token_info:
            new_token_info['expires_at'] = time.time() + new_token_info.get('expires_in', 3600)
            with open(self.token_file, 'w') as f:
                json.dump(new_token_info, f)
            return new_token_info['access_token']
        return None

    def get_access_token(self):
        token = self._refresh_token()
        if token:
            return token
        print("No valid token found. Launching browser for authentication.")
        code = self._get_auth_code()
        return self._get_token_from_code(code)
