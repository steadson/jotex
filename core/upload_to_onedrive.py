import os
import requests
from dotenv import load_dotenv
import json
import time

# Load environment variables
load_dotenv()

CLIENT_ID = os.getenv('STOCK_SHAREPOINT_EXCEL_FINANCE_CLIENT_ID')
CLIENT_SECRET = os.getenv('STOCK_SHAREPOINT_EXCEL_FINANCE_CLIENT_SECRET')
TENANT_ID = os.getenv('STOCK_SHAREPOINT_EXCEL_FINANCE_TENANT_ID')
REDIRECT_URI = os.getenv('REDIRECT_URI', 'http://localhost:8000')
TOKEN_FILE = "ms_token.json"

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = "Files.ReadWrite.All offline_access"

UPLOAD_DIR = os.path.join(os.getcwd(), "data/output")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Upload targets
UPLOAD_PATHS = {
    "updated": "/FINANCE/TEMP/updated",
    "not_transferred": "/FINANCE/TEMP/not_transferred"
}

def load_token():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'r') as f:
            return json.load(f)
    return None

def refresh_token():
    token = load_token()
    if not token or 'refresh_token' not in token:
        raise Exception("Missing refresh token. Run initial authorization.")

    data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'refresh_token',
        'refresh_token': token['refresh_token'],
        'scope': SCOPE
    }

    response = requests.post(f"{AUTHORITY}/oauth2/v2.0/token", data=data)
    new_token = response.json()

    if 'access_token' not in new_token:
        raise Exception(f"Token refresh failed: {new_token}")

    new_token['expires_at'] = time.time() + new_token.get('expires_in', 3600)
    with open(TOKEN_FILE, 'w') as f:
        json.dump(new_token, f)

    return new_token['access_token']

def get_access_token():
    token = load_token()
    if token and token.get('expires_at', 0) > time.time() + 300:
        return token['access_token']
    return refresh_token()

def upload_file(file_path, graph_path, access_token):
    filename = os.path.basename(file_path)
    upload_url = f"https://graph.microsoft.com/v1.0/me/drive/root:{graph_path}/{filename}:/content"
    headers = {'Authorization': f'Bearer {access_token}'}

    with open(file_path, 'rb') as f:
        response = requests.put(upload_url, headers=headers, data=f)

    if response.ok:
        print(f"✅ Uploaded: {filename}")
        os.remove(file_path)
        print(f"🗑️ Deleted local file: {filename}")
    else:
        print(f"❌ Upload failed for {filename}: {response.status_code} - {response.text}")

def main():
    token = get_access_token()

    for file in os.listdir(UPLOAD_DIR):
        if file.endswith("_updated.xlsx"):
            upload_file(
                os.path.join(UPLOAD_DIR, file),
                UPLOAD_PATHS["updated"],
                token
            )
        elif file.endswith("_not_transferred.xlsx"):
            upload_file(
                os.path.join(UPLOAD_DIR, file),
                UPLOAD_PATHS["not_transferred"],
                token
            )

if __name__ == "__main__":
    main()
