import os
import sys
import json
import time
import shutil
import logging
import requests
from dotenv import load_dotenv

# Add parent directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.access_auth import MicrosoftAuth
from modules.logger import setup_logging

# Load environment variables
load_dotenv()

# Setup logging
setup_logging('upload_to_onedrive')

# Constants
UPLOAD_DIR = os.path.join(os.getcwd(), "data/output")
BACKUP_DIR = os.path.join(os.getcwd(), "data/output_backups")
UPLOAD_PATHS = {
    "updated": "/FINANCE/TEMP/updated",
    "not_transferred": "/FINANCE/TEMP/not_transferred"
}

# Ensure necessary directories exist
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

ms_auth = MicrosoftAuth(
    client_id=os.getenv("STOCK_SHAREPOINT_EXCEL_FINANCE_CLIENT_ID"),
    client_secret=os.getenv("STOCK_SHAREPOINT_EXCEL_FINANCE_CLIENT_SECRET"),
    tenant_id=os.getenv("STOCK_SHAREPOINT_EXCEL_FINANCE_TENANT_ID"),
    redirect_uri=os.getenv("REDIRECT_URI", "http://localhost:8000"),
    token_file="ms_token.json",
    scope="Files.Read Files.Read.All Sites.Read.All offline_access"
)

def upload_file(file_path: str, graph_path: str, access_token: str) -> None:
    """Uploads a file to OneDrive and moves it to backup if successful."""
    filename = os.path.basename(file_path)
    upload_url = f"https://graph.microsoft.com/v1.0/me/drive/root:{graph_path}/{filename}:/content"
    headers = {'Authorization': f'Bearer {access_token}'}

    try:
        with open(file_path, 'rb') as file:
            response = requests.put(upload_url, headers=headers, data=file)

        if response.ok:
            logging.info(f"Uploaded: {filename}")
            backup_path = os.path.join(BACKUP_DIR, filename)
            shutil.move(file_path, backup_path)
            logging.info(f"Moved to backup: {backup_path}")
        else:
            logging.error(f"Upload failed for {filename}: {response.status_code} - {response.text}")

    except Exception as e:
        logging.exception(f"Exception while uploading {filename}: {str(e)}")

def main() -> None:
    """Main function to handle file uploads."""
    access_token = ms_auth.get_access_token()

    for filename in os.listdir(UPLOAD_DIR):
        file_path = os.path.join(UPLOAD_DIR, filename)

        if filename.endswith("_updated.xlsx"):
            upload_file(file_path, UPLOAD_PATHS["updated"], access_token)
        elif filename.endswith("_not_transferred.xlsx"):
            upload_file(file_path, UPLOAD_PATHS["not_transferred"], access_token)

if __name__ == "__main__":
    main()
