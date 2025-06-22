"""
Bank Transaction Processing Workflow

This script orchestrates the complete workflow for processing bank transactions
through a series of steps from downloading data to uploading processed results.

Workflow Sequence:
- `core/download_excel_oauth.py`
    - Depends on the `data/downloads/new_rows` download files filter the rows, if the row "CUSTOMER_NAME" is not empty, remove the row.
- Pairs run:
    - `MBB 2025.csv` ⟶ `nlp_parser/MY_mbb_txn_parser_nlp.py` ⟶ `core/MY_mbb_create_pymt.py`
    - `PBB 2025.csv` ⟶ `nlp_parser/MY_pbb_txn_parser_nlp.py` ⟶ `core/MY_pbb_create_pymt.py`
    - `JOTEX PTE LTD MAYBANK SG 2025.csv` ⟶ `parser/SG_mbb_txn_parser.py` ⟶ `core/SG_mbb_create_pymt.py`
    - `Smarthome MBB 2025.csv` ⟶ `parser/smarthome_mbb_txn_parser.py` ⟶ `core/smarthome_mbb_create_pymt.py`
- After finish all `core/upload_to_onedrive.py`
- Delete files in `data/downloads/new_rows` and `data/temp` folder
"""

import logging
import datetime
import subprocess
import sys
from pathlib import Path
import os

# Add the project root (parent of core/) to sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))

# Core components
import download_excel_oauth, MY_mbb_create_pymt, MY_pbb_create_pymt, SG_mbb_create_pymt, smarthome_mbb_create_pymt, upload_to_onedrive

# Parsers
from nlp_parser import MY_mbb_txn_parser_nlp, MY_pbb_txn_parser_nlp
from parser import SG_mbb_txn_parser, smarthome_mbb_txn_parser

# Utilities
from utils.filter_utils import filter_empty_rows
from utils.cleanup_utils import delete_workflow_files


def run_script(script_func):
    try:
        script_func.main() if hasattr(script_func, "main") else script_func()
        return True
    except Exception as e:
        logging.error(f"Script failed: {e}")
        return False


def execute_workflow():
    logging.info("Starting workflow execution")
    print("Starting workflow execution")

    # Step 1: Download transactions
    try:
        logging.info("Step 1: Downloading transactions...")
        subprocess.run([sys.executable, "core/download_excel_oauth.py"], check=True)
        logging.info("Download completed successfully")
    except Exception as e:
        logging.error(f"Download failed: {e}")
        return

    # Step 2: Process each file pair
    steps = [
        {
            "file": "data/downloads/new_rows/MBB 2025.csv",
            "parser": MY_mbb_txn_parser_nlp,
            "payment": MY_mbb_create_pymt
        },
        {
            "file": "data/downloads/new_rows/PBB 2025.csv",
            "parser": MY_pbb_txn_parser_nlp,
            "payment": MY_pbb_create_pymt
        },
        {
            "file": "data/downloads/new_rows/JOTEX PTE LTD MAYBANK SG 2025.csv",
            "parser": SG_mbb_txn_parser,
            "payment": SG_mbb_create_pymt
        },
        {
            "file": "data/downloads/new_rows/Smarthome MBB 2025.csv",
            "parser": smarthome_mbb_txn_parser,
            "payment": smarthome_mbb_create_pymt
        }
    ]

    any_processed = False

    for step in steps:
        file_path = step["file"]
        if Path(file_path).exists():
            logging.info(f"Processing {file_path}")
            print(f"Processing {file_path}")
            
            # Filter rows (remove rows where CUSTOMER_NAME is NOT empty)
            if filter_empty_rows(file_path):
                # Run parser
                logging.info(f"Running parser for {file_path}")
                if run_script(step["parser"]):
                    # Run payment creation
                    logging.info(f"Running payment creation for {file_path}")
                    if run_script(step["payment"]):
                        any_processed = True
                        logging.info(f"Successfully processed {file_path}")
                    else:
                        logging.error(f"Payment creation failed for {file_path}")
                else:
                    logging.error(f"Parser failed for {file_path}")
            else:
                logging.info(f"No rows to process in {file_path}")
        else:
            logging.info(f"File not found: {file_path}")

    # Step 3: Upload to OneDrive if anything was processed
    if any_processed:
        logging.info("Step 3: Uploading to OneDrive...")
        print("Uploading to OneDrive...")
        run_script(upload_to_onedrive)
    else:
        logging.info("No files were processed. Skipping upload.")

    # Step 4: Clean up files
    logging.info("Step 4: Cleaning up files...")
    print("Cleaning up files...")
    delete_workflow_files()

    logging.info("Workflow completed successfully")
    print("Workflow completed successfully")


def main():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.datetime.now().strftime('%d%m%Y_%H%M%S')
    log_file = log_dir / f"{timestamp}_workflow.log"

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    execute_workflow()


if __name__ == "__main__":
    main()
