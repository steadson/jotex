"""
Bank Transaction Processing Workflow

This script orchestrates the complete workflow for processing bank transactions
through a series of steps from downloading data to uploading processed results.

Workflow Sequence:
1. download_excel_oauth.py - Download transaction data
2. For each downloaded file (e.g., MBB 2025, PBB 2025, SG MBB, Smarthome MBB), check if the 'CUSTOMER_NAME' column contains any empty values:
   - If any rows have empty 'CUSTOMER_NAME', retain only those rows for processing.
   - Skip files with no empty 'CUSTOMER_NAME' entries.
3. Conditional execution for each file type with valid empty 'CUSTOMER_NAME' rows:
   - Malaysia MBB:
     - nlp_parser/MY_mbb_txn_parser_nlp.py — Parse MBB transactions.
     - MY_mbb_create_pymt.py — Create MBB payments.
   - Malaysia PBB:
     - nlp_parser/MY_pbb_txn_parser_nlp.py — Parse PBB transactions.
     - MY_pbb_create_pymt.py — Create PBB payments.
   - Singapore MBB:
     - parser/SG_mbb_txn_parser.py — Parse SG MBB transactions.
     - SG_mbb_create_pymt.py — Create SG MBB payments.
   - Smarthome MBB:
     - parser/smarthome_mbb_txn_parser.py — Parse Smarthome MBB transactions.
     - smarthome_mbb_create_pymt.py — Create Smarthome MBB payments.
4. upload_to_onedrive.py - Upload results to OneDrive
"""

import logging
import datetime
import subprocess
import sys
from pathlib import Path
import os
import pandas as pd

# Add the project root (parent of core/) to sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))

# Core components
import download_excel_oauth, MY_mbb_create_pymt, MY_pbb_create_pymt, upload_to_onedrive
import SG_mbb_create_pymt, smarthome_mbb_create_pymt

# Parsers
from nlp_parser import MY_mbb_txn_parser_nlp, MY_pbb_txn_parser_nlp
from parser import SG_mbb_txn_parser, smarthome_mbb_txn_parser


def filter_empty_rows(file_path, key_column="CUSTOMER_NAME"):
    if not Path(file_path).exists():
        return False
    try:
        df = pd.read_csv(file_path)
        df_filtered = df[df[key_column].isna() | (df[key_column].astype(str).str.strip() == "")]
        if not df_filtered.empty:
            df_filtered.to_csv(file_path, index=False)
            return True
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
    return False


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
        subprocess.run([sys.executable, "core/download_excel_oauth.py"], check=True)

    except Exception as e:
        logging.error(f"Download failed: {e}")
        return

    # Step 2 & 3: Conditional processing
    steps = [
        {
            "file": "downloads/new_rows/MBB 2025.csv",
            "parser": MY_mbb_txn_parser_nlp,
            "payment": MY_mbb_create_pymt
        },
        {
            "file": "downloads/new_rows/PBB 2025.csv",
            "parser": MY_pbb_txn_parser_nlp,
            "payment": MY_pbb_create_pymt
        },
        {
            "file": "downloads/new_rows/SG_MBB.csv",
            "parser": SG_mbb_txn_parser,
            "payment": SG_mbb_create_pymt
        },
        {
            "file": "downloads/new_rows/Smarthome_MBB.csv",
            "parser": smarthome_mbb_txn_parser,
            "payment": smarthome_mbb_create_pymt
        }
    ]

    any_processed = False

    for step in steps:
        if filter_empty_rows(step["file"]):
            logging.info(f"Processing {step['file']}")
            print(f"Processing {step['file']}")
            if run_script(step["parser"]) and run_script(step["payment"]):
                any_processed = True

    # Step 4: Upload if anything was processed
    if any_processed:
        run_script(upload_to_onedrive)
    else:
        logging.info("No files had rows with empty CUSTOMER_NAME. Nothing to process.")

    print("Workflow completed.")


def main():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.datetime.now().strftime('%d%m%Y_%H%M%S')
    log_file = log_dir / f"workflow_{timestamp}.log"

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    execute_workflow()


if __name__ == "__main__":
    main()
