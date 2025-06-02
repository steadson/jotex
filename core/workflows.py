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
import download_excel_oauth, MY_mbb_create_pymt, MY_pbb_create_pymt, SG_mbb_create_pymt, smarthome_mbb_create_pymt, upload_to_onedrive

# Parsers
from nlp_parser import MY_mbb_txn_parser_nlp, MY_pbb_txn_parser_nlp
from parser import SG_mbb_txn_parser, smarthome_mbb_txn_parser


def filter_empty_rows(file_path, key_column="CUSTOMER_NAME"):
    if not Path(file_path).exists():
        logging.error(f"File does not exist: {file_path}")
        return False
    try:
        # Read CSV with explicit encoding and handle potential BOM
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        logging.info(f"Reading file {file_path} - Found {len(df)} rows")
        logging.info(f"Columns in file: {df.columns.tolist()}")
        
        # Find the exact column name (case-insensitive)
        matching_columns = [col for col in df.columns if col.strip().upper() == key_column.upper()]
        if not matching_columns:
            logging.error(f"Column '{key_column}' not found in file. Available columns: {df.columns.tolist()}")
            return False
            
        actual_column = matching_columns[0]
        logging.info(f"Using column: '{actual_column}'")
        
        # More comprehensive empty value check
        df_filtered = df[
            df[actual_column].isna() |  # NaN values
            (df[actual_column].astype(str).str.strip() == "") |  # Empty strings
            (df[actual_column].astype(str).str.strip() == "nan") |  # "nan" strings
            (df[actual_column].astype(str).str.strip() == "None")  # "None" strings
        ]
        
        logging.info(f"Found {len(df_filtered)} rows with empty {key_column}")
        if not df_filtered.empty:
            df_filtered.to_csv(file_path, index=False, encoding='utf-8-sig')
            logging.info(f"Saved filtered data back to {file_path}")
            return True
        else:
            logging.info(f"No empty {key_column} values found in {file_path}")
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        import traceback
        logging.error(traceback.format_exc())
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
    new_rows_downloaded = False  # Add a flag to track if any new rows were downloaded

    try:
        subprocess.run([sys.executable, "core/download_excel_oauth.py"], check=True)
        # Check if any new rows were downloaded (based on logs or a return value from the download script)
        new_rows_downloaded = True  # Set this based on actual logic
    except Exception as e:
        logging.error(f"Download failed: {e}")
        return

    # Skip processing if no new rows were downloaded
    if not new_rows_downloaded:
        logging.info("No new rows downloaded. Skipping processing.")
        print("No new rows downloaded. Skipping processing.")
        return

    # Step 2 & 3: Conditional processing
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
    log_file = log_dir / f"{timestamp}_workflow.log"

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    execute_workflow()


if __name__ == "__main__":
    main()
