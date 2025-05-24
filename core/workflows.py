"""
Bank Transaction Processing Workflow

This script orchestrates the complete workflow for processing bank transactions
through a series of steps from downloading data to uploading processed results.

Workflow Sequence:
1. download_excel_oauth.py - Download transaction data
2. nlp_parser/mbb_txn_parser_nlp.py - Parse MBB transactions
3. create_pymt_mbb.py - Create MBB payments
4. nlp_parser/pbb_txn_parser_nlp.py - Parse PBB transactions
5. create_pymt_pbb.py - Create PBB payments
6. upload_to_onedrive.py - Upload results to OneDrive
"""

import logging
import datetime
import subprocess
import sys
from pathlib import Path
import os
import pandas as pd

# Fix 1: Use relative imports since we're inside the core directory
# from . import download_excel_oauth, create_pymt_mbb, create_pymt_pbb, upload_to_onedrive

# Alternative Fix 2: Import directly from the same directory
import download_excel_oauth, create_pymt_mbb, create_pymt_pbb, upload_to_onedrive

# Rest of the code remains the same
def setup_logging():
    """Configure logging for the workflow execution."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.datetime.now().strftime('%d%m%Y_%H%M')
    log_file = log_dir / f"{timestamp}_finance_workflow.log"
    
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    return log_file


def run_script(script_path):
    """
    Execute a Python script using subprocess (safer than exec).
    
    Args:
        script_path: Path to the script to execute
        
    Returns:
        bool: True if execution was successful, False otherwise
    """
    try:
        logging.info(f"Executing step: {script_path}")
        print(f"Executing step: {script_path}")
        
        # Run the script using the same Python interpreter
        result = subprocess.run(
            [sys.executable, script_path],
            check=True,
            capture_output=True,
            text=True
        )
        
        # Log the output
        if result.stdout:
            logging.info(f"Output from {script_path}:\n{result.stdout}")
            print(result.stdout)
            
        logging.info(f"Successfully completed step: {script_path}")
        return True
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Error executing {script_path}: {str(e)}"
        logging.error(error_msg)
        print(error_msg)
        
        if e.stdout:
            logging.info(f"Output from {script_path}:\n{e.stdout}")
            print(e.stdout)
            
        if e.stderr:
            logging.error(f"Error output from {script_path}:\n{e.stderr}")
            print(e.stderr)
            
        logging.exception("Exception details:")
        return False


def get_workflow_steps():
    """
    Define the sequence of workflow steps.
    
    Returns:
        list: Ordered list of script paths to execute
    """
    return [
        'core/download_excel_oauth.py',
        'nlp_parser/mbb_txn_parser_nlp.py', 
        'nlp_parser/pbb_txn_parser_nlp.py',
        'core/create_pymt_mbb.py',
        'core/create_pymt_pbb.py',
        'core/upload_to_onedrive.py'
    ]


def check_new_rows_from_output(output):
    """
    Check if new rows are available for MBB and PBB files based on the download script output.
    
    Args:
        output: The output from the download_excel_oauth.py script
        
    Returns:
        tuple: (mbb_has_new_rows, pbb_has_new_rows) indicating if each file has new rows
    """
    # Default to no new rows
    mbb_has_new_rows = False
    pbb_has_new_rows = False
    
    # Check for specific patterns in the output
    if output:
        # Check for MBB file status
        if "MBB 2025.xlsx has not changed" in output or "Downloaded 0 new rows" in output:
            logging.info("Output indicates no new MBB rows")
        else:
            # Look for patterns indicating new rows for MBB
            import re
            mbb_matches = re.search(r"Found (\d+) new rows.*MBB 2025", output)
            if mbb_matches and int(mbb_matches.group(1)) > 0:
                mbb_has_new_rows = True
                logging.info(f"Output indicates {mbb_matches.group(1)} new MBB rows")
            elif "new_rows_MBB" in output:
                # If we see the new rows file mentioned in the output
                mbb_has_new_rows = True
                logging.info("Output indicates new MBB rows were created")
        
        # Check for PBB file status
        if "PBB 2025.xlsx has not changed" in output or "Downloaded 0 new rows" in output:
            logging.info("Output indicates no new PBB rows")
        else:
            # Look for patterns indicating new rows for PBB
            import re
            pbb_matches = re.search(r"Found (\d+) new rows.*PBB 2025", output)
            if pbb_matches and int(pbb_matches.group(1)) > 0:
                pbb_has_new_rows = True
                logging.info(f"Output indicates {pbb_matches.group(1)} new PBB rows")
            elif "new_rows_PBB" in output:
                # If we see the new rows file mentioned in the output
                pbb_has_new_rows = True
                logging.info("Output indicates new PBB rows were created")
    
    # As a fallback, also check if the files exist and have content

    
    mbb_new_rows_file = Path("downloads/new_rows/MBB 2025.csv")
    pbb_new_rows_file = Path("downloads/new_rows/PBB 2025.csv")
    
    # If we couldn't determine from output, check if files exist and were recently modified
    if not mbb_has_new_rows and mbb_new_rows_file.exists():
        try:
            mbb_df = pd.read_csv(mbb_new_rows_file)
            if len(mbb_df) > 0:
                # Check if file was modified in the last 5 minutes
                import time
                from datetime import datetime, timedelta
                mtime = datetime.fromtimestamp(mbb_new_rows_file.stat().st_mtime)
                if mtime > datetime.now() - timedelta(minutes=5):
                    mbb_has_new_rows = True
                    logging.info(f"MBB file was recently modified and has {len(mbb_df)} rows")
        except Exception as e:
            logging.error(f"Error reading MBB new rows file: {e}")
    
    if not pbb_has_new_rows and pbb_new_rows_file.exists():
        try:
            pbb_df = pd.read_csv(pbb_new_rows_file)
            if len(pbb_df) > 0:
                # Check if file was modified in the last 5 minutes
                import time
                from datetime import datetime, timedelta
                mtime = datetime.fromtimestamp(pbb_new_rows_file.stat().st_mtime)
                if mtime > datetime.now() - timedelta(minutes=5):
                    pbb_has_new_rows = True
                    logging.info(f"PBB file was recently modified and has {len(pbb_df)} rows")
        except Exception as e:
            logging.error(f"Error reading PBB new rows file: {e}")
    
    logging.info(f"New rows check result - MBB: {mbb_has_new_rows}, PBB: {pbb_has_new_rows}")
    return mbb_has_new_rows, pbb_has_new_rows


def filter_empty_rows(file_path, columns_to_check=None):
    """
    Filter rows where specified columns are empty and save only those rows.
    
    Args:
        file_path: Path to the CSV file to filter
        columns_to_check: List of column names to check for empty values
        
    Returns:
        int: Number of rows with empty values in specified columns
    """
    if columns_to_check is None:
        columns_to_check = ["CUSTOMER_NAME"]
    
    if not Path(file_path).exists():
        logging.warning(f"File {file_path} does not exist, cannot filter rows")
        return 0
    
    try:
        df = pd.read_csv(file_path)
        original_count = len(df)
        logging.info(f"Original file {file_path} has {original_count} rows")
        
        # Check if all specified columns exist in the dataframe
        existing_columns = [col for col in columns_to_check if col in df.columns]
        
        if not existing_columns:
            logging.warning(f"None of the specified columns {columns_to_check} exist in {file_path}")
            return original_count  # If none of the columns exist, keep all rows
        
        # Create a mask for rows where ANY specified columns are empty
        empty_mask = pd.Series([False] * len(df))  # Start with all False
        
        for col in existing_columns:
            # For each column, check if it's empty (NaN or empty string)
            col_empty = df[col].isna() | (df[col] == '') | (df[col].astype(str).str.strip() == '')
            empty_mask = empty_mask | col_empty  # Use OR to find any empty columns
        
        # Filter to keep only rows where any specified columns are empty
        filtered_df = df[empty_mask]
        filtered_count = len(filtered_df)
        
        logging.info(f"Found {filtered_count} rows with empty values in columns {existing_columns}")
        print(f"Found {filtered_count} rows with empty values in columns {existing_columns}")
        
        if filtered_count > 0:
            # Save the filtered data back to the same file
            filtered_df.to_csv(file_path, index=False)
            logging.info(f"Saved {filtered_count} filtered rows to {file_path}")
            print(f"Saved {filtered_count} filtered rows to {file_path}")
        else:
            logging.info(f"No rows with empty columns found in {file_path}")
            print(f"No rows with empty columns found in {file_path}")
        
        return filtered_count
        
    except Exception as e:
        logging.error(f"Error filtering rows in {file_path}: {e}")
        return 0


def execute_workflow(continue_on_error=False):
    """
    Main workflow execution function that runs the bank transaction processing pipeline
    in the correct sequence.
    
    Args:
        continue_on_error: If True, continue execution even if a step fails
        
    Returns:
        bool: True if all steps completed successfully, False otherwise
    """
    logging.info("Starting workflow execution")
    print("Starting workflow execution")
    
    workflow_steps = get_workflow_steps()
    success = True
    
    # Run the download step and capture its output
    download_step = workflow_steps[0]  # 'core/download_excel_oauth.py'
    
    try:
        logging.info(f"Executing step: {download_step}")
        print(f"Executing step: {download_step}")
        
        # Run the script using the same Python interpreter
        result = subprocess.run(
            [sys.executable, download_step],
            check=True,
            capture_output=True,
            text=True
        )
        
        download_output = result.stdout
        
        # Log the output
        if download_output:
            logging.info(f"Output from {download_step}:\n{download_output}")
            print(download_output)
            
        logging.info(f"Successfully completed step: {download_step}")
        download_success = True
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Error executing {download_step}: {str(e)}"
        logging.error(error_msg)
        print(error_msg)
        
        download_output = e.stdout if e.stdout else ""
        
        if e.stdout:
            logging.info(f"Output from {download_step}:\n{e.stdout}")
            print(e.stdout)
            
        if e.stderr:
            logging.error(f"Error output from {download_step}:\n{e.stderr}")
            print(e.stderr)
            
        logging.exception("Exception details:")
        download_success = False
    
    if not download_success:
        logging.error(f"Workflow stopped due to error in download step: {download_step}")
        print(f"Workflow stopped due to error in download step: {download_step}")
        return False
    
    # Check if new rows are available based on the download output
    mbb_has_new_rows, pbb_has_new_rows =  (download_output)
    
    if not mbb_has_new_rows and not pbb_has_new_rows:
        logging.info("No new rows found for both MBB and PBB. Stopping workflow.")
        print("No new rows found for both MBB and PBB. Stopping workflow.")
        return True
    
    # Filter the downloaded files to keep only rows with empty columns
    mbb_filtered_count = 0
    pbb_filtered_count = 0
    
    if mbb_has_new_rows:
        mbb_file = Path("downloads/new_rows/MBB 2025.csv")
        mbb_filtered_count = filter_empty_rows(mbb_file)
        if mbb_filtered_count == 0:
            logging.info("No MBB rows with empty columns found after filtering")
            print("No MBB rows with empty columns found after filtering")
            mbb_has_new_rows = False
    
    if pbb_has_new_rows:
        pbb_file = Path("downloads/new_rows/PBB 2025.csv")
        pbb_filtered_count = filter_empty_rows(pbb_file)
        if pbb_filtered_count == 0:
            logging.info("No PBB rows with empty columns found after filtering")
            print("No PBB rows with empty columns found after filtering")
            pbb_has_new_rows = False
    
    # Check if we still have rows to process after filtering
    if not mbb_has_new_rows and not pbb_has_new_rows:
        logging.info("No rows with empty columns found for both MBB and PBB after filtering. Stopping workflow.")
        print("No rows with empty columns found for both MBB and PBB after filtering. Stopping workflow.")
        return True
    
    # Determine which steps to run based on available filtered rows
    steps_to_run = []
    
    if mbb_has_new_rows and mbb_filtered_count > 0:
        logging.info(f"Will process {mbb_filtered_count} MBB rows with empty columns.")
        print(f"Will process {mbb_filtered_count} MBB rows with empty columns.")
        steps_to_run.extend([
            'nlp_parser/mbb_txn_parser_nlp.py',
            'core/create_pymt_mbb.py'
        ])
    
    if pbb_has_new_rows and pbb_filtered_count > 0:
        logging.info(f"Will process {pbb_filtered_count} PBB rows with empty columns.")
        print(f"Will process {pbb_filtered_count} PBB rows with empty columns.")
        steps_to_run.extend([
            'nlp_parser/pbb_txn_parser_nlp.py',
            'core/create_pymt_pbb.py'
        ])
    
    # Always add upload step if any processing was done
    if steps_to_run:
        steps_to_run.append('core/upload_to_onedrive.py')
    
    # Run the selected steps
    for step in steps_to_run:
        step_success = run_script(step)
        
        if not step_success:
            success = False
            if not continue_on_error:
                logging.error(f"Workflow stopped due to error in step: {step}")
                print(f"Workflow stopped due to error in step: {step}")
                break
    
    return success


def main():
    """Main entry point for the workflow."""
    log_file = setup_logging()
    print(f"Logging to: {log_file}")
    
    try:
        success = execute_workflow(continue_on_error=False)
        
        if success:
            logging.info("Workflow completed successfully")
            print("Workflow completed successfully")
        else:
            logging.warning("Workflow completed with errors")
            print("Workflow completed with errors")
            
    except Exception as e:
        logging.error(f"Workflow failed: {str(e)}")
        print(f"Workflow failed: {str(e)}")
        logging.exception("Exception details:")
        return 1
        
    return 0


if __name__ == "__main__":
    sys.exit(main())