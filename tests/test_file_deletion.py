import os
from pathlib import Path
import sys
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def delete_existing_new_rows_files():
    """
    Delete any existing new rows files before running the download step.
    """
    mbb_new_rows_file = Path("downloads/new_rows_MBB 2025.csv")
    pbb_new_rows_file = Path("downloads/new_rows_PBB 2025.csv")
    
    # Delete MBB file if it exists
    if mbb_new_rows_file.exists():
        try:
            mbb_new_rows_file.unlink()
            logging.info(f"Deleted existing file: {mbb_new_rows_file}")
        except Exception as e:
            logging.error(f"Error deleting {mbb_new_rows_file}: {e}")
    
    # Delete PBB file if it exists
    if pbb_new_rows_file.exists():
        try:
            pbb_new_rows_file.unlink()
            logging.info(f"Deleted existing file: {pbb_new_rows_file}")
        except Exception as e:
            logging.error(f"Error deleting {pbb_new_rows_file}: {e}")

def check_files_exist():
    """Check if the files exist and print their status"""
    mbb_new_rows_file = Path("downloads/new_rows_MBB 2025.csv")
    pbb_new_rows_file = Path("downloads/new_rows_PBB 2025.csv")
    
    print(f"MBB file exists: {mbb_new_rows_file.exists()}")
    print(f"PBB file exists: {pbb_new_rows_file.exists()}")

if __name__ == "__main__":
    print("Before deletion:")
    check_files_exist()
    
    print("\nDeleting files...")
    delete_existing_new_rows_files()
    
    print("\nAfter deletion:")
    check_files_exist()