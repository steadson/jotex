"""
Utility functions for filtering data files.
"""

import logging
import pandas as pd
from pathlib import Path


def filter_empty_rows(file_path, key_column="CUSTOMER_NAME"):
    """
    Filter rows where CUSTOMER_NAME is NOT empty (remove those rows).
    Keep only rows where CUSTOMER_NAME is empty.
    
    Args:
        file_path (str): Path to the CSV file to filter
        key_column (str): Column name to check for empty values (default: "CUSTOMER_NAME")
    
    Returns:
        bool: True if filtering was successful and rows were kept, False otherwise
    """
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
        
        # Filter to keep only rows where CUSTOMER_NAME is empty
        # (remove rows where CUSTOMER_NAME is NOT empty)
        df_filtered = df[
            df[actual_column].isna() |  # NaN values
            (df[actual_column].astype(str).str.strip() == "") |  # Empty strings
            (df[actual_column].astype(str).str.strip() == "nan") |  # "nan" strings
            (df[actual_column].astype(str).str.strip() == "None")  # "None" strings
        ]
        
        removed_count = len(df) - len(df_filtered)
        logging.info(f"Removed {removed_count} rows with non-empty {key_column}, kept {len(df_filtered)} rows with empty {key_column}")
        
        if len(df_filtered) > 0:
            df_filtered.to_csv(file_path, index=False, encoding='utf-8-sig')
            logging.info(f"Saved filtered data back to {file_path}")
            return True
        else:
            logging.info(f"No rows with empty {key_column} found in {file_path}")
            return False
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        import traceback
        logging.error(traceback.format_exc())
    return False


if __name__ == "__main__":
    # Test the function
    import sys
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
        print(f"Testing filter_empty_rows on {test_file}")
        result = filter_empty_rows(test_file)
        print(f"Result: {result}")
    else:
        print("Usage: python filter_utils.py <csv_file_path>") 