import pandas as pd
from pathlib import Path

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
        print(f"[INFO]  Original file {file_path} has {original_count} rows")
        
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
        
        print(f"[INFO]  Found {filtered_count} rows with empty values in columns {existing_columns}")

        
        return filtered_count
        
    except Exception as e:
        print(f"[ERROR]  Error filtering rows in {file_path}: {e}")
        return 0



if __name__ == "__main__":
    # 🧪 Change this path to the one you want to test
    test_file = Path("data/downloads/new_rows/PBB 2025.csv")
    filter_empty_rows(test_file)
