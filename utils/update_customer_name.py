from pathlib import Path
import pandas as pd
from difflib import SequenceMatcher

def similarity(a, b):
    """
    Calculate similarity ratio between two strings.
    Returns a value between 0 and 1, where 1 means identical.
    """
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def update_customer_name(customer_path, input_file, similarity_threshold=0.95):
    """
    Update customer names in input file by matching against customer database.
    Matches CUSTOMER_NAME from input file against SPECIAL NAME BANK IN column from customer database.
    When there's a match, replaces CUSTOMER_NAME in input file with CUSTOMER NAME from customer database.
    
    Args:
        customer_path (str): Path to customer database CSV file
        input_file (str): Path to input CSV file to update
        similarity_threshold (float): Minimum similarity ratio for matching (0.0 to 1.0), default 0.85 for near-exact matches
    """
    customer_df = pd.read_csv(customer_path)
    input_df = pd.read_csv(input_file)
    
    # Check if required columns exist
    if "SPECIAL NAME BANK IN" not in customer_df.columns:
        print("Error: 'SPECIAL NAME BANK IN' column not found in customer database")
        return
    
    if "CUSTOMER NAME" not in customer_df.columns:
        print("Error: 'CUSTOMER NAME' column not found in customer database")
        return
    
    if "CUSTOMER_NAME" not in input_df.columns:
        print("Error: 'CUSTOMER_NAME' column not found in input file")
        return
    
    
    updated_count = 0
    
    for index, row in input_df.iterrows():
        input_customer_name = str(row["CUSTOMER_NAME"]).strip()
        
        # Skip if customer name is empty or NaN
        if pd.isna(input_customer_name) or input_customer_name == "":
            continue
        
        best_match = None
        best_similarity = 0
        
        # Find the best match in customer database
        for _, customer_row in customer_df.iterrows():
            special_name = str(customer_row["SPECIAL NAME BANK IN"]).strip()
            
            # Skip if special name is empty or NaN
            if pd.isna(special_name) or special_name == "":
                continue
            
            # Calculate similarity between input customer name and special name
            sim_ratio = similarity(input_customer_name, special_name)
            
            # Check if this is the best match so far
            if sim_ratio > best_similarity and sim_ratio >= similarity_threshold:
                best_similarity = sim_ratio
                best_match = customer_row
        
        # If we found a good match, update the customer name
        if best_match is not None:
            new_customer_name = str(best_match["CUSTOMER NAME"]).strip()
            original_customer_name = str(row["CUSTOMER_NAME"]).strip()
            
            # Update the customer name in input dataframe
            input_df.at[index, "CUSTOMER_NAME"] = new_customer_name
            updated_count += 1
            
            print(f"Updated: '{original_customer_name}' -> '{new_customer_name}' (similarity: {best_similarity:.2f})")
            print(f"  Matched with database entry: '{best_match['SPECIAL NAME BANK IN']}'")
            print()
    
    # Save the updated dataframe
    input_df.to_csv(input_file, index=False)
    print(f"Updated {updated_count} customer names in {input_file}")

def update_customer_name_for_file(processed_file_path):
    """
    Automatically determine the correct customer database and update customer names
    based on the processed file name pattern.
    
    Args:
        processed_file_path (str): Path to the processed CSV file in data/temp/
    
    Returns:
        bool: True if update was successful, False otherwise
    """
    processed_file_path = Path(processed_file_path)
    
    # Check if file exists
    if not processed_file_path.exists():
        print(f"Error: Processed file not found: {processed_file_path}")
        return False
    
    # Determine customer database based on file name
    file_name = processed_file_path.name.upper()
    customer_db_mapping = {
        "MBB_2025_PROCESSED.CSV": "data/customer_db/MY_MBB_CUSTOMER_NAME.csv",
        "PBB_2025_PROCESSED.CSV": "data/customer_db/MY_PBB_CUSTOMER_NAME.csv", 
        "SG_MBB_2025_PROCESSED.CSV": "data/customer_db/SG_MBB_customer_name.csv",
        "SMARTHOME_MBB_2025_PROCESSED.CSV": "data/customer_db/MY_MBB_CUSTOMER_NAME.csv"  # Assuming Smarthome uses MY_MBB database
    }
    
    customer_db_path = None
    for pattern, db_path in customer_db_mapping.items():
        if pattern in file_name:
            customer_db_path = db_path
            break
    
    if customer_db_path is None:
        print(f"Warning: No customer database mapping found for file: {file_name}")
        return False
    
    customer_db_path = Path(customer_db_path)
    if not customer_db_path.exists():
        print(f"Error: Customer database not found: {customer_db_path}")
        return False
    
    print(f"Updating customer names for {processed_file_path} using database {customer_db_path}")
    
    try:
        update_customer_name(
            customer_path=str(customer_db_path),
            input_file=str(processed_file_path),
            similarity_threshold=0.95
        )
        return True
    except Exception as e:
        print(f"Error updating customer names: {e}")
        return False


def main():
    """
    Main function for standalone execution - processes all available files
    """
    temp_dir = Path("data/temp")
    if not temp_dir.exists():
        print("Error: data/temp directory not found")
        return
    
    # Look for processed files in temp directory
    processed_files = [
        temp_dir / "MBB_2025_processed.csv",
        temp_dir / "PBB_2025_processed.csv", 
        temp_dir / "SG_MBB_2025_processed.csv",
        temp_dir / "Smarthome_MBB_2025_processed.csv"
    ]
    
    for file_path in processed_files:
        if file_path.exists():
            print(f"\nProcessing: {file_path}")
            update_customer_name_for_file(file_path)
        else:
            print(f"File not found: {file_path}")


if __name__ == "__main__":
    main()
