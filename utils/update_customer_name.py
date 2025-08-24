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

if __name__ == "__main__":
    update_customer_name(
        # customer_path=Path("data/customer_db/MY_PBB_customer_name.csv"),
        # input_file=Path("data/temp/PBB_2025_processed.csv")

        customer_path=Path("data/customer_db/MY_MBB_customer_name.csv"),
        input_file=Path("data/temp/MBB_2025_processed.csv")

        # customer_path=Path("data/customer_db/SG_MBB_customer_name.csv"),
        # input_file=Path("data/temp/SG_MBB_2025_processed.csv")
    )
