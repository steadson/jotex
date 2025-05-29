import pandas as pd
import re
import os
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def find_transaction_description_column(df):
    """Find the column containing descriptions.
    
    First looks specifically for 'Description', then tries generic matches.
    """
    # First try the exact column name
    if "Description" in df.columns:
        return "Description"
    
    # Fall back to more generic search
    for col in df.columns:
        if any(term in str(col).lower() for term in ['description', 'particulars', 'details']):
            return col
    
    return None

def clean_customer_name(name):
    """Clean and standardize customer names, returning both clean name and extra info for description."""
    if not name:
        return '', ''
    
    # Convert to string and strip whitespace
    name = str(name).strip()
    extra_info = ''
    
    # Remove trailing periods
    name = name.rstrip('.')
    
    # Fix "SDN. BHD." to "SDN BHD"
    name = re.sub(r'SDN\.\s*BHD\.?', 'SDN BHD', name)
    
    # Fix abbreviated names
    if name.endswith('SB'):
        name = name.replace('SB', '').strip()

    # Extract numeric sequences and what follows them
    match = re.search(r'\s+(\d{8,}.*$)', name)
    if match:
        extra_info = match.group(1).strip()
        name = name[:match.start()].strip()
    
    # Extract invoice/document numbers
    match = re.search(r'\s+([A-Z]{2,3}[-\d]+.*$)', name)
    if match:
        extra_info = (extra_info + ' ' + match.group(1)).strip()
        name = name[:match.start()].strip()
    
    # Extract dates
    match = re.search(r'\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{2}.*$', name, flags=re.IGNORECASE)
    if match:
        extra_info = (extra_info + ' ' + match.group(0)).strip()
        name = name[:match.start()].strip()
    
    # Extract year
    match = re.search(r'\s+(20\d{2}).*$', name)
    if match:
        extra_info = (extra_info + ' ' + match.group(1)).strip()
        name = name[:match.start()].strip()
    
    # Extract repeated company name at the end
    words = name.split()
    if len(words) > 3:
        company_name = ' '.join(words[:3]).lower()
        remaining = ' '.join(words[3:]).lower()
        if remaining.startswith(company_name):
            extra_info = (extra_info + ' ' + ' '.join(words[3:])).strip()
            name = ' '.join(words[:3])
    
    return name.strip(), extra_info.strip()

def process_inward_fast_transaction(txn_desc):
    """Process Inward FAST transaction descriptions."""
    customer_name = ''
    description = ''
    
    # Remove the prefix "Inward FAST - "
    remaining = txn_desc[len("Inward FAST - "):]
    
    # Split by comma
    parts = remaining.split(", ", 1)  # Split by first comma only
    
    # First part is the customer name
    customer_name = parts[0]
    
    # Remove all periods from customer name
    customer_name = customer_name.replace('.', '')
    
    # Normalize PTE LTD format
    customer_name = re.sub(r'PTE\s*LTD', 'PTE LTD', customer_name)
    
    # Second part, if exists, is the description
    if len(parts) > 1:
        description = parts[1].strip()
        
        # No need to handle "OTHR-Other" specially anymore, keep as is
    
    # Final cleanup for customer names
    if customer_name.endswith(" PTE"):
        customer_name += " LTD"
    
    return customer_name, description

def process_inward_paynow_transaction(txn_desc):
    """Process Inward PayNow transaction descriptions."""
    customer_name = ''
    description = ''
    
    # Remove the prefix "Inward PayNow from "
    remaining = txn_desc[len("Inward PayNow from "):]
    
    # Look for code patterns that indicate a description
    patterns = ["BEXP-", "IVPT-", "OTHR-", "GDDS-", "SUPP-"]
    desc_start_idx = -1
    
    for pattern in patterns:
        idx = remaining.find(pattern)
        if idx != -1:
            desc_start_idx = idx
            break
    
    if desc_start_idx != -1:
        # Extract customer name and description
        customer_name = remaining[:desc_start_idx].strip()
        description = remaining[desc_start_idx:].strip()
    else:
        # Check for other separators like spaces followed by keywords
        for keyword in ["February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]:
            pattern = f" {keyword}"
            idx = remaining.find(pattern)
            if idx != -1:
                desc_start_idx = idx
                customer_name = remaining[:desc_start_idx].strip()
                description = remaining[desc_start_idx:].strip()
                break
        
        # If still no description found, the entire remaining is the customer name
        if desc_start_idx == -1:
            customer_name = remaining.strip()
    
    # Remove all periods from customer name
    customer_name = customer_name.replace('.', '')
    
    # Normalize PTE LTD format
    customer_name = re.sub(r'PTE\s*LTD', 'PTE LTD', customer_name)
    
    # Special case: do not modify "MOOD COLLECTIVES PTE" by adding LTD
    special_cases = ['MOOD COLLECTIVES PTE']
    if customer_name.endswith(" PTE") and customer_name not in special_cases:
        customer_name += " LTD"
    
    return customer_name, description

def process_giro_credit_transaction(txn_desc):
    """Process Giro Credit transaction descriptions."""
    customer_name = ''
    description = ''
    
    # Remove the prefix "Giro Credit from "
    remaining = txn_desc[len("Giro Credit from "):]
    
    # Look for code patterns that indicate a description
    patterns = ["BEXP-", "IVPT-", "OTHR-", "GDDS-", "SUPP-"]
    desc_start_idx = -1
    
    for pattern in patterns:
        idx = remaining.find(pattern)
        if idx != -1:
            desc_start_idx = idx
            break
    
    if desc_start_idx != -1:
        # Extract customer name and description
        customer_name = remaining[:desc_start_idx].strip()
        description = remaining[desc_start_idx:].strip()
    else:
        # No description found, the entire remaining is the customer name
        customer_name = remaining.strip()
    
    # Remove all periods from customer name
    customer_name = customer_name.replace('.', '')
    
    # Final cleanup for customer names
    if customer_name.endswith(" PTE"):
        customer_name += " LTD"
    
    # Normalize PTE LTD format
    customer_name = re.sub(r'PTE\s*LTD', 'PTE LTD', customer_name)
    
    return customer_name, description

def process_ib_transfer_transaction(txn_desc):
    """Process IB Transfer transaction descriptions."""
    # Remove the prefix "IB Transfer from "
    customer_name = txn_desc[len("IB Transfer from "):].strip()
    description = ''
    
    # Remove all periods from customer name
    customer_name = customer_name.replace('.', '')
    
    # Check for special case where the name includes a P at the end (like D'ZANDER INTERIORS P)
    if customer_name.endswith(" P"):
        # This should likely be PTE LTD but we'll keep it as is per the example
        pass
    # Final cleanup for customer names
    elif customer_name.endswith(" PTE"):
        customer_name += " LTD"
    
    # Normalize PTE LTD format
    customer_name = re.sub(r'PTE\s*LTD', 'PTE LTD', customer_name)
    
    return customer_name, description

def clean_description(description):
    """Clean transaction descriptions."""
    if not description:
        return ""
    
    description = str(description).strip()
    
    # Don't uppercase descriptions anymore - keep as is
    return description

def extract_transaction_info(txn_desc):
    """Extract customer name and description from transaction description."""
    if not txn_desc or pd.isna(txn_desc):
        return '', ''
    
    txn_desc = str(txn_desc).strip()
    
    # Process transactions with separate functions
    if txn_desc.startswith("Inward FAST - "):
        customer_name, description = process_inward_fast_transaction(txn_desc)
    elif txn_desc.startswith("Inward PayNow from "):
        customer_name, description = process_inward_paynow_transaction(txn_desc)
    elif txn_desc.startswith("Giro Credit from "):
        customer_name, description = process_giro_credit_transaction(txn_desc)
    elif txn_desc.startswith("IB Transfer from "):
        customer_name, description = process_ib_transfer_transaction(txn_desc)
    else:
        return '', ''
    
    # Clean up the extracted data but no longer get extra info - simpler approach
    customer_name = customer_name.strip()
    
    # No longer uppercase description - keep as is in the original format
    description = clean_description(description)
    
    return customer_name, description

def process_transactions(input_file, output_file):
    """
    Main function to process bank transactions from CSV and extract customer names and descriptions.
    
    Parameters:
    input_file (str): Path to the input CSV file
    output_file (str): Path to save the output CSV file
    
    Returns:
    bool: True if processing was successful, False otherwise
    """
    try:
        print(f"Processing file: {input_file}")
        
        # Try to detect encoding - sometimes bank CSV files use different encodings
        encodings = ['utf-8', 'latin1', 'iso-8859-1']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(input_file, encoding=encoding)
                print(f"Successfully read file with {encoding} encoding")
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            print("Error: Could not read CSV file with any common encoding")
            return False
        
        # Find transaction description column
        txn_desc_col = find_transaction_description_column(df)
        if not txn_desc_col:
            print("Error: Could not find Description column")
            return False
        
        print(f"Found Description column: {txn_desc_col}")
        
        # Ensure CUSTOMER_NAME and DESCRIPTION columns exist
        if "CUSTOMER_NAME" not in df.columns:
            df["CUSTOMER_NAME"] = ""
        
        if "DESCRIPTION" not in df.columns:
            df["DESCRIPTION"] = ""
        
        # Process each row
        customer_count = 0
        desc_count = 0
        total_rows = 0
        
        for index, row in df.iterrows():
            txn_desc = row[txn_desc_col]
            
            if txn_desc and not pd.isna(txn_desc):
                total_rows += 1
                # Extract customer name and description
                customer_name, description = extract_transaction_info(txn_desc)
                
                # Update DataFrame cells
                if customer_name:
                    df.at[index, "CUSTOMER_NAME"] = customer_name
                    customer_count += 1
                
                if description:
                    df.at[index, "DESCRIPTION"] = description
                    desc_count += 1
        
        # Save the DataFrame to CSV
        df.to_csv(output_file, index=False)
        
        print(f"Processed {total_rows} transactions")
        print(f"Extracted {customer_count} customer names")
        print(f"Extracted {desc_count} descriptions")
        print(f"Saved to {output_file}")
        
        return True
    
    except Exception as e:
        print(f"Error processing transactions: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function to be called by the workflow orchestrator."""
    input_file = Path('data/downloads/new_rows') / 'JOTEX PTE LTD MAYBANK SG 2025.csv'
    output_file = Path('data/temp') / 'SG_MBB_2025_processed.csv'
    
    # Create output directory if it doesn't exist
    os.makedirs(Path('data/temp'), exist_ok=True)
    
    success = process_transactions(input_file, output_file)
    
    if success:
        print("SG MBB transaction parsing completed successfully!")
        return True
    else:
        print("SG MBB transaction parsing failed. Check the error messages above.")
        return False

# Example usage
if __name__ == "__main__":
    main()