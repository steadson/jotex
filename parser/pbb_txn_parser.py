import pandas as pd
import re
import os
from pathlib import Path
import warnings
warnings.filterwarnings('ignore', category=UserWarning)

def find_transaction_description_column(df):
    """Find the column containing transaction descriptions.
    
    First looks specifically for 'Transaction Description', then tries generic matches.
    """
    # First try the exact column name
    if "Transaction Description" in df.columns:
        return "Transaction Description"
    
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

def clean_description(description):
    """Clean transaction descriptions."""
    if not description:
        return ""
    
    description = str(description).strip()
    
    # Remove or simplify common prefixes
    if description == "Fund transfer":
        return ""
    elif description.startswith("Fund transfer "):
        description = description[len("Fund transfer "):].strip()
    elif description.startswith("Sent from "):
        parts = description.split(" ", 3)  # Split into ["Sent", "from", "source", "rest of text"]
        if len(parts) >= 4:
            description = parts[3].strip()
        elif len(parts) >= 3:
            description = parts[2].strip()
    
    return description.upper()

def process_duitnow_transaction(txn_desc):
    """Process DUITNOW transaction descriptions."""
    customer_name = ''
    description = ''
    
    parts = txn_desc.split('DUITNOW TRSF CR - NO: ')
    if len(parts) <= 1:
        return customer_name, description
        
    after_no = parts[1]
    num_match = re.match(r'^\d+\s+', after_no)
    if not num_match:
        return after_no.strip(), ''
        
    after_number = after_no[len(num_match.group(0)):]
    
    # Handle special case for CUSTOMIND DESIGN
    if 'CUSTOMIND DESIGN' in after_number:
        customer_name = 'CUSTOMIND DESIGN'
        after_name_idx = after_number.find('CUSTOMIND DESIGN') + len('CUSTOMIND DESIGN')
        description = after_number[after_name_idx:].strip()
        return customer_name, description
    
    # Find where description starts using markers
    desc_markers = [
        'Fund transfer', 'PV-', 'SO', 'INV', 'BINVOICE', 'Statement',
        'Payment for', 'TOP UP', 'paym', 'invoice', 'Sent', 'Jotex', 'Bill', 'PS', 'PO', 'Doc'
    ]
    
    desc_start_idx = len(after_number)
    for marker in desc_markers:
        idx = after_number.find(marker)
        if idx > 0 and idx < desc_start_idx:
            desc_start_idx = idx
    
    # Extract customer name and description
    if desc_start_idx < len(after_number):
        customer_name = after_number[:desc_start_idx].strip()
        description = after_number[desc_start_idx:].strip()
    else:
        customer_name = after_number.strip()
        
    return customer_name, description

def process_tsfr_fund_transaction(txn_desc):
    """Process TSFR FUND transaction descriptions."""
    customer_name = ''
    description = ''
    
    parts = txn_desc.split('TSFR FUND CR-ATM/EFT - NO: ')
    if len(parts) <= 1:
        return customer_name, description
        
    after_no = parts[1]
    acc_match = re.match(r'^\d+\s+XXXXXX\d+\s+', after_no)
    if not acc_match:
        return after_no.strip(), ''
        
    after_account = after_no[len(acc_match.group(0)):]
    
    # Find description markers
    desc_markers = [
        'Fund transfer', 'PV-', 'SO', 'INV', 'BINVOICE', 'Statement',
        'Payment for', 'TOP UP', 'paym', 'invoice', 'Sent', 'Jotex'
    ]
    
    desc_start_idx = len(after_account)
    
    # 1. Look for common description markers
    for marker in desc_markers:
        idx = after_account.find(marker)
        if idx > 0 and idx < desc_start_idx:
            desc_start_idx = idx
    
    # 2. Look for numeric patterns (like invoice numbers)
    num_match = re.search(r'\s+\d{4,}', after_account)
    if num_match and num_match.start() < desc_start_idx:
        desc_start_idx = num_match.start()
    
    # 3. Check for short word at the end
    words = after_account.split()
    if len(words) > 1 and len(words[-1]) < 5:
        last_word_idx = after_account.rfind(' ' + words[-1])
        if last_word_idx > 0 and last_word_idx < desc_start_idx:
            desc_start_idx = last_word_idx
    
    # Extract customer name and description
    if desc_start_idx < len(after_account):
        customer_name = after_account[:desc_start_idx].strip()
        description = after_account[desc_start_idx:].strip()
    else:
        customer_name = after_account.strip()
        
    return customer_name, description

def process_dep_ecp_transaction(txn_desc):
    """Process DEP-ECP transaction descriptions."""
    customer_name = ''
    description = ''
    
    parts = txn_desc.split('DEP-ECP - NO: ')
    if len(parts) <= 1:
        return customer_name, description
        
    after_no = parts[1]
    
    # Try pattern matching first
    pattern = r'^\d+\s+IMEPS\d+\s+([A-Z][A-Z0-9\s&.]+?)(?:\s+)(CIM|HLB|MBB|CIMB|RHB|PBB|[0-9])'
    match = re.search(pattern, after_no)
    
    if match:
        customer_name = match.group(1).strip()
        description_start = after_no.find(match.group(1)) + len(match.group(1))
        description = after_no[description_start:].strip()
        return customer_name, description
    
    # Fallback: look for bank indicators
    bank_indicators = ['CIM', 'HLB', 'MBB', 'CIMB', 'RHB', 'PBB']
    desc_start_idx = len(after_no)
    
    for bank in bank_indicators:
        bank_idx = after_no.find(bank)
        if bank_idx > 10 and bank_idx < desc_start_idx:
            desc_start_idx = bank_idx
    
    # Look for invoice numbers
    num_match = re.search(r'\s+\d{4}[\s-]\d{4}', after_no)
    if num_match and num_match.start() < desc_start_idx:
        desc_start_idx = num_match.start()
    
    # Extract based on split point
    if desc_start_idx < len(after_no) and desc_start_idx > 10:
        customer_name = after_no[:desc_start_idx].strip()
        description = after_no[desc_start_idx:].strip()
        return customer_name, description
    
    # Look for capital letters pattern as last resort
    words = after_no.split()
    capital_word_idx = -1
    
    for idx, word in enumerate(words):
        if len(word) > 2 and word.isupper() and idx > 1:
            capital_word_idx = idx
            break
    
    if capital_word_idx >= 0:
        company_end_idx = -1
        for idx in range(capital_word_idx + 1, len(words)):
            if not words[idx].isupper() or any(bank in words[idx] for bank in bank_indicators):
                company_end_idx = idx
                break
        
        if company_end_idx > 0:
            customer_name = ' '.join(words[capital_word_idx:company_end_idx])
            description = ' '.join(words[company_end_idx:])
        else:
            customer_name = words[capital_word_idx]
            description = ' '.join(words[capital_word_idx+1:])
    
    return customer_name, description

def process_cheq_transaction(txn_desc):
    """Process CHEQ transaction descriptions."""
    customer_name = ''
    description = ''
    
    # Determine the cheque type
    cheq_type = 'DEP-LOC CHEQ - NO:' if 'DEP-LOC CHEQ - NO:' in txn_desc else 'DEP-HSE CHEQ - NO:'
    
    parts = txn_desc.split(cheq_type)
    if len(parts) <= 1:
        return customer_name, description
    
    after_no = parts[1].strip()
    
    # Look for asterisk
    asterisk_idx = after_no.find('*')
    if asterisk_idx < 0:
        return customer_name, description
    
    after_asterisk = after_no[asterisk_idx + 1:].strip()
    
    # Check for date in parentheses
    parentheses_match = re.search(r'^(.*?)(\s+\([^)]+\))$', after_asterisk)
    if parentheses_match:
        customer_name = parentheses_match.group(1).strip()
        description = parentheses_match.group(2).strip()
    else:
        customer_name = after_asterisk
        description = ""
    
    return customer_name, description

def extract_transaction_info(txn_desc):
    """Extract customer name and description from transaction description."""
    if not txn_desc or pd.isna(txn_desc):
        return '', ''
    
    txn_desc = str(txn_desc)
    
    # Process different transaction types
    if 'DUITNOW TRSF CR - NO:' in txn_desc:
        customer_name, description = process_duitnow_transaction(txn_desc)
    elif 'TSFR FUND CR-ATM/EFT - NO:' in txn_desc:
        customer_name, description = process_tsfr_fund_transaction(txn_desc)
    elif 'DEP-ECP - NO:' in txn_desc:
        customer_name, description = process_dep_ecp_transaction(txn_desc)
    elif 'DEP-LOC CHEQ - NO:' in txn_desc or 'DEP-HSE CHEQ - NO:' in txn_desc:
        customer_name, description = process_cheq_transaction(txn_desc)
    else:
        return '', ''
    
    # Clean up the extracted data and get extra info
    clean_name, extra_info = clean_customer_name(customer_name)
    
    # Combine extra info with existing description
    if extra_info:
        description = (extra_info + ' ' + description).strip()
    
    description = clean_description(description)
    
    return clean_name, description

def process_transactions(input_file, output_file, encoding='utf-8'):
    """
    Main function to process bank transactions and extract customer names and descriptions.
    
    Parameters:
    input_file (str): Path to the input CSV file
    output_file (str): Path to save the output CSV file
    encoding (str): Encoding to use when reading the CSV file
    
    Returns:
    bool: True if processing was successful, False otherwise
    """
    try:
        print(f"Processing file: {input_file}")
        
        # Try to read the CSV file with the specified encoding
        try:
            df = pd.read_csv(input_file, encoding=encoding)
        except UnicodeDecodeError:
            # Try with a different encoding if the specified one fails
            print(f"Failed to read with {encoding} encoding, trying latin1...")
            df = pd.read_csv(input_file, encoding='latin1')
        
        # Find transaction description column
        txn_desc_col = find_transaction_description_column(df)
        
        if not txn_desc_col:
            print("Error: Could not find Transaction Description column")
            return False
        
        print(f"Found Transaction Description column: {txn_desc_col}")
        
        # Create output dataframe with original data
        result_df = df.copy()
        
        # Add CUSTOMER_NAME and DESCRIPTION columns if they don't exist
        if 'CUSTOMER_NAME' not in result_df.columns:
            result_df['CUSTOMER_NAME'] = ''
        
        if 'DESCRIPTION' not in result_df.columns:
            result_df['DESCRIPTION'] = ''
        
        # Process each row
        customer_count = 0
        desc_count = 0
        
        for idx, row in df.iterrows():
            # Get transaction description
            txn_desc = row[txn_desc_col]
            
            if pd.notna(txn_desc):
                # Extract customer name and description
                customer_name, description = extract_transaction_info(txn_desc)
                
                # Update the output dataframe
                if customer_name:
                    result_df.at[idx, 'CUSTOMER_NAME'] = customer_name
                    customer_count += 1
                
                if description:
                    result_df.at[idx, 'DESCRIPTION'] = description
                    desc_count += 1
        
        # Save to CSV
        result_df.to_csv(output_file, index=False, encoding=encoding)
        
        print(f"Processed {len(df)} transactions")
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
    input_folder = 'downloads/new_rows'
    input_file = Path(input_folder) / 'PBB 2025.csv'
    output_folder = 'OUTPUT'
    os.makedirs(output_folder, exist_ok=True)
    output_file = Path(output_folder) / 'PBB_2025_processed.csv'
    
    success = process_transactions(input_file, output_file)
    
    if success:
        print("Processing completed successfully!")
    else:
        print("Processing failed. Check the error messages above.")

if __name__ == "__main__":
    main()