import pandas as pd
import re
import os
import pickle
from pathlib import Path
import warnings

# Suppress warnings
warnings.filterwarnings('ignore', category=UserWarning)


def find_transaction_description_column(df):
    """
    Find the column containing transaction descriptions.
    
    Args:
        df (pd.DataFrame): DataFrame containing transaction data
        
    Returns:
        str or None: Name of the transaction description column if found, None otherwise
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
    """
    Clean and standardize customer names.
    
    Args:
        name (str): Raw customer name from transaction
        
    Returns:
        tuple: (clean_name, extra_info) where clean_name is the standardized customer name
               and extra_info is additional information extracted from the name
    """
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
    """
    Clean transaction descriptions.
    
    Args:
        description (str): Raw transaction description
        
    Returns:
        str: Cleaned transaction description
    """
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
    """
    Process DUITNOW transaction descriptions.
    
    Args:
        txn_desc (str): Transaction description
        
    Returns:
        tuple: (customer_name, description) extracted from the transaction
    """
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
    """
    Process TSFR FUND transaction descriptions.
    
    Args:
        txn_desc (str): Transaction description
        
    Returns:
        tuple: (customer_name, description) extracted from the transaction
    """
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
    """
    Process DEP-ECP transaction descriptions.
    
    Args:
        txn_desc (str): Transaction description
        
    Returns:
        tuple: (customer_name, description) extracted from the transaction
    """
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
    """
    Process CHEQ transaction descriptions.
    
    Args:
        txn_desc (str): Transaction description
        
    Returns:
        tuple: (customer_name, description) extracted from the transaction
    """
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


def load_customer_name_model(model_path):
    """
    Load the trained customer name model from a pickle file.
    
    Args:
        model_path (str): Path to the model pickle file
        
    Returns:
        dict or None: Dictionary containing model components if successful, None otherwise
    """
    try:
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
        print(f"Loaded customer name model with {len(model_data['reference_dict'])} reference names")
        return model_data
    except Exception as e:
        print(f"Error loading customer name model: {e}")
        return None


def predict_customer_name(txn_desc, model_data):
    """
    Use the trained model to predict a clean customer name from a transaction description.
    
    Args:
        txn_desc (str): Transaction description
        model_data (dict): Model data including vectorizer, classifier, and reference dict
    
    Returns:
        str: Predicted clean customer name
    """
    if not model_data or not txn_desc:
        return ""
    
    txn_desc = str(txn_desc).lower()
    
    # First try exact match from reference dictionary
    if txn_desc in model_data['reference_dict']:
        return model_data['reference_dict'][txn_desc]
    
    # If no exact match, use the ML model
    try:
        # Transform input using the same vectorizer
        X = model_data['vectorizer'].transform([txn_desc])
        
        # Predict using the classifier
        predicted_name = model_data['classifier'].predict(X)[0]
        
        return predicted_name
    except Exception as e:
        print(f"Error predicting customer name: {e}")
        return ""


def extract_transaction_info(txn_desc, model_data=None):
    """
    Extract customer name and description from transaction description, using NLP model if available.
    
    Args:
        txn_desc (str): Transaction description
        model_data (dict, optional): Dictionary containing trained model components
        
    Returns:
        tuple: (customer_name, description) extracted from the transaction
    """
    if not txn_desc or pd.isna(txn_desc):
        return '', ''
    
    txn_desc = str(txn_desc)
    
    # First try using rule-based methods
    if any(pattern in txn_desc for pattern in [
        'DUITNOW TRSF CR - NO:', 'TSFR FUND CR-ATM/EFT - NO:',
        'DEP-ECP - NO:', 'DEP-LOC CHEQ - NO:', 'DEP-HSE CHEQ - NO:'
    ]):
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
            customer_name, description = '', ''
    else:
        # For unsupported transaction types, set empty values initially
        customer_name, description = '', ''
    
    # If we couldn't extract customer name with rule-based methods, try NLP model
    if not customer_name and model_data:
        customer_name = predict_customer_name(txn_desc, model_data)
    
    # Clean up the extracted data and get extra info
    clean_name, extra_info = clean_customer_name(customer_name)
    
    # Combine extra info with existing description
    if extra_info:
        description = (extra_info + ' ' + description).strip()
    
    description = clean_description(description)
    
    return clean_name, description


def parse_pbb_txn(file_path, encoding='utf-8', model_data=None):
    """
    Parses a given CSV file containing PBB transaction data and returns a DataFrame 
    with 'CUSTOMER_NAME' and 'DESCRIPTION' columns.

    Args:
        file_path (str): Path to the CSV file.
        encoding (str, optional): File encoding. Defaults to 'utf-8'.
        model_data (dict, optional): Dictionary containing trained model components.

    Returns:
        pd.DataFrame: DataFrame containing the parsed transaction data.
    """
    # Check if the file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    # Load the CSV file
    try:
        df = pd.read_csv(file_path, encoding=encoding)
    except UnicodeDecodeError:
        # Try with a different encoding if UTF-8 fails
        try:
            df = pd.read_csv(file_path, encoding='latin1')
        except Exception as e:
            raise ValueError(f"Error reading the CSV file with alternate encoding: {e}")
    except Exception as e:
        raise ValueError(f"Error reading the CSV file: {e}")

    # Find transaction description column
    txn_desc_col = find_transaction_description_column(df)
    
    if not txn_desc_col:
        raise ValueError("Could not find Transaction Description column")
    
    print(f"Found Transaction Description column: {txn_desc_col}")
    
    # Create output dataframe with original data
    result_df = df.copy()
    
    # Add CUSTOMER_NAME and DESCRIPTION columns with explicit string dtype
    if 'CUSTOMER_NAME' not in result_df.columns:
        result_df['CUSTOMER_NAME'] = pd.Series(dtype='str')
    else:
        # Convert existing column to string dtype if it exists
        result_df['CUSTOMER_NAME'] = result_df['CUSTOMER_NAME'].astype('str')
    
    if 'DESCRIPTION' not in result_df.columns:
        result_df['DESCRIPTION'] = pd.Series(dtype='str')
    else:
        # Convert existing column to string dtype if it exists
        result_df['DESCRIPTION'] = result_df['DESCRIPTION'].astype('str')
    
    # Process each row
    customer_count = 0
    desc_count = 0
    rule_based_count = 0
    model_based_count = 0
    
    # Store original transaction descriptions for later use
    result_df['original_desc'] = result_df[txn_desc_col].astype(str)
    
    for idx, row in df.iterrows():
        # Get transaction description
        txn_desc = row[txn_desc_col]
        
        if pd.notna(txn_desc):
            # Extract customer name and description
            customer_name, description = extract_transaction_info(txn_desc, model_data)
            
            # Update the output dataframe
            if customer_name:
                result_df.at[idx, 'CUSTOMER_NAME'] = customer_name
                customer_count += 1
                
                # Track which method was used
                if model_data and customer_name == predict_customer_name(txn_desc, model_data):
                    model_based_count += 1
                else:
                    rule_based_count += 1
            
            if description:
                result_df.at[idx, 'DESCRIPTION'] = description
                desc_count += 1
    
    # Drop temporary columns
    result_df = result_df.drop(['original_desc'], axis=1)
    
    print(f"Processed {len(df)} transactions")
    print(f"Extracted {customer_count} customer names (Rule-based: {rule_based_count}, Model-based: {model_based_count})")
    print(f"Extracted {desc_count} descriptions")
    
    return result_df


def main():
    """
    Main function to run the PBB transaction parser.
    """
    input_folder = 'data/downloads/new_rows'
    file_path = Path(input_folder) / 'PBB 2025.csv'
    output_folder = 'data/temp'
    os.makedirs(output_folder, exist_ok=True)
    output_file_path = Path(output_folder) / 'PBB_2025_processed.csv'
    
    # Path to your trained model
    model_path = 'models/pbb_customer_name_model.pkl'
    
    try:
        # Load trained model if it exists
        model_data = None
        if os.path.exists(model_path):
            try:
                with open(model_path, 'rb') as f:
                    model_data = pickle.load(f)
                print(f"Loaded customer name model with {len(model_data['reference_dict'])} references")
            except Exception as e:
                print(f"Warning: Could not load model: {e}")
        
        # Parse the transactions with ML enhancement
        print(f"Processing {file_path}")
        df = parse_pbb_txn(file_path, model_data=model_data)
        print(f"Processed {len(df)} transactions")
        
        # Save to CSV
        df.to_csv(output_file_path, index=False)
        print(f"Saved processed data to {output_file_path}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
