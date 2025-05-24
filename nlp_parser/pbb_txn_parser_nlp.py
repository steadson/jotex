import pandas as pd
import re
import os
import pickle
from pathlib import Path
import warnings

# Suppress warnings
warnings.filterwarnings('ignore', category=UserWarning)

# Global description markers (SO must be followed by numbers only)
DESC_MARKERS = [
    r'PV-', r'SO\d+', r'INV', r'BINVOICE', r'Statement',
    r'Payment for', r'TOP UP', r'paym', r'invoice', r'Sent', r'Jotex', r'Bill', r'PS', r'PO', r'Doc',
    r'PAYMENT', r'PYMT', r'CUS\d+'
]

def find_transaction_description_column(df):
    if "Transaction Description" in df.columns:
        return "Transaction Description"
    for col in df.columns:
        if any(term in str(col).lower() for term in ['description', 'particulars', 'details']):
            return col
    return None

def clean_customer_name(name):
    if not name:
        return '', ''
    name = str(name).strip().rstrip('.')
    name = re.sub(r'SDN\.\s*BHD\.?', 'SDN BHD', name)
    if name.endswith('SB'):
        name = name.replace('SB', '').strip()
    
    # Remove patterns like "XXXXXX2108"
    name = re.sub(r'\b[A-Z]{6}\d{4}\b', '', name).strip()  # Adjust regex as needed

    match = re.search(r'\s+(\d{8,}.*$)', name)
    extra_info = match.group(1).strip() if match else ''
    if match:
        name = name[:match.start()].strip()
    match = re.search(r'\s+([A-Z]{2,3}[-\d]+.*$)', name)
    if match:
        extra_info = (extra_info + ' ' + match.group(1)).strip()
        name = name[:match.start()].strip()
    match = re.search(r'\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{2}.*$', name, flags=re.IGNORECASE)
    if match:
        extra_info = (extra_info + ' ' + match.group(0)).strip()
        name = name[:match.start()].strip()
    match = re.search(r'\s+(20\d{2}).*$', name)
    if match:
        extra_info = (extra_info + ' ' + match.group(1)).strip()
        name = name[:match.start()].strip()
    words = name.split()
    if len(words) > 3:
        company_name = ' '.join(words[:3]).lower()
        remaining = ' '.join(words[3:]).lower()
        if remaining.startswith(company_name):
            extra_info = (extra_info + ' ' + ' '.join(words[3:])).strip()
            name = ' '.join(words[:3])
    return name.strip(), extra_info.strip()


def clean_description(description):
    if not description:
        return ""
    description = str(description).strip()
    if description == "Fund transfer":
        return ""
    elif description.startswith("Fund transfer "):
        description = description[len("Fund transfer "):].strip()
    elif description.startswith("Sent from "):
        parts = description.split(" ", 3)
        if len(parts) >= 4:
            description = parts[3].strip()
        elif len(parts) >= 3:
            description = parts[2].strip()
    return description.upper()

def process_transaction_generic(txn_desc, split_marker):
    parts = txn_desc.split(split_marker)
    if len(parts) <= 1:
        return '', ''
    after_no = parts[1].strip()
    desc_start_idx = len(after_no)
    for marker in DESC_MARKERS:
        match = re.search(marker, after_no)
        if match and match.start() < desc_start_idx:
            desc_start_idx = match.start()
    customer_name = after_no[:desc_start_idx].strip()
    description = after_no[desc_start_idx:].strip() if desc_start_idx < len(after_no) else ''
    customer_name = ' '.join(word for word in customer_name.split() if word.isupper())
    return customer_name, description

def process_duitnow_transaction(txn_desc):
    return process_transaction_generic(txn_desc, 'DUITNOW TRSF CR - NO:')

def process_tsfr_fund_transaction(txn_desc):
    return process_transaction_generic(txn_desc, 'TSFR FUND CR-ATM/EFT - NO:')

def process_dep_ecp_transaction(txn_desc):
    return process_transaction_generic(txn_desc, 'DEP-ECP - NO:')

def process_cheq_transaction(txn_desc):
    cheq_type = 'DEP-LOC CHEQ - NO:' if 'DEP-LOC CHEQ - NO:' in txn_desc else 'DEP-HSE CHEQ - NO:'
    parts = txn_desc.split(cheq_type)
    if len(parts) <= 1:
        return '', ''
    after_no = parts[1].strip()
    asterisk_idx = after_no.find('*')
    if asterisk_idx < 0:
        return '', ''
    after_asterisk = after_no[asterisk_idx + 1:].strip()
    match = re.search(r'^(.*?)(\s+\([^)]+\))$', after_asterisk)
    if match:
        customer_name = match.group(1).strip()
        description = match.group(2).strip()
    else:
        customer_name = after_asterisk
        description = ''
    customer_name = ' '.join(word for word in customer_name.split() if word.isupper())
    return customer_name, description

def predict_customer_name(txn_desc, model_data):
    if not model_data or not txn_desc:
        return ""
    txn_desc = str(txn_desc).lower()
    if txn_desc in model_data['reference_dict']:
        return model_data['reference_dict'][txn_desc]
    try:
        X = model_data['vectorizer'].transform([txn_desc])
        return model_data['classifier'].predict(X)[0]
    except:
        return ""

def extract_transaction_info(txn_desc, model_data=None):
    if not txn_desc or pd.isna(txn_desc):
        return '', ''
    txn_desc = str(txn_desc)
    customer_name, description = '', ''
    if 'DUITNOW TRSF CR - NO:' in txn_desc:
        customer_name, description = process_duitnow_transaction(txn_desc)
    elif 'TSFR FUND CR-ATM/EFT - NO:' in txn_desc:
        customer_name, description = process_tsfr_fund_transaction(txn_desc)
    elif 'DEP-ECP - NO:' in txn_desc:
        customer_name, description = process_dep_ecp_transaction(txn_desc)
    elif 'DEP-LOC CHEQ - NO:' in txn_desc or 'DEP-HSE CHEQ - NO:' in txn_desc:
        customer_name, description = process_cheq_transaction(txn_desc)
    if not customer_name and model_data:
        customer_name = predict_customer_name(txn_desc, model_data)
    clean_name, extra_info = clean_customer_name(customer_name)
    full_desc = f"{extra_info} {description}".strip() if extra_info or description else ''
    return clean_name, clean_description(full_desc)

def parse_pbb_txn(file_path, encoding='utf-8', model_data=None):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    try:
        df = pd.read_csv(file_path, encoding=encoding)
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding='latin1')
    txn_desc_col = find_transaction_description_column(df)
    if not txn_desc_col:
        raise ValueError("Could not find Transaction Description column")
    print(f"Found Transaction Description column: {txn_desc_col}")
    result_df = df.copy()
    result_df['CUSTOMER_NAME'] = ''
    result_df['DESCRIPTION'] = ''
    customer_count = 0
    desc_count = 0
    rule_based_count = 0
    model_based_count = 0
    for idx, row in df.iterrows():
        txn_desc = row[txn_desc_col]
        if pd.notna(txn_desc):
            customer_name, description = extract_transaction_info(txn_desc, model_data)
            if customer_name:
                result_df.at[idx, 'CUSTOMER_NAME'] = customer_name
                customer_count += 1
                if model_data and customer_name == predict_customer_name(txn_desc, model_data):
                    model_based_count += 1
                else:
                    rule_based_count += 1
            result_df.at[idx, 'DESCRIPTION'] = description if description else ''
            if description:
                desc_count += 1
        else:
            result_df.at[idx, 'DESCRIPTION'] = ''
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
