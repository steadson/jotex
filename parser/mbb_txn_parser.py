import pandas as pd
import re
import os
import warnings
from pathlib import Path

# Suppress warnings
warnings.filterwarnings('ignore', category=UserWarning)


def parse_mbb_txn(file_path, encoding='utf-8'):
    """
    Parses a given CSV file containing MBB transaction data and returns a DataFrame 
    with 'CUSTOMER_NAME' and 'DESCRIPTION' columns.

    Args:
        file_path (str): Path to the CSV file.
        encoding (str, optional): File encoding. Defaults to 'utf-8'.

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

    # Validate required columns exist
    required_columns = ['Transaction Description.1', 'Transaction Description', 
                        'Transaction Ref', 'Posting date']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
    
    # Skip rows with empty "Posting date"
    df = df[pd.notna(df['Posting date'])].copy()
    
    # Extract and clean customer names
    df['CUSTOMER_NAME'] = clean_customer_names(df)
    
    # Process and clean descriptions
    df['DESCRIPTION'] = clean_descriptions(df)

    return df


def clean_customer_names(df):
    """
    Extracts and cleans customer names:
    - If Transaction Description.1 has '*':
        * Use part after '*' if it's non-empty and looks meaningful.
        * Else use the part before '*'.
    - If no '*', or if Transaction Description.1 is missing:
        * Extract all content before any known patterns (dates, numbers, etc).
    - Always strip and clean.
    """
    def extract_name(row):
        desc1 = str(row['Transaction Description.1']) if pd.notna(row['Transaction Description.1']) else ''
        desc2 = str(row['Transaction Description']) if pd.notna(row['Transaction Description']) else ''

        def clean_basic(text):
            # Remove date patterns like (2-APR), (18APR2025), etc.
            text = re.sub(r'\(\s*\d{1,2}[-]?[A-Za-z]{3,}[0-9]*\s*\)', '', text)
            text = re.sub(r'\(\s*\)', '', text)  # Remove empty ()
            return text.strip()

        if desc1 and desc1 != '-':
            parts = desc1.split('*')
            if len(parts) == 2:
                left, right = parts[0].strip(), parts[1].strip()
                # Heuristic: use right side only if it's longer than 2 words or more than 10 characters
                if right and (len(right.split()) >= 2 or len(right) >= 10):
                    return clean_basic(right)
                else:
                    return clean_basic(left)
            else:
                return clean_basic(desc1)
        else:
            # Use fallback
            # Extract uppercase chunks from fallback description
            matches = re.findall(r'\b[A-Z][A-Z\s&.-]*[A-Z]\b', desc2)
            if matches:
                return clean_basic(matches[-1])
            return clean_basic(desc2)

    customer_names = df.apply(extract_name, axis=1)
    
    # Apply all cleaning operations
    customer_names = (customer_names
        .astype(str)
        .str.replace(r'^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+(?:invoice|invoices|inv|payment|receipt)s?\s+', '', regex=True)  # Remove month date invoice prefixes
        .str.replace(r'@\w+', '', regex=True)
        .str.replace(r'-', '', regex=True)
        .str.replace(r'IBG PAYMENT INTO A/C\s*', '', regex=True)
        .str.replace(r'MBB CT\s*', '', regex=True)
        .str.replace(r'\[.*?\]', '', regex=True)
        .str.replace(r'SDN\.', 'SDN', regex=True)
        .str.replace(r'BHD\.', 'BHD', regex=True)
        .str.replace(r'BH\.', 'BH', regex=True)
        .str.replace(r'SDN\s*BH', 'SDN BH', regex=True)
        .str.strip()
    )
    
    return customer_names
    
def extract_invoice_date(text):
    """Extract invoice date prefix from text if it exists."""
    import re
    if not isinstance(text, str):
        return ""
    match = re.search(r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+(?:invoice|invoices|inv|payment|receipt)s?)\s+', text)
    return match.group(1) if match else ""

def clean_descriptions(df):
    """
    Clean and create descriptions from transaction data,
    ensuring CUSTOMER_NAME is removed from the description.
    """
    invoice_dates = df.apply(
        lambda row: extract_invoice_date(str(row['Transaction Description.1'])), 
        axis=1
    )

    # Initial raw description construction
    descriptions = df.apply(
        lambda row: f"{row['Transaction Ref']} {invoice_dates[row.name]} {row['Transaction Description']}".strip(), 
        axis=1
    )

    # Remove CUSTOMER_NAME from description
    descriptions = descriptions.combine(df['CUSTOMER_NAME'], lambda desc, name: desc.replace(name, '').strip())

    # Final cleaning
    descriptions = (descriptions
        .str.replace(r'IBG PAYMENT INTO A/C.*', '', regex=True)
        .str.replace(r'MBB CT-.*', '', regex=True)
        .str.replace(r'-', '', regex=True)
        .str.replace(r'\*', '', regex=True)
        .str.replace(r'Fund transfer', '', regex=True)
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )

    return descriptions


def main():
    """
    Main function to run the MBB transaction parser.
    """
    input_folder = 'downloads/new_rows'
    file_path = Path(input_folder) / 'MBB 2025.csv'
    output_folder = 'OUTPUT'
    os.makedirs(output_folder, exist_ok=True)
    output_file_path = Path(output_folder) / 'MBB_2025_processed.csv'
    
    try:
        # Parse the transactions
        print(f"Processing {file_path}")
        df = parse_mbb_txn(file_path)
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