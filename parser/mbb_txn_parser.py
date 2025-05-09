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
    Clean and extract customer names from transaction data.
    
    Args:
        df (pd.DataFrame): DataFrame containing transaction data
        
    Returns:
        pd.Series: Cleaned customer names
    """
    # Extract customer name from appropriate column
    customer_names = df.apply(
        lambda row: row['Transaction Description.1'] 
        if pd.notna(row['Transaction Description.1']) and row['Transaction Description.1'] != '-' 
        else row['Transaction Description'], 
        axis=1
    )
    
    # Apply all cleaning operations
    customer_names = (customer_names
        .astype(str)
        .str.replace(r'^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+(?:invoice|invoices|payment|receipt)s?\s+', '', regex=True)  # Remove month date invoice prefixes
        .str.replace(r'^\d+\s*', '', regex=True)           # Remove leading digits
        .str.replace(r'\*.*','', regex=True)             # Remove content from asterisk onwards
        .str.replace(r'@\w+', '', regex=True)              # Remove any "@" and content after it
        .str.replace(r'-', '', regex=True)                 # Replace "-" with empty space
        .str.replace(r'IBG PAYMENT INTO A/C\s*', '', regex=True)  # Remove IBG payment text
        .str.replace(r'MBB CT\s*', '', regex=True)         # Remove MBB CT text
        .str.replace(r'\[.*?\]', '', regex=True)           # Remove content within square brackets
        .str.replace(r'\(\s*\)', '', regex=True)           # Remove only empty parentheses like "()"
        .str.replace(r'\(\s*[0-9]+\s*\)', '', regex=True)  # Remove parentheses containing only numbers like "(123)"
        .str.replace(r'\(\s*\d{1,2}[-]?[A-Za-z]{3}[-]?\d*\s*\)', '', regex=True)  # Remove date patterns like (18-APR) or (18APR)
        .str.replace(r'SDN\.', 'SDN', regex=True)          # Replace 'SDN.' with 'SDN'
        .str.replace(r'BHD\.', 'BHD', regex=True)          # Replace 'BHD.' with 'BHD'
        .str.replace(r'BH\.', 'BH', regex=True)            # Replace 'BH.' with 'BH'
        # Preserve the space between SDN and BH
        .str.replace(r'SDN\s*BH', 'SDN BH', regex=True)    # Ensure space between SDN and BH
        .str.strip()  
    )
    
    return customer_names
    
def extract_invoice_date(text):
    """Extract invoice date prefix from text if it exists."""
    import re
    if not isinstance(text, str):
        return ""
    match = re.search(r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+(?:invoice|invoices|payment|receipt)s?)\s+', text)
    return match.group(1) if match else ""

def clean_descriptions(df):
    """
    Clean and create descriptions from transaction data.
    
    Args:
        df (pd.DataFrame): DataFrame containing transaction data
        
    Returns:
        pd.Series: Cleaned descriptions
    """
    # Extract date invoice patterns from customer names
    invoice_dates = df.apply(
        lambda row: extract_invoice_date(str(row['Transaction Description.1'])), 
        axis=1
    )
    
    # Combine transaction reference, invoice date, and description
    descriptions = df.apply(
        lambda row: f"{row['Transaction Ref']} {invoice_dates[row.name]} {row['Transaction Description']}", 
        axis=1
    )
    
    # Apply all cleaning operations
    descriptions = (descriptions
        .str.replace(r'IBG PAYMENT INTO A/C.*', '', regex=True)  # Remove IBG payment text
        .str.replace(r'MBB CT-.*', '', regex=True)        # Remove MBB CT text
        .str.replace(r'-', '', regex=True)                # Remove "-" characters
        .str.replace(r'\*', '', regex=True)               # Remove "*" characters
        .str.replace(r'Fund transfer', '', regex=True)    # Remove "Fund transfer" text
        .str.replace(r'\s+', ' ', regex=True)             # Replace multiple spaces with a single space
        .str.strip()                                      # Strip whitespace
    )
    
    return descriptions



def clean_descriptions(df):
    """
    Clean and create descriptions from transaction data.
    
    Args:
        df (pd.DataFrame): DataFrame containing transaction data
        
    Returns:
        pd.Series: Cleaned descriptions
    """
    # Combine transaction reference and description
    descriptions = df.apply(
        lambda row: f"{row['Transaction Ref']} {row['Transaction Description']}", 
        axis=1
    )
    
    # Apply all cleaning operations
    descriptions = (descriptions
        .str.replace(r'IBG PAYMENT INTO A/C.*', '', regex=True)  # Remove IBG payment text
        .str.replace(r'MBB CT-.*', '', regex=True)        # Remove MBB CT text
        .str.replace(r'-', '', regex=True)                # Remove "-" characters
        .str.replace(r'\*', '', regex=True)               # Remove "*" characters
        .str.replace(r'Fund transfer', '', regex=True)    # Remove "Fund transfer" text
        .str.strip()                                      # Strip whitespace
    )
    
    return descriptions




def clean_descriptions(df):
    """
    Clean and create descriptions from transaction data.
    
    Args:
        df (pd.DataFrame): DataFrame containing transaction data
        
    Returns:
        pd.Series: Cleaned descriptions
    """
    # Combine transaction reference and description
    descriptions = df.apply(
        lambda row: f"{row['Transaction Ref']} {row['Transaction Description']}", 
        axis=1
    )
    
    # Apply all cleaning operations
    descriptions = (descriptions
        .str.replace(r'IBG PAYMENT INTO A/C.*', '', regex=True)  # Remove IBG payment text
        .str.replace(r'MBB CT-.*', '', regex=True)        # Remove MBB CT text
        .str.replace(r'-', '', regex=True)                # Remove "-" characters
        .str.replace(r'\*', '', regex=True)               # Remove "*" characters
        .str.replace(r'Fund transfer', '', regex=True)    # Remove "Fund transfer" text
        .str.strip()                                      # Strip whitespace
    )
    
    return descriptions


def main():
    """
    Main function to run the MBB transaction parser.
    """
    input_folder = 'downloads'
    file_path = Path(input_folder) / 'new_rows_MBB 2025.csv'
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