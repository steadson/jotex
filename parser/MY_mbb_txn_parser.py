import pandas as pd
import re
import os
import warnings
import pickle
from pathlib import Path
import logging
import sys
from fuzzywuzzy import process

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.logger import setup_logging

# Setup logger for MBB parser
logger = setup_logging('MY_mbb_parser')

# Suppress warnings
warnings.filterwarnings('ignore', category=UserWarning)


def predict_clean_name(raw_name, model_data, min_confidence=0.5, fuzzy_threshold=85):
    """
    Predict clean customer name using the trained model.
    
    Args:
        raw_name (str): Raw customer name from transaction
        model_data (dict): Dictionary containing trained model components
        min_confidence (float): Minimum confidence for classifier prediction
        fuzzy_threshold (int): Threshold for fuzzy matching score
        
    Returns:
        str: Clean customer name prediction
    """
    if not raw_name or not isinstance(raw_name, str):
        return raw_name
        
    # Try exact match first (case insensitive)
    if raw_name.lower() in model_data['reference_dict']:
        result = model_data['reference_dict'][raw_name.lower()]
        return format_customer_name(result)
    
    # Try fuzzy matching before using the classifier
    best_match, score = process.extractOne(raw_name.lower(), model_data['training_examples'])
    if score >= fuzzy_threshold:
        result = model_data['reference_dict'][best_match]
        return format_customer_name(result)
    
    # Use the classifier if no good fuzzy match
    X = model_data['vectorizer'].transform([raw_name])
    pred_proba = model_data['classifier'].predict_proba(X)
    
    # Get the highest confidence prediction
    max_prob = pred_proba.max()
    if max_prob >= min_confidence:
        pred_class = model_data['classifier'].predict(X)[0]
        return format_customer_name(pred_class)
    
    # Return basic cleaned version if confidence is too low
    result = basic_clean_customer_name(raw_name)
    return format_customer_name(result)


def extract_additional_info(raw_name, clean_name):
    """
    Extract additional information removed during cleaning.
    
    Args:
        raw_name (str): Original raw customer name
        clean_name (str): Cleaned customer name
    
    Returns:
        str: Additional information that was removed
    """
    if not raw_name or not clean_name or not isinstance(raw_name, str) or not isinstance(clean_name, str):
        return ""
    
    # Extract date patterns
    date_pattern = ""
    date_match = re.search(r'(\d{1,2}[\s-]?[A-Za-z]{3}[\s-]?\d{0,4})', raw_name)
    if date_match:
        date_pattern = date_match.group(1)
    
    # Extract invoice/payment references
    invoice_pattern = ""
    invoice_match = re.search(r'((?:invoice|payment|receipt|bill)s?(?:\s+no\.?\s*\d+)?)', raw_name, re.IGNORECASE)
    if invoice_match:
        invoice_pattern = invoice_match.group(1)
    
    # Extract content in parentheses
    paren_content = ""
    paren_match = re.search(r'\((.*?)\)', raw_name)
    if paren_match and paren_match.group(1) not in clean_name:
        paren_content = f"({paren_match.group(1)})"
    
    # Combine all additional info
    additional_info = " ".join(filter(None, [date_pattern, invoice_pattern, paren_content]))
    
    return additional_info.strip()


def basic_clean_customer_name(text):
    """
    Apply basic cleaning to customer name.
    
    Args:
        text (str): Raw customer name
        
    Returns:
        str: Basic cleaned customer name
    """
    if not text or not isinstance(text, str):
        return text
        
    # Apply basic cleaning operations
    cleaned = text
    cleaned = re.sub(r'^\d+\s*', '', cleaned)                # Remove leading digits
    cleaned = re.sub(r'\*.*$', '', cleaned)                  # Remove content from asterisk onwards
    cleaned = re.sub(r'@\w+', '', cleaned)                   # Remove any "@" and content after it
    cleaned = re.sub(r'-', '', cleaned)                      # Replace "-" with empty space
    cleaned = re.sub(r'IBG PAYMENT INTO A/C\s*', '', cleaned)  # Remove IBG payment text
    cleaned = re.sub(r'MBB CT\s*', '', cleaned)              # Remove MBB CT text
    cleaned = re.sub(r'\(\s*\)', '', cleaned)                # Remove empty parentheses
    cleaned = re.sub(r'\(\s*[0-9]+\s*\)', '', cleaned)       # Remove numbers in parentheses
    cleaned = re.sub(r'\(\s*\d{1,2}[-]?[A-Za-z]{3}[-]?\d*\s*\)', '', cleaned)  # Remove date patterns
    cleaned = re.sub(r'\[.*?\]', '', cleaned)                # Remove content within square brackets
    cleaned = re.sub(r'\s*\/+\s*$', '', cleaned)             # Remove trailing slash with any spaces
    cleaned = re.sub(r'SDN\.', 'SDN', cleaned)               # Replace 'SDN.' with 'SDN'
    cleaned = re.sub(r'BHD\.', 'BHD', cleaned)               # Replace 'BHD.' with 'BHD'
    cleaned = re.sub(r'BH\.', 'BH', cleaned)                 # Replace 'BH.' with 'BH'
    cleaned = re.sub(r'SDN\s*BH', 'SDN BH', cleaned)         # Ensure space between SDN and BH
    cleaned = re.sub(r'SDNBH', 'SDN BH', cleaned)            # Fix instances where space was removed
    cleaned = re.sub(r'SDNBHD', 'SDN BHD', cleaned)          # Fix instances where space was removed
    cleaned = re.sub(r'S/B', '', cleaned)                    # Remove 'S/B'
    cleaned = cleaned.strip()                                # Final whitespace clean
    
    return cleaned


def format_customer_name(name):
    """
    Format customer name by replacing special characters and converting to uppercase.
    
    Args:
        name (str): Customer name to format
        
    Returns:
        str: Formatted customer name
    """
    if not name:
        return name
    name = str(name)
    name = name.replace('É', 'E')
    name = name.replace('&amp;', '&')
    return name.upper()


def parse_mbb_txn(file_path, encoding='utf-8', model_data=None):
    """
    Parses a given CSV file containing MBB transaction data and returns a DataFrame 
    with 'CUSTOMER_NAME' and 'DESCRIPTION' columns.

    Args:
        file_path (str): Path to the CSV file.
        encoding (str, optional): File encoding. Defaults to 'utf-8'.
        model_data (dict, optional): Dictionary containing trained model components.

    Returns:
        pd.DataFrame: DataFrame containing the parsed transaction data.
    """
    logger.info(f"Starting MBB transaction parsing: {file_path}")
    # Check if the file exists
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    # Load the CSV file
    try:
        df = pd.read_csv(file_path, encoding=encoding)
        logger.info(f"Successfully loaded CSV with {len(df)} rows using {encoding} encoding")
    except UnicodeDecodeError:
        logger.warning(f"UTF-8 encoding failed, trying latin1")
        # Try with a different encoding if UTF-8 fails
        try:
            df = pd.read_csv(file_path, encoding='latin1')
            logger.info(f"Successfully loaded CSV with {len(df)} rows using latin1 encoding")
        except Exception as e:
            logger.error(f"Failed to read CSV with alternate encoding: {e}")
            raise ValueError(f"Error reading the CSV file with alternate encoding: {e}")
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise ValueError(f"Error reading the CSV file: {e}")

    # Validate required columns exist
    required_columns = ['Transaction Description.1', 'Transaction Description', 'Transaction Ref', 'Posting date']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
    logger.info(f"All required columns found: {required_columns}")
    # Skip rows with empty "Posting date"
    original_count = len(df)
    df = df[pd.notna(df['Posting date'])].copy()
    filtered_count = len(df)
    logger.info(f"Filtered out {original_count - filtered_count} rows with empty posting dates")
    logger.info(f"Processing {filtered_count} valid transactions")
    
    # Store original transaction descriptions for later use
    df['original_desc1'] = df['Transaction Description.1'].astype(str)
    df['original_desc'] = df['Transaction Description'].astype(str)
    
    # Extract customer name from appropriate column
    df['raw_customer_name'] = df.apply(
        lambda row: row['Transaction Description.1'] 
        if pd.notna(row['Transaction Description.1']) and row['Transaction Description.1'] != '-' 
        else row['Transaction Description'], 
        axis=1
    )
    # Track extraction statistics
    extraction_stats = {
        'total_processed': 0,
        'successful_extractions': 0,
        'empty_extractions': 0,
        'model_used': model_data is not None
    }
    # Apply cleaning to extract the CUSTOMER_NAME
    if model_data is not None:
        logger.info(f"Using ML model for customer name cleaning with {len(model_data['reference_dict'])} references")
        for idx, name in enumerate(df['raw_customer_name']):
            extraction_stats['total_processed'] += 1
            cleaned_name = predict_clean_name(name, model_data)
            df.at[idx, 'CUSTOMER_NAME'] = cleaned_name
            
            if cleaned_name and cleaned_name.strip():
                extraction_stats['successful_extractions'] += 1
                logger.debug(f"Row {idx+1}: '{name}' -> '{cleaned_name}' (ML)")
            else:
                extraction_stats['empty_extractions'] += 1
                logger.debug(f"Row {idx+1}: '{name}' -> [EMPTY] (ML)")
    else:
        logger.info("Using basic cleaning for customer names")
        for idx, name in enumerate(df['raw_customer_name']):
            extraction_stats['total_processed'] += 1
            cleaned_name = format_customer_name(basic_clean_customer_name(name))
            df.at[idx, 'CUSTOMER_NAME'] = cleaned_name
            
            if cleaned_name and cleaned_name.strip():
                extraction_stats['successful_extractions'] += 1
                logger.debug(f"Row {idx+1}: '{name}' -> '{cleaned_name}' (Basic)")
            else:
                extraction_stats['empty_extractions'] += 1
                logger.debug(f"Row {idx+1}: '{name}' -> [EMPTY] (Basic)")
    
    # Log extraction statistics
    logger.info(f"Customer name extraction completed:")
    logger.info(f"  Total processed: {extraction_stats['total_processed']}")
    logger.info(f"  Successful extractions: {extraction_stats['successful_extractions']}")
    logger.info(f"  Empty results: {extraction_stats['empty_extractions']}")
    logger.info(f"  Success rate: {(extraction_stats['successful_extractions']/extraction_stats['total_processed']*100):.1f}%")
    logger.info(f"  Method used: {'ML Model' if extraction_stats['model_used'] else 'Basic Cleaning'}")

    # Extract additional info that was removed during cleaning
    df['additional_info'] = df.apply(
        lambda row: extract_additional_info(row['raw_customer_name'], row['CUSTOMER_NAME']), 
        axis=1
    )
    
    # Create empty descriptions
    df['DESCRIPTION'] = ''
    
    # Drop temporary columns
    df = df.drop(['original_desc1', 'original_desc', 'raw_customer_name', 'additional_info'], axis=1)
    
    return df


def main(input_dir, output_dir):
    """
    Main function to run the MBB transaction parser.
    """

    logger.info(f"=== MBB Transaction Parser Started ===")
    logger.info(f"Input: {input_dir}")
    logger.info(f"Output: {output_dir}")
    # Path to your trained model
    model_path = 'mbb_my_customer_name_model.pkl'
    
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
        print(f"Processing {input_dir}")
        df = parse_mbb_txn(input_dir, model_data=model_data)
        print(f"Processed {len(df)} transactions")
        
        # Save to CSV
        df.to_csv(output_dir, index=False)
        logger.info(f"Successfully saved {len(df)} processed transactions to {output_dir}")
        logger.info(f"=== MBB Transaction Parser Completed Successfully ===")
        print(f"Saved processed data to {output_dir}")
            
    except Exception as e:
        logger.error(f"Parser failed with error: {str(e)}")
        logger.error(f"=== MBB Transaction Parser Failed ===")
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    input_folder = 'data/downloads/new_rows'
    file_path = Path(input_folder) / 'MBB 2025.csv'
    output_folder = 'data/temp'
    os.makedirs(output_folder, exist_ok=True)
    output_file_path = Path(output_folder) / 'MBB_2025_processed.csv'
    main(file_path, output_file_path)