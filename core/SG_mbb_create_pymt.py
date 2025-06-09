import os
import sys
import logging
import pandas as pd
import csv
import requests
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.access_auth import BusinessCentralAuth
from modules.business_central import BusinessCentralClient
from modules.logger import setup_logging

# Load environment variables
load_dotenv()

# Setup logger
logger = setup_logging('SG_mbb_create_pymt')

# Initialize authentication and client
bc_auth = BusinessCentralAuth(
    tenant_id=os.getenv("TENANT_ID"),
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET")
)

bc_client = BusinessCentralClient(
    url=os.getenv('BASE_URL'),
    company_id=os.getenv('JOTEX_PTE_LTD_COMPANY_ID'),
    access_token=bc_auth.get_access_token(),
    journal_id=os.getenv('JOTEX_PTE_LTD_MBB_JOURNAL_ID'),
    logger=logger
)

class SGMBBWorkflow:
    def __init__(self, csv_file='data/temp/MBB_2025_processed.csv'):
        self.logger = logger
        self.csv_file = csv_file
        self.stats = {'processed': 0, 'failed': 0}
        self.not_transferred_rows = []
        # Add missing journal_id attribute
        self.journal_id = os.getenv('JOTEX_PTE_LTD_MBB_JOURNAL_ID')

    def convert_date(self, date_string):
        if pd.isna(date_string):
            return None
        try:
            from dateutil import parser
            if 'MY (UTC' in str(date_string):
                date_string = str(date_string).split('MY')[0].strip()
            return parser.parse(str(date_string)).strftime('%Y-%m-%d')
        except Exception as e:
            self.logger.warning(f"Date conversion failed for '{date_string}': {e}")
            return None

    def get_customer_info(self, customer_name):
        """
        Get customer information from Business Central
        This method was referenced but not implemented in the original code
        """
        try:
            # This should be implemented based on your BusinessCentralClient methods
            # For now, returning a placeholder structure
            customer_info = bc_client.get_customer_info(customer_name)
            if customer_info:
                return {
                    'customerId': customer_info.get('id'),
                    'customerNumber': customer_info.get('number')
                }
            return None
        except Exception as e:
            self.logger.error(f"Error getting customer info for '{customer_name}': {e}")
            return None

    def read_csv_file(self):
        try:
            # Read the file with quoting to handle quoted numbers correctly
            df = pd.read_csv(self.csv_file, quoting=csv.QUOTE_MINIMAL)
            
            # Strip spaces from column names
            df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
            
            # Log all column names to help with debugging
            self.logger.info(f"CSV columns after stripping spaces: {df.columns.tolist()}")
            
            # Initialize columns if not present
            if 'STATUS' not in df.columns:
                df['STATUS'] = ''
            if 'payment_ID' not in df.columns:
                df['payment_ID'] = ''
            if 'REMARKS' not in df.columns:
                df['REMARKS'] = ''
            
            # Ensure proper types to avoid FutureWarnings
            df['STATUS'] = df['STATUS'].astype(str)
            df['payment_ID'] = df['payment_ID'].astype(str)
            df['REMARKS'] = df.get('REMARKS', '').astype(str)
            
            # Find the credit column
            credit_col = None
            if 'Credit' in df.columns:
                credit_col = 'Credit'
            
            if credit_col:
                self.logger.info(f"Found credit column: '{credit_col}'")
                # Log raw values to debug
                self.logger.info(f"Sample Credit values: {df[credit_col].head(5).tolist()}")
                
                # Function to clean credit amount values
                def clean_amount(val):
                    if pd.isna(val) or val == '':
                        return '0'
                    
                    # Convert to string if not already
                    val_str = str(val).strip()
                    
                    # Remove surrounding quotes if present
                    if (val_str.startswith('"') and val_str.endswith('"')) or \
                       (val_str.startswith("'") and val_str.endswith("'")):
                        val_str = val_str[1:-1]
                    
                    # Remove commas
                    val_str = val_str.replace(',', '')
                    
                    return val_str
                
                # Create a standardized 'Credit' column for consistent access
                df['Credit'] = df[credit_col].apply(clean_amount)
                self.logger.info(f"Processed Credit column successfully")
            else:
                self.logger.warning(f"Credit column not found in CSV. Available columns: {df.columns.tolist()}")
            
            # Convert dates
            if 'Transaction Date' in df.columns:
                df['FormattedDate'] = df['Transaction Date'].apply(self.convert_date)
            else:
                self.logger.error("'Transaction Date' column not found in CSV")
                # Set a default date or handle this case appropriately
                df['FormattedDate'] = None
                
            self.logger.info(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
            return df
            
        except Exception as e:
            self.logger.error(f"Error with standard CSV reading: {e}")
            
            # More robust fallback with custom CSV parsing
            try:
                rows = []
                
                with open(self.csv_file, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    headers_raw = next(reader)  # Get header row
                    
                    # Strip spaces from headers
                    headers = [header.strip() if isinstance(header, str) else header for header in headers_raw]
                    self.logger.info(f"Headers after stripping spaces: {headers}")
                    
                    # Find the credit column index
                    credit_col_index = None
                    credit_col_name = None
                    for i, header in enumerate(headers):
                        if header == 'Credit':
                            credit_col_index = i
                            credit_col_name = header
                            break
                    
                    if credit_col_index is not None:
                        self.logger.info(f"Found credit column in manual parsing: '{credit_col_name}' at index {credit_col_index}")
                    else:
                        self.logger.warning(f"Credit column not found in CSV headers: {headers}")
                    
                    for row in reader:
                        # Create a dictionary with all columns from the original file
                        row_data = {}
                        for i, header_raw in enumerate(headers_raw):
                            header = headers[i] if i < len(headers) else header_raw  # Use stripped header
                            if i < len(row):
                                value = row[i]
                                # Special handling for Credit column
                                if credit_col_index is not None and i == credit_col_index and value:
                                    # Remove quotes if present
                                    if (value.startswith('"') and value.endswith('"')) or \
                                       (value.startswith("'") and value.endswith("'")):
                                        value = value[1:-1]
                                    # Also strip spaces
                                    value = value.strip()
                                row_data[header] = value
                            else:
                                row_data[header] = ''
                        
                        # Add required columns if they don't exist
                        if 'STATUS' not in row_data:
                            row_data['STATUS'] = ''
                        if 'payment_ID' not in row_data:
                            row_data['payment_ID'] = ''
                        if 'REMARKS' not in row_data:
                            row_data['REMARKS'] = ''
                            
                        # Add a standardized Credit column if we found the credit column
                        if credit_col_index is not None and credit_col_name in row_data:
                            # Clean the credit amount
                            credit_val = row_data[credit_col_name]
                            if credit_val:
                                credit_val = credit_val.strip().replace(',', '')
                            row_data['Credit'] = credit_val
                            
                        rows.append(row_data)
                
                df = pd.DataFrame(rows)
                
                # Log sample data for debugging
                if len(df) > 0 and 'Credit' in df.columns:
                    self.logger.info(f"Sample Credit values after manual parsing: {df['Credit'].head(5).tolist()}")
                
                # Convert dates
                if 'Transaction Date' in df.columns:
                    df['FormattedDate'] = df['Transaction Date'].apply(self.convert_date)
                else:
                    df['FormattedDate'] = None
                
                self.logger.warning(f"Used manual CSV parsing, processed {len(df)} rows with {len(df.columns)} columns")
                return df
                
            except Exception as e2:
                self.logger.error(f"All CSV reading approaches failed: {e2}")
                raise

    def process(self):
        try:
            df = self.read_csv_file()
            
            for i, row in df.iterrows():
                try:
                    # Skip already transferred rows
                    if str(row.get('STATUS', '')).strip().lower() == 'transferred':
                        self.logger.info(f"Row {i}: Already transferred, skipping")
                        continue

                    # Get customer name
                    name = row.get('CUSTOMER_NAME')
                    if pd.isna(name) or not str(name).strip():
                        df.at[i, 'REMARKS'] = 'Missing customer name'
                        self.stats['failed'] += 1
                        self.logger.warning(f"Row {i}: Missing customer name")
                        continue

                    # Get customer info
                    customer_info = self.get_customer_info(str(name).strip())
                    if not customer_info:
                        df.at[i, 'REMARKS'] = f"Customer not found - {name}"
                        self.stats['failed'] += 1
                        self.logger.warning(f"Row {i}: Customer not found - {name}")
                        continue

                    # Get amount from Credit column
                    amount = 0
                    if 'Credit' in row and pd.notna(row['Credit']) and str(row['Credit']).strip():
                        try:
                            amount_str = str(row['Credit']).strip().replace(',', '')
                            amount = float(amount_str)
                        except (ValueError, TypeError) as e:
                            df.at[i, 'REMARKS'] = f'Invalid amount format: {row["Credit"]}'
                            self.stats['failed'] += 1
                            self.logger.warning(f"Row {i}: Invalid amount format - {row['Credit']}: {e}")
                            continue
                    else:
                        df.at[i, 'REMARKS'] = 'Missing or invalid amount'
                        self.stats['failed'] += 1
                        self.logger.warning(f"Row {i}: Missing or invalid amount")
                        continue

                    # Create description
                    description = f"Payment for {name}"
                    if 'Description' in row and pd.notna(row['Description']):
                        description = str(row['Description']).strip()

                    # Validate posting date
                    posting_date = row.get('FormattedDate')
                    if not posting_date:
                        df.at[i, 'REMARKS'] = 'Invalid transaction date'
                        self.stats['failed'] += 1
                        self.logger.warning(f"Row {i}: Invalid transaction date")
                        continue

                    # Create payload
                    payload = {
                        "journalId": self.journal_id,
                        "journalDisplayName": "CRJ-MBB",
                        "customerId": customer_info['customerId'],
                        "customerNumber": customer_info['customerNumber'],
                        "postingDate": posting_date,
                        "amount": amount,
                        "description": description
                    }

                    self.logger.info(f"Row {i}: Creating payment for {name}, amount: {amount}")
                    
                    # Create payment
                    payment_id = bc_client.create_customer_journal_line(payload)
                    if payment_id:
                        df.at[i, 'STATUS'] = 'Transferred'
                        df.at[i, 'payment_ID'] = payment_id
                        df.at[i, 'REMARKS'] = 'Successfully transferred'
                        self.stats['processed'] += 1
                        self.logger.info(f"Row {i}: Payment created successfully - ID: {payment_id}")
                    else:
                        df.at[i, 'REMARKS'] = 'Payment creation failed'
                        self.stats['failed'] += 1
                        self.logger.error(f"Row {i}: Payment creation failed")

                except Exception as row_error:
                    df.at[i, 'REMARKS'] = f'Processing error: {str(row_error)}'
                    self.stats['failed'] += 1
                    self.logger.error(f"Row {i}: Processing error - {row_error}")

            # Save results
            self.save_results(df)
            
            # Log final statistics
            self.logger.info(f"Processing completed - Processed: {self.stats['processed']}, Failed: {self.stats['failed']}")
            
        except Exception as e:
            self.logger.error(f"Fatal error in process method: {e}")
            raise

    def save_results(self, df):
        try:
            # Extract the base filename without path
            base_filename = os.path.basename(self.csv_file)
            base_name = os.path.splitext(base_filename)[0]
            
            # Create the output path in the OUTPUT folder
            excel_updated_file = os.path.join("data/output", f"{base_name}_updated.xlsx")
            
            # Ensure OUTPUT directory exists
            os.makedirs("data/output", exist_ok=True)
            
            # Save the file
            df.to_excel(excel_updated_file, index=False)
            
            self.logger.info(f"Saved updated Excel: {excel_updated_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
            raise

def main():
    try:
        workflow = SGMBBWorkflow('data/temp/SG_MBB_2025_processed.csv')
        workflow.process()
        print(f"Processing completed successfully!")
        print(f"Processed: {workflow.stats['processed']}")
        print(f"Failed: {workflow.stats['failed']}")
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        print(f"Execution failed: {e}")

if __name__ == '__main__':
    main()