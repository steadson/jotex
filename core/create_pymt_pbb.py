import os
import sys
import logging
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def setup_logging(log_file='PBB_finance_workflow.log'):
    current_date = datetime.now().strftime("%m%d%Y_%H%M")
    log_file = os.path.join('logs', f'{current_date}_PBB_finance_workflow.log')
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)])
    return logging.getLogger(__name__)

def get_access_token():
    tenant_id = os.getenv('TENANT_ID')
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    if not all([tenant_id, client_id, client_secret]):
        logging.error("Missing OAuth credentials in .env file")
        return None

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://api.businesscentral.dynamics.com/.default'
    }

    try:
        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        return response.json().get('access_token')
    except requests.exceptions.RequestException as e:
        logging.error(f"Error obtaining access token: {e}")
        return None

class FinanceWorkflow:
    def __init__(self, csv_file='data/temp/PBB_2025.csv'):
        self.logger = setup_logging()
        self.csv_file = csv_file
        self.url = os.getenv('BASE_URL')
        self.company_id = os.getenv('COMPANY_ID')
        self.journal_id = os.getenv('PBB_JOURNAL_ID')

        required_env_vars = ['TENANT_ID', 'CLIENT_ID', 'CLIENT_SECRET', 'BASE_URL', 'COMPANY_ID', 'PBB_JOURNAL_ID']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing environment variables: {missing_vars}")

        self.access_token = get_access_token()
        self.stats = {'processed': 0, 'failed': 0}
        self.not_transferred_rows = []

    def convert_date(self, date_string):
        if pd.isna(date_string) or not date_string:
            return None
        try:
            from dateutil import parser
            
            # Convert to string and clean up
            date_str = str(date_string).strip()
            
            # Handle various date formats
            if 'MY (UTC' in date_str:
                date_str = date_str.split('MY')[0].strip()
                
            # Try to parse with flexible parsing
            try:
                return parser.parse(date_str).strftime('%Y-%m-%d')
            except:
                # Try common Malaysian date format: DD/MM/YYYY
                import re
                date_parts = re.findall(r'(\d+)[/\-.](\d+)[/\-.](\d+)', date_str)
                if date_parts:
                    day, month, year = date_parts[0]
                    # Handle 2-digit years
                    if len(year) == 2:
                        year = '20' + year
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                
                return None
        except Exception as e:
            self.logger.debug(f"Date conversion error for '{date_string}': {e}")
            return None

    def read_csv_file(self):
        try:
            # Read the file with quoting to handle quoted numbers correctly
            df = pd.read_csv(self.csv_file, quoting=pd.io.common.csv.QUOTE_MINIMAL)
            
            # Strip spaces from column names
            df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
            self.logger.info(f"CSV columns after stripping spaces: {df.columns.tolist()}")
            
            # Initialize columns if not present but preserve all original columns
            if 'STATUS' not in df.columns:
                df['STATUS'] = ''
            if 'payment_ID' not in df.columns:
                df['payment_ID'] = ''
            
            # Set Posting date from Transaction Date
            df['Posting date'] = df['Transaction Date']

            # Ensure proper types
            df['STATUS'] = df['STATUS'].astype(str)
            df['payment_ID'] = df['payment_ID'].astype(str)
            
            # Process Credit Amount - special handling for quoted values
            # Check for Credit Amount column (with or without spaces)
            credit_amount_col = None
            if 'Credit Amount' in df.columns:
                credit_amount_col = 'Credit Amount'
                
            # Log raw values to debug
            if credit_amount_col:
                self.logger.info(f"Sample Credit Amount values: {df[credit_amount_col].head(5).tolist()}")
                
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
                
                df[credit_amount_col] = df[credit_amount_col].apply(clean_amount)
                
                # Create a standardized 'Credit Amount' column if the original had spaces
                if credit_amount_col != 'Credit Amount':
                    df['Credit Amount'] = df[credit_amount_col]
            
            # Convert dates
            df['FormattedDate'] = df['Transaction Date'].apply(self.convert_date)
            
            self.logger.info(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
            return df
            
        except Exception as e:
            self.logger.error(f"Error with standard CSV reading: {e}")
            
            # More robust fallback with custom CSV parsing
            try:
                import csv
                rows = []
                
                with open(self.csv_file, 'r', newline='') as csvfile:
                    reader = csv.reader(csvfile)
                    headers_raw = next(reader)  # Get header row
                    
                    # Strip spaces from headers
                    headers = [header.strip() if isinstance(header, str) else header for header in headers_raw]
                    self.logger.info(f"Headers after stripping spaces: {headers}")
                    
                    # Find the Credit Amount column index
                    credit_amount_col_index = None
                    credit_amount_col_name = None
                    for i, header in enumerate(headers):
                        if header == 'Credit Amount':
                            credit_amount_col_index = i
                            credit_amount_col_name = header
                            break
                    
                    for row in reader:
                        # Create a dictionary with all columns from the original file
                        row_data = {}
                        for i, header_raw in enumerate(headers_raw):
                            header = headers[i] if i < len(headers) else header_raw  # Use stripped header
                            if i < len(row):
                                value = row[i]
                                # Special handling for Credit Amount
                                if (header == 'Credit Amount' or
                                    (credit_amount_col_index is not None and i == credit_amount_col_index)) and value:
                                    # Remove quotes if present
                                    if (value.startswith('"') and value.endswith('"')) or \
                                       (value.startswith("'") and value.endswith("'")):
                                        value = value[1:-1]
                                    # Don't remove commas here - we'll handle that during conversion
                                row_data[header] = value
                            else:
                                row_data[header] = ''
                        
                        # Add required columns if they don't exist
                        if 'STATUS' not in row_data:
                            row_data['STATUS'] = ''
                        if 'payment_ID' not in row_data:
                            row_data['payment_ID'] = ''
                            
                        rows.append(row_data)
                
                df = pd.DataFrame(rows)
                df['Posting date'] = df['Transaction Date']
                df['FormattedDate'] = df['Transaction Date'].apply(self.convert_date)
                
                self.logger.warning(f"Used manual CSV parsing, processed {len(df)} rows with {len(df.columns)} columns")
                return df
                
            except Exception as e2:
                self.logger.error(f"All CSV reading approaches failed: {e2}")
                raise


    def get_customer_info(self, customer_name):
        if not customer_name:
            return None
        customer_name = str(customer_name).strip().replace('&', '%26')
        if not self.access_token:
            self.access_token = get_access_token()
        headers = {'Authorization': f'Bearer {self.access_token}', 'Content-Type': 'application/json'}
        endpoint = f"{self.url}/companies({self.company_id})/customers?$filter=contains(displayName,'{customer_name}')"
        try:
            response = requests.get(endpoint, headers=headers)
            if response.status_code == 200:
                customers = response.json().get('value', [])
                if customers:
                    return {
                        'customerId': customers[0].get('id'),
                        'customerNumber': customers[0].get('number'),
                        'customerName': customers[0].get('displayName')
                    }
        except Exception as e:
            self.logger.error(f"Customer lookup failed: {e}")
        return None

    def create_payment(self, customer_info, row):
        if not self.access_token:
            self.access_token = get_access_token()

        try:
            # Handle credit amount with better error handling
            amount_str = str(row['Credit Amount']).strip()
            self.logger.debug(f"Processing Credit Amount: '{amount_str}'")
            
            # Skip empty values
            if not amount_str or amount_str == '':
                self.logger.warning("Empty Credit Amount, skipping row")
                self.stats['failed'] += 1
                return None
                
            # Convert to float with better error handling
            try:
                # Remove commas before conversion
                cleaned_amount = amount_str.replace(',', '')
                amount = float(cleaned_amount)
            except ValueError as e:
                self.logger.warning(f"Failed to convert Credit Amount '{amount_str}' to float: {e}")
                self.stats['failed'] += 1
                return None
                
            amount = -abs(amount)  # Ensure negative for payments
            
            self.logger.debug(f"Converted amount: {amount}")
            
        except Exception as e:
            self.logger.warning(f"Invalid amount format: {row.get('Credit Amount', 'N/A')}, error: {e}")
            self.stats['failed'] += 1
            return None

        if not row.get('FormattedDate'):
            # Try to format the date here as a fallback
            try:
                from dateutil import parser
                date_str = str(row.get('Transaction Date', ''))
                if date_str:
                    formatted_date = parser.parse(date_str).strftime('%Y-%m-%d')
                    row['FormattedDate'] = formatted_date
                else:
                    self.logger.warning("Missing transaction date, skipping row")
                    self.stats['failed'] += 1
                    return None
            except Exception as e:
                self.logger.warning(f"Invalid date format: {row.get('Transaction Date', 'N/A')}, error: {e}")
                self.stats['failed'] += 1
                return None

        # Get description, with fallback to customer name
        description = ''
        if pd.notna(row.get('DESCRIPTION')) and str(row.get('DESCRIPTION')).strip():
            description = str(row.get('DESCRIPTION')).strip()
        else:
            description = customer_info.get('customerName', '')
            
        if not description:
            description = f"Payment from {customer_info.get('customerNumber', 'unknown')}"


        payload = {
            "journalId": self.journal_id,
            "journalDisplayName": "PBB",
            "customerId": customer_info['customerId'],
            "customerNumber": customer_info['customerNumber'],
            "postingDate": row['FormattedDate'],
            "amount": amount,
            "description": description
        }

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

        endpoint = f"{self.url}/companies({self.company_id})/customerPayments"

        try:
            response = requests.post(endpoint, headers=headers, json=payload)
            if response.status_code == 201:
                self.stats['processed'] += 1
                return response.json().get('id')
            else:
                self.logger.warning(f"API error {response.status_code}: {response.text}")
                self.stats['failed'] += 1
        except Exception as e:
            self.logger.error(f"Payment creation failed: {e}")
            self.stats['failed'] += 1
        return None

    def process(self):
        df = self.read_csv_file()
        self.logger.info(f"Original columns: {df.columns.tolist()}")
        
        for i, row in df.iterrows():
            if row.get('STATUS') == 'Transferred':
                continue

            name = row.get('CUSTOMER_NAME')
            if pd.isna(name) or not str(name).strip():
                self.logger.info(f"Skipping row {i+2}: Missing customer name")
                self.not_transferred_rows.append(row)
                continue

            customer_info = self.get_customer_info(name)
            if not customer_info:
                self.logger.info(f"Skipping row {i+2}: Customer not found - {name}")
                self.not_transferred_rows.append(row)
                continue

            payment_id = self.create_payment(customer_info, row)
            if payment_id:
                df.at[i, 'STATUS'] = 'Transferred'
                df.at[i, 'payment_ID'] = str(payment_id)

        self.save_updated_csv(df)
        self.save_not_transferred_rows()
        self.logger.info(f"Processed: {self.stats['processed']}, Failed: {self.stats['failed']}")

    def save_updated_csv(self, df):
        # Extract the base filename without path
        base_filename = os.path.basename(self.csv_file)
        base_name = os.path.splitext(base_filename)[0]
        
        # Create the output path in the OUTPUT folder
        excel_updated_file = os.path.join("data/output", f"{base_name}_updated.xlsx")
        
        # Ensure OUTPUT directory exists
        os.makedirs("data/output", exist_ok=True)
        
        # Save the file
        df.to_excel(excel_updated_file, index=False)

        self.logger.info(f"Saved updated EXCEL: {excel_updated_file}")

    def save_not_transferred_rows(self):
        if self.not_transferred_rows:
            not_transferred_df = pd.DataFrame(self.not_transferred_rows)
            
            # Extract the base filename without path
            base_filename = os.path.basename(self.csv_file)
            base_name = os.path.splitext(base_filename)[0]
            
            # Create the output path in the OUTPUT folder
            excel_not_transferred_file = os.path.join("data/output", f"{base_name}_not_transferred.xlsx")
            
            # Ensure OUTPUT directory exists
            os.makedirs("data/output", exist_ok=True)
            
            # Save the file
            not_transferred_df.to_excel(excel_not_transferred_file, index=False)
            
            self.logger.info(f"Saved not transferred rows to {excel_not_transferred_file}")



def main():
    workflow = FinanceWorkflow('data/temp/PBB_2025_processed.csv')
    workflow.process()

if __name__ == '__main__':
    main()