import os
import sys
import logging
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv
import csv

# Load environment variables
load_dotenv()

def setup_logging(log_file='JUDAH_MBB_finance_workflow.log'):
    current_date = datetime.now().strftime("%d%m%Y_%H%M")
    log_file = os.path.join('logs', f'{current_date}_JUDAH_MBB_finance_workflow.log')
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
    def __init__(self, csv_file='data/temp/Judah_MBB_2025_processed.csv'):
        self.logger = setup_logging()
        self.csv_file = csv_file
        self.url = os.getenv('BASE_URL')
        self.company_id = os.getenv('JUDAH_COMPANY_ID')
        self.journal_id = os.getenv('JUDAH_MBB_JOURNAL_ID')

        required_env_vars = ['TENANT_ID', 'CLIENT_ID', 'CLIENT_SECRET', 'BASE_URL', 'JUDAH_COMPANY_ID', 'JUDAH_MBB_JOURNAL_ID']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing environment variables: {missing_vars}")

        self.access_token = get_access_token()
        self.stats = {'processed': 0, 'failed': 0}
        self.not_transferred_rows = []

    def convert_date(self, date_string):
        if pd.isna(date_string):
            return None
        try:
            from dateutil import parser
            if 'MY (UTC' in str(date_string):
                date_string = str(date_string).split('MY')[0].strip()
            return parser.parse(str(date_string)).strftime('%Y-%m-%d')
        except Exception:
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

            # Ensure proper types to avoid FutureWarnings
            df['STATUS'] = df['STATUS'].astype(str)
            df['payment_ID'] = df['payment_ID'].astype(str)

            # Clean Credit column
            credit_col = 'Credit' if 'Credit' in df.columns else None
            if credit_col:
                self.logger.info(f"Found credit column: '{credit_col}'")
                self.logger.info(f"Sample Credit values: {df[credit_col].head(5).tolist()}")

                def clean_amount(val):
                    if pd.isna(val) or val == '':
                        return '0'
                    val_str = str(val).strip()
                    if (val_str.startswith('"') and val_str.endswith('"')) or \
                    (val_str.startswith("'") and val_str.endswith("'")):
                        val_str = val_str[1:-1]
                    val_str = val_str.replace(',', '')
                    return val_str

                df['Credit'] = df[credit_col].apply(clean_amount)
                self.logger.info(f"Processed Credit column successfully")
            else:
                self.logger.warning(f"Credit column not found in CSV. Available columns: {df.columns.tolist()}")

            # Clean Debit column
            debit_col = 'DEBIT' if 'DEBIT' in df.columns else None
            if debit_col:
                self.logger.info(f"Found DEBIT column: '{debit_col}'")
                self.logger.info(f"Sample DEBIT values: {df[debit_col].head(5).tolist()}")

                def clean_debit(val):
                    if pd.isna(val) or val == '':
                        return '0'
                    val_str = str(val).strip()

                    # Remove currency symbols like 'RM' and non-numeric characters except dot and minus
                    import re
                    val_str = re.sub(r'[^\d\.-]', '', val_str)

                    try:
                        return str(abs(float(val_str)))
                    except ValueError:
                        self.logger.warning(f"Invalid Debit value encountered: {val}")
                        return '0'

                df['DEBIT'] = df[debit_col].apply(clean_debit)
                self.logger.info("Processed Debit column successfully")
            else:
                self.logger.warning(f"Debit column not found in CSV. Available columns: {df.columns.tolist()}")

            df['FormattedDate'] = df['DATE'].apply(self.convert_date)
            self.logger.info(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
            return df

        except Exception as e:
            self.logger.error(f"Error with standard CSV reading: {e}")
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
                    # Check if the first customer is blocked
                    if customers[0].get('blocked') == "All" and len(customers) > 1:
                        # If first customer is blocked and there's another customer, use the next one
                        self.logger.info(f"First customer {customers[0].get('displayName')} is blocked, using next customer {customers[1].get('displayName')}")
                        return {
                            'customerId': customers[1].get('id'),
                            'customerNumber': customers[1].get('number'),
                            'customerName': customers[1].get('displayName')
                        }
                    else:
                        # Use the first customer as before
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
            # Handle debit amount with better error handling
            amount_str = str(row['DEBIT']).strip()
            self.logger.info(f"Processing Debit Amount: '{amount_str}'")
            
            # Skip empty values
            if not amount_str or amount_str == '':
                self.logger.warning("Empty Debit Amount, skipping row")
                self.stats['failed'] += 1
                return None
                
            # Convert to float with better error handling
            try:
                # Remove commas before conversion
                cleaned_amount = amount_str.replace(',', '')
                amount = float(cleaned_amount)
                self.logger.info(f"Successfully converted '{amount_str}' to {amount}")
            except ValueError as e:
                self.logger.warning(f"Failed to convert Debit Amount '{amount_str}' to float: {e}")
                self.stats['failed'] += 1
                return None
                
            # amount = -abs(amount)  # Ensure negative for payments
            
            self.logger.info(f"Final amount value: {amount}")
            
        except Exception as e:
            self.logger.warning(f"Invalid amount format: {row.get('DEBIT', 'N/A')}, error: {e}")
            self.stats['failed'] += 1
            return None

        if not row.get('FormattedDate'):
            # Try to format the date here as a fallback
            try:
                from dateutil import parser
                date_str = str(row.get('DATE', ''))
                if date_str:
                    formatted_date = parser.parse(date_str).strftime('%Y-%m-%d')
                    row['FormattedDate'] = formatted_date
                else:
                    self.logger.warning("Missing posting date, skipping row")
                    self.stats['failed'] += 1
                    return None
            except Exception as e:
                self.logger.warning(f"Invalid date format: {row.get('DATE', 'N/A')}, error: {e}")
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
            "journalDisplayName": "CRJ-MBB",
            "customerId": customer_info['customerId'],
            "customerNumber": customer_info['customerNumber'],
            "postingDate": row['FormattedDate'],
            "amount": amount
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
        for i, row in df.iterrows():
            if row.get('STATUS') == 'Transferred':
                continue

            name = row.get('CUSTOMER  ACCOUNT')
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
        
        self.logger.info(f"Saved updated Excel: {excel_updated_file}")

    def save_not_transferred_rows(self):
        if self.not_transferred_rows:
            df = pd.DataFrame(self.not_transferred_rows)
            
            # Extract the base filename without path
            base_filename = os.path.basename(self.csv_file)
            base_name = os.path.splitext(base_filename)[0]
            
            # Create the output path in the OUTPUT folder
            excel_not_transferred_file = os.path.join("data/output", f"{base_name}_not_transferred.xlsx")
            
            # Ensure OUTPUT directory exists
            os.makedirs("data/output", exist_ok=True)
            
            # Save the file
            df.to_excel(excel_not_transferred_file, index=False)

            self.logger.info(f"Saved not transferred rows to {excel_not_transferred_file}")



def main():
    workflow = FinanceWorkflow('data/temp/Judah_MBB_2025_processed.csv')
    workflow.process()

if __name__ == '__main__':
    main()
