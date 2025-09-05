import os
import sys
import logging
import pandas as pd
from dotenv import load_dotenv
import csv
# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.access_auth import BusinessCentralAuth
from modules.business_central import BusinessCentralClient
from utils.payment_utils import normalize_columns, clean_numeric, build_payment_payload, save_excel
from utils.date_utils import convert_date
from utils.logger import setup_logging

# Load environment variables
load_dotenv()

# Setup logger
logger = setup_logging('Smarthome_create_pymt')

# Initialize authentication and client
bc_auth = BusinessCentralAuth(
    tenant_id=os.getenv("TENANT_ID"),
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET")
)

bc_client = BusinessCentralClient(
    url=os.getenv('BASE_URL'),
    company_id=os.getenv('SMARTHOME_COMPANY_ID'),
    access_token=bc_auth.get_access_token(),
    journal_id=os.getenv('SMARTHOME_MBB_JOURNAL_ID'),
    logger=logger
)

class SmarthomeFinanceWorkflow:
    def __init__(self, csv_file='data/temp/Smarthome_MBB_2025_processed.csv'):
        self.logger = logger
        self.csv_file = csv_file
        self.journal_id = os.getenv('SMARTHOME_MBB_JOURNAL_ID')
        self.stats = {'processed': 0, 'failed': 0}
        self.not_transferred_rows = []
        
        self.logger.info(f"Initializing Smarthome workflow with file: {csv_file}")
        self.logger.info(f"Using journal ID: {self.journal_id}")
    

    def read_csv_file(self):
        self.logger.info(f"Reading CSV file: {self.csv_file}")
        
        try:
            
            df = pd.read_csv(self.csv_file, quoting=csv.QUOTE_MINIMAL, keep_default_na=False)
            self.logger.info(f"Successfully loaded CSV with {len(df)} rows")
        except Exception as e:
            self.logger.error(f"Error reading CSV: {e}")
            raise
        
        df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
        self.logger.info(f"CSV columns after stripping spaces: {df.columns.tolist()}")

        df = normalize_columns(df, ['STATUS', 'payment_ID', 'REMARKS'])
        
        if 'Credit' in df.columns:
            df['Credit'] = df['Credit'].apply(clean_numeric)
            self.logger.info("Processed Credit column successfully")
        else:
            self.logger.warning(f"Credit column not found in CSV. Available columns: {df.columns.tolist()}")

        df['FormattedDate'] = df['Posting date'].apply(convert_date) if 'Posting date' in df.columns else ""
        self.logger.info(f"Successfully read CSV with {len(df)} rows and {len(df.columns)} columns")
        
        return df

    def process(self):
        self.logger.info("Starting Smarthome payment processing")
        already_transferred = 0
        missing_customer_name = 0
        customer_not_found = 0
        invalid_amount = 0
        invalid_date = 0
        df = self.read_csv_file()
        for i, row in df.iterrows():
            try:
                self.logger.debug(f"Processing row {i + 1}")
                
                if row.get('STATUS', '').strip().lower() == 'transferred':
                    already_transferred += 1
                    self.logger.debug(f"Row {i + 1}: Already transferred, skipping")
                    continue

                name = str(row.get('CUSTOMER_NAME', '')).strip()
                if not name:
                    df.at[i, 'REMARKS'] = 'Missing customer name'
                    self.stats['failed'] += 1
                    missing_customer_name += 1
                    self.logger.warning(f"Row {i + 1}: Missing customer name")
                    continue

                self.logger.debug(f"Row {i + 1}: Looking up customer '{name}'")
                customer_info = bc_client.get_customer_info(name)
            
                if not customer_info:
                    df.at[i, 'REMARKS'] = f"Customer not found - {name}"
                    self.stats['failed'] += 1
                    customer_not_found += 1
                    self.logger.warning(f"Row {i + 1}: Customer not found - {name}")
                    continue
                
                self.logger.debug(f"Row {i + 1}: Found customer {customer_info.get('customerNumber', 'N/A')}")

                credit = clean_numeric(row.get('Credit'))
                try:
                    amount = -abs(float(credit))
                    self.logger.debug(f"Row {i + 1}: Processed amount: {amount}")
                except ValueError:
                    df.at[i, 'REMARKS'] = f"Invalid amount format: {credit}"
                    self.stats['failed'] += 1
                    invalid_amount += 1
                    self.logger.error(f"Row {i + 1}: Invalid amount format: {credit}")
                    continue

                posting_date = row.get('FormattedDate')
                if not posting_date:
                    df.at[i, 'REMARKS'] = 'Invalid transaction date'
                    self.stats['failed'] += 1
                    invalid_date += 1
                    self.logger.error(f"Row {i + 1}: Invalid transaction date")
                    continue

                description = str(row.get('DESCRIPTION', f"Payment for {name}")).strip()
                
                self.logger.debug(f"Row {i + 1}: Building payment payload")
                payload = build_payment_payload(
                    self.journal_id,
                    "MBB",
                    customer_info,
                    posting_date,
                    amount,
                    description
                )

                self.logger.debug(f"Row {i + 1}: Creating payment in Business Central")
                payment_id = bc_client.create_customer_journal_line(payload)
                
                if payment_id:
                    df.at[i, 'STATUS'] = 'Transferred'
                    df.at[i, 'payment_ID'] = payment_id
                    df.at[i, 'REMARKS'] = 'Successfully transferred'
                    self.stats['processed'] += 1
                    self.logger.info(f"Row {i + 1}: Payment created successfully - ID: {payment_id}")
                else:
                    df.at[i, 'REMARKS'] = 'Payment creation failed'
                    self.stats['failed'] += 1
                    self.logger.error(f"Row {i + 1}: Payment creation failed")

            except Exception as e:
                df.at[i, 'REMARKS'] = f'Processing error: {e}'
                self.stats['failed'] += 1
                self.logger.error(f"Row {i + 1} failed: {e}")
        
        # Log final statistics
        self.logger.info(f"Processing completed:")
        self.logger.info(f"  Total rows: {len(df)}")
        self.logger.info(f"  Already transferred: {already_transferred}")
        self.logger.info(f"  Successfully processed: {self.stats['processed']}")
        self.logger.info(f"  Failed: {self.stats['failed']}")
        self.logger.info(f"  Missing customer name: {missing_customer_name}")
        self.logger.info(f"  Customer not found: {customer_not_found}")
        self.logger.info(f"  Invalid amount: {invalid_amount}")
        self.logger.info(f"  Invalid date: {invalid_date}")

        self.save_results(df)
        self.logger.info(f"Done - Processed: {self.stats['processed']}, Failed: {self.stats['failed']}")
    def save_results(self, df):
        self.logger.info("Saving results to Excel")
        
        try:
            output_path = save_excel(df, self.csv_file)
            self.logger.info(f"Successfully saved results to {output_path}")
        except Exception as e:
            self.logger.error(f"Failed to save results: {e}")
            raise

def main():
    try:
        workflow = SmarthomeFinanceWorkflow('data/temp/Smarthome_MBB_2025_processed.csv')
        workflow.process()
        logger.info("Processing completed successfully!")
        logger.info(f"Processed: {workflow.stats['processed']}")
        logger.info(f"Failed: {workflow.stats['failed']}")
    except Exception as e:
        logger.error(f"Main execution failed: {e}")

if __name__ == '__main__':
    main()
