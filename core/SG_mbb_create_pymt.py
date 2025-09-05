import os
import sys
import logging
import pandas as pd
import csv
from dotenv import load_dotenv

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.access_auth import BusinessCentralAuth
from modules.business_central import BusinessCentralClient
from utils.payment_utils import normalize_columns, clean_numeric, build_payment_payload, save_excel
from utils.date_utils import convert_date
from utils.logger import setup_logging

load_dotenv()
logger = setup_logging('SG_mbb_create_pymt')

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

# --- Helper Functions ---
# Use utils.payment_utils.clean_numeric and convert_date instead of local helpers

# --- Workflow Class ---
class SGMBBWorkflow:
    def __init__(self, csv_file='data/temp/SG_MBB_2025_processed.csv'):
        self.csv_file = csv_file
        self.logger = logger
        self.stats = {'processed': 0, 'failed': 0}
        self.journal_id = os.getenv('JOTEX_PTE_LTD_MBB_JOURNAL_ID')
        
        self.logger.info(f"Initializing SG MBB workflow with file: {csv_file}")
        self.logger.info(f"Using journal ID: {self.journal_id}")

    def read_csv_file(self):
        self.logger.info(f"Reading CSV file: {self.csv_file}")
        
        try:
            df = pd.read_csv(self.csv_file, quoting=csv.QUOTE_MINIMAL)
            self.logger.info(f"Successfully loaded CSV with {len(df)} rows")
        except Exception as e:
            self.logger.error(f"CSV read failed: {e}")
            raise
        
        df.columns = [col.strip() for col in df.columns]
        self.logger.info(f"Column names after stripping: {list(df.columns)}")
        
        df = normalize_columns(df, ['STATUS', 'payment_ID', 'REMARKS'])
        
        if 'Credit' in df.columns:
            df['Credit'] = df['Credit'].apply(clean_numeric)
            self.logger.info("Processed 'Credit' column successfully")
        else:
            self.logger.warning("Missing 'Credit' column in CSV")
        
        df['FormattedDate'] = df['Transaction Date'].apply(convert_date) if 'Transaction Date' in df.columns else ''
        self.logger.info(f"Loaded {len(df)} rows from CSV")
        
        return df

    def process(self):
        self.logger.info("Starting SG MBB payment processing")
        
        df = self.read_csv_file()
        
        already_transferred = 0
        missing_customer_name = 0
        customer_not_found = 0
        invalid_amount = 0
        invalid_date = 0
        
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
                    amount = float(credit)
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

                description = str(row.get('Description', f"Payment for {name}")).strip()
                
                self.logger.debug(f"Row {i + 1}: Building payment payload")
                payload = build_payment_payload(
                    self.journal_id,
                    "CRJ-MBB",
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

# --- Entry Point ---
def main():
    try:
        workflow = SGMBBWorkflow('data/temp/SG_MBB_2025_processed.csv')
        workflow.process()
        print(f"Processing completed successfully!\nProcessed: {workflow.stats['processed']}\nFailed: {workflow.stats['failed']}")
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        print(f"Execution failed: {e}")

if __name__ == '__main__':
    main()
