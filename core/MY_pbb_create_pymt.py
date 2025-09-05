import os
import sys
import logging
import pandas as pd
import csv
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.access_auth import BusinessCentralAuth
from modules.business_central import BusinessCentralClient
from utils.payment_utils import normalize_columns, clean_numeric, build_payment_payload, save_excel
from utils.date_utils import convert_date
from utils.logger import setup_logging

# Load environment variables
load_dotenv()

# Initialize logger and auth/client classes
logger = setup_logging('MY_pbb_create_pymt')

bc_auth = BusinessCentralAuth(
    tenant_id=os.getenv("TENANT_ID"),
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET")
)

bc_client = BusinessCentralClient(
    url=os.getenv('BASE_URL'),
    company_id=os.getenv('COMPANY_ID'),
    access_token=bc_auth.get_access_token(),
    journal_id=os.getenv('PBB_JOURNAL_ID'),
    logger=logger
)

class PBBWorkflow:
    def __init__(self, csv_file='data/temp/PBB_2025.csv'):
        self.logger = logger
        self.csv_file = csv_file
        self.journal_id = os.getenv('PBB_JOURNAL_ID')
        self.stats = {'processed': 0, 'failed': 0}
        
        self.logger.info(f"Initializing PBB workflow with file: {csv_file}")
        self.logger.info(f"Using journal ID: {self.journal_id}")

    def read_csv(self):
        self.logger.info(f"Reading CSV file: {self.csv_file}")
        
        try:
            df = pd.read_csv(self.csv_file, quoting=csv.QUOTE_MINIMAL)
            self.logger.info(f"Successfully loaded CSV with {len(df)} rows")
        except Exception as e:
            self.logger.error(f"Failed to read CSV file: {e}")
            raise
        
        df.columns = [col.strip() for col in df.columns]
        self.logger.info(f"Column names after stripping: {list(df.columns)}")
        
        df = normalize_columns(df, ['STATUS', 'payment_ID', 'REMARKS'])
        df['Posting date'] = df.get('Transaction Date')
        df['FormattedDate'] = df['Transaction Date'].apply(convert_date) if 'Transaction Date' in df.columns else ''
        
        if 'Credit Amount' in df.columns:
            df['Credit Amount'] = df['Credit Amount'].apply(clean_numeric)
            self.logger.info("Processed Credit Amount column")
        else:
            self.logger.warning("Credit Amount column not found")
        
        self.logger.info(f"CSV loaded and processed with {len(df)} rows.")
        return df

    def process(self):
        self.logger.info("Starting PBB payment processing")
        
        df = self.read_csv()
        
        already_transferred = 0
        missing_customer_name = 0
        customer_not_found = 0
        invalid_amount = 0
        missing_date = 0
        
        for i, row in df.iterrows():
            self.logger.debug(f"Processing row {i + 1}")
            
            if row.get('STATUS') == 'Transferred':
                already_transferred += 1
                self.logger.debug(f"Row {i + 1}: Already transferred, skipping")
                continue

            customer_name = row.get('CUSTOMER_NAME')
            if not customer_name or pd.isna(customer_name):
                df.at[i, 'REMARKS'] = 'Missing customer name'
                self.stats['failed'] += 1
                missing_customer_name += 1
                self.logger.warning(f"Row {i + 1}: Missing customer name")
                continue

            self.logger.debug(f"Row {i + 1}: Looking up customer '{customer_name}'")
            customer_info = bc_client.get_customer_info(customer_name)
            
            if not customer_info:
                df.at[i, 'REMARKS'] = 'Customer not found'
                self.stats['failed'] += 1
                customer_not_found += 1
                self.logger.warning(f"Row {i + 1}: Customer not found - {customer_name}")
                continue
            
            self.logger.debug(f"Row {i + 1}: Found customer {customer_info.get('customerNumber', 'N/A')}")

            try:
                amount = -abs(float(row.get('Credit Amount')))
                self.logger.debug(f"Row {i + 1}: Processed amount: {amount}")
            except Exception as e:
                df.at[i, 'REMARKS'] = f"Invalid credit amount: {e}"
                self.stats['failed'] += 1
                invalid_amount += 1
                self.logger.error(f"Row {i + 1}: Invalid credit amount - {e}")
                continue

            if not row.get('FormattedDate'):
                df.at[i, 'REMARKS'] = 'Missing formatted date'
                self.stats['failed'] += 1
                missing_date += 1
                self.logger.error(f"Row {i + 1}: Missing formatted date")
                continue

            description = str(row.get('DESCRIPTION')).strip() if pd.notna(row.get('DESCRIPTION')) else customer_info.get('customerName', '')
            if not description:
                description = f"Payment from {customer_info.get('customerNumber', 'unknown')}"
            
            self.logger.debug(f"Row {i + 1}: Building payment payload")
            payload = build_payment_payload(
                self.journal_id,
                "PBB",
                customer_info,
                row.get('FormattedDate'),
                amount,
                description
            )

            self.logger.debug(f"Row {i + 1}: Creating payment in Business Central")
            payment_id = bc_client.create_customer_journal_line(payload)
            
            if payment_id:
                df.at[i, 'STATUS'] = 'Transferred'
                df.at[i, 'payment_ID'] = payment_id
                df.at[i, 'REMARKS'] = ''
                self.stats['processed'] += 1
                self.logger.info(f"Row {i + 1}: Payment created successfully - ID: {payment_id}")
            else:
                df.at[i, 'REMARKS'] = 'Payment creation failed'
                self.stats['failed'] += 1
                self.logger.error(f"Row {i + 1}: Payment creation failed")

        # Log final statistics
        self.logger.info(f"Processing completed:")
        self.logger.info(f"  Total rows: {len(df)}")
        self.logger.info(f"  Already transferred: {already_transferred}")
        self.logger.info(f"  Successfully processed: {self.stats['processed']}")
        self.logger.info(f"  Failed: {self.stats['failed']}")
        self.logger.info(f"  Missing customer name: {missing_customer_name}")
        self.logger.info(f"  Customer not found: {customer_not_found}")
        self.logger.info(f"  Invalid amount: {invalid_amount}")
        self.logger.info(f"  Missing date: {missing_date}")
        
        self.save_results(df)

    def save_results(self, df):
        self.logger.info("Saving results to Excel")
        
        try:
            output_file = save_excel(df, self.csv_file)
            self.logger.info(f"Successfully saved updated Excel to: {output_file}")
            self.logger.info(f"Final stats - Processed: {self.stats['processed']}, Failed: {self.stats['failed']}")
        except Exception as e:
            self.logger.error(f"Failed to save results: {e}")
            raise

def main():
    workflow = PBBWorkflow('data/temp/PBB_2025_processed.csv')
    workflow.process()

if __name__ == '__main__':
    main()
