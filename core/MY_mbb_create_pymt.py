import os
import sys
import logging
import pandas as pd
import argparse
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.access_auth import BusinessCentralAuth
from modules.business_central import BusinessCentralClient
from utils.logger import setup_logging
from utils.payment_utils import normalize_columns, clean_numeric, build_payment_payload, save_excel
from utils.date_utils import convert_date

# Load environment variables
load_dotenv()

# Setup logger
logger = setup_logging('MY_mbb_create_pymt')

# Initialize authentication and client
bc_auth = BusinessCentralAuth(
    tenant_id=os.getenv("TENANT_ID"),
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET")
)

bc_client = BusinessCentralClient(
    url=os.getenv('BASE_URL'),
    company_id=os.getenv('COMPANY_ID'),
    access_token=bc_auth.get_access_token(),
    journal_id=os.getenv('MBB_JOURNAL_ID'),
    logger=logger
)

class FinanceWorkflow:
    def __init__(self, csv_file='data/temp/MBB_2025_processed.csv'):
        self.logger = logger
        self.csv_file = csv_file
        self.journal_id = os.getenv('MBB_JOURNAL_ID')
        self.stats = {'processed': 0, 'failed': 0, 'skipped_transferred':0}

        self.not_transferred_rows = []

        # Log initialization
        self.logger.info(f"=== MBB Payment Creation Started ===")
        self.logger.info(f"Processing file: {csv_file}")
        self.logger.info(f"Journal ID: {self.journal_id}")

    def read_csv_file(self):
        try:
            df = pd.read_csv(self.csv_file)
            df.columns = [col.strip() for col in df.columns]
            df = normalize_columns(df, ['STATUS', 'payment_ID'])
            if 'Credit' in df.columns:
                df['Credit'] = df['Credit'].apply(clean_numeric)
            df['FormattedDate'] = df['Posting date'].apply(convert_date) if 'Posting date' in df.columns else ''
            return df
        except Exception as e:
            self.logger.error(f"Failed to read CSV: {e}")
            raise

    def process(self):
        df = self.read_csv_file()
        if 'REMARKS' not in df.columns:
            df['REMARKS'] = ''

        total_rows = len(df)
        self.logger.info(f"Total rows to process: {total_rows}")
        # Add detailed counters like PBB
        already_transferred = 0
        missing_customer_name = 0
        customer_not_found = 0
        invalid_amount = 0
        missing_date = 0
        for i, row in df.iterrows():
            row_num = i + 2  # Excel row number (1-indexed + header)
            self.logger.debug(f"Processing row {row_num}/{total_rows + 1}")
            
            # Skip already transferred
            if row.get('STATUS') == 'Transferred':
                already_transferred += 1
                self.logger.debug(f"Row {row_num}: Already transferred, skipping")
                self.stats['skipped_transferred'] += 1
                continue

            name = row.get('CUSTOMER_NAME')
            if pd.isna(name) or not str(name).strip():
                reason = "Missing customer name"
                self.logger.info(f"Skipping row {i+2}: {reason}")
                self.logger.warning(f"Row {row_num}: - {reason}")
                df.at[i, 'REMARKS'] = reason
                missing_customer_name += 1
                self.stats['failed'] += 1
                continue


            self.logger.debug(f"Row {row_num}: Looking up customer '{name}'")
            customer_info = bc_client.get_customer_info(name)
            if not customer_info:
                reason = "Customer not found"
                self.logger.info(f"Skipping row {i+2}: {reason} - {name}")
                self.logger.warning(f"Row {row_num}:  - {reason} for '{name}'")
                df.at[i, 'REMARKS'] = reason
                customer_not_found += 1
                self.stats['failed'] += 1
                continue
            self.logger.debug(f"Row {row_num}: Customer found - {customer_info.get('customerNumber', 'N/A')} - {customer_info.get('customerName', 'N/A')}")

            try:
                amount = -abs(float(row['Credit']))
                self.logger.debug(f"Row {row_num}: Amount processed - {amount}")
            except Exception as e:
                reason = f"Invalid Credit Amount '{row.get('Credit')}'"
                self.logger.warning(f"Skipping row {i+2}: {reason}, error: {e}")
                self.logger.warning(f"Row {row_num}:  - {reason}, error: {e}")
                invalid_amount += 1
                df.at[i, 'REMARKS'] = reason
                self.stats['failed'] += 1
                continue

            #Date validation
            posting_date = row.get('FormattedDate')
            if not posting_date:
                reason = "Missing formatted date"
                self.logger.warning(f"Skipping row {i+2}: {reason}")
                self.logger.warning(f"Row {row_num}:  - {reason}")
                df.at[i, 'REMARKS'] = reason
                missing_date += 1
                self.stats['failed'] += 1
                continue
            self.logger.debug(f"Row {row_num}: Posting date - {posting_date}")

            description = str(row.get('DESCRIPTION')).strip() if pd.notna(row.get('DESCRIPTION')) else customer_info.get('customerName', '')
            if not description:
                description = f"Payment from {customer_info.get('customerNumber', 'unknown')}"
            self.logger.debug(f"Row {row_num}: Description - '{description}'")
            payload = build_payment_payload(
                self.journal_id,
                "MBB",
                customer_info,
                posting_date,
                amount,
                description
            )
            self.logger.debug(f"Row {row_num}: Payload built, creating payment...")
            # Create payment with detailed logging
            payment_id = bc_client.create_customer_journal_line(payload)
            if payment_id:
                df.at[i, 'STATUS'] = 'Transferred'
                df.at[i, 'payment_ID'] = str(payment_id)
                df.at[i, 'REMARKS'] = ''
                self.stats['processed'] += 1
                self.logger.info(f"Row {row_num}: SUCCESS - Payment created with ID {payment_id} for {customer_info.get('customerName', name)}")
            else:
                reason = "Payment creation failed"
                df.at[i, 'REMARKS'] = reason
                self.stats['failed'] += 1
                self.logger.error(f"Row {row_num}: FAILED - {reason} for {customer_info.get('customerName', name)}")

        self.save_updated_csv(df)
        # Final statistics
        self.logger.info(f"=== MBB Payment Creation Completed ===")
        self.logger.info(f"Total rows: {total_rows}")
        self.logger.info(f"Already transferred: {already_transferred}")
        self.logger.info(f"Successfully processed: {self.stats['processed']}")
        self.logger.info(f"Failed: {self.stats['failed']}")
        self.logger.info(f"  Missing customer name: {missing_customer_name}")
        self.logger.info(f"  Customer not found: {customer_not_found}")
        self.logger.info(f"  Invalid amount: {invalid_amount}")
        self.logger.info(f"  Missing date: {missing_date}")
        self.logger.info(f"Success rate: {(self.stats['processed']/(total_rows-self.stats['skipped_transferred'])*100):.1f}%" if (total_rows-self.stats['skipped_transferred']) > 0 else "Success rate: 0%")


    def save_updated_csv(self, df):
        output_file = save_excel(df, self.csv_file)
        self.logger.info(f"Saved updated Excel: {output_file}")


def main():
    workflow = FinanceWorkflow('data/temp/MBB_2025_processed.csv')
    workflow.process()

if __name__ == '__main__':
    main()
