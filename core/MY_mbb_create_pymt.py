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
        self.stats = {'processed': 0, 'failed': 0}
        self.not_transferred_rows = []

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

        for i, row in df.iterrows():
            if row.get('STATUS') == 'Transferred':
                continue

            name = row.get('CUSTOMER_NAME')
            if pd.isna(name) or not str(name).strip():
                reason = "Missing customer name"
                self.logger.info(f"Skipping row {i+2}: {reason}")
                df.at[i, 'REMARKS'] = reason
                self.stats['failed'] += 1
                continue

            customer_info = bc_client.get_customer_info(name)
            if not customer_info:
                reason = "Customer not found"
                self.logger.info(f"Skipping row {i+2}: {reason} - {name}")
                df.at[i, 'REMARKS'] = reason
                self.stats['failed'] += 1
                continue

            try:
                amount = -abs(float(row['Credit']))
            except Exception as e:
                reason = f"Invalid Credit Amount '{row.get('Credit')}'"
                self.logger.warning(f"Skipping row {i+2}: {reason}, error: {e}")
                df.at[i, 'REMARKS'] = reason
                self.stats['failed'] += 1
                continue

            posting_date = row.get('FormattedDate')
            if not posting_date:
                reason = "Missing formatted date"
                self.logger.warning(f"Skipping row {i+2}: {reason}")
                df.at[i, 'REMARKS'] = reason
                self.stats['failed'] += 1
                continue

            description = str(row.get('DESCRIPTION')).strip() if pd.notna(row.get('DESCRIPTION')) else customer_info.get('customerName', '')
            if not description:
                description = f"Payment from {customer_info.get('customerNumber', 'unknown')}"

            payload = build_payment_payload(
                self.journal_id,
                "MBB",
                customer_info,
                posting_date,
                amount,
                description
            )

            payment_id = bc_client.create_customer_journal_line(payload)
            if payment_id:
                df.at[i, 'STATUS'] = 'Transferred'
                df.at[i, 'payment_ID'] = str(payment_id)
                df.at[i, 'REMARKS'] = ''
                self.stats['processed'] += 1
            else:
                reason = "Payment creation failed"
                df.at[i, 'REMARKS'] = reason
                self.stats['failed'] += 1

        self.save_updated_csv(df)
        self.logger.info(f"Processed: {self.stats['processed']}, Failed: {self.stats['failed']}")


    def save_updated_csv(self, df):
        output_file = save_excel(df, self.csv_file)
        self.logger.info(f"Saved updated Excel: {output_file}")


def main():
    workflow = FinanceWorkflow('data/temp/MBB_2025_processed.csv')
    workflow.process()

if __name__ == '__main__':
    main()
