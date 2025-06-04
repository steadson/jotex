import os
import sys
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.access_auth import BusinessCentralAuth
from modules.business_central import BusinessCentralClient
from modules.logger import setup_logging

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
            df = pd.read_csv(self.csv_file)
            df.columns = [col.strip() for col in df.columns]
            if 'STATUS' not in df.columns:
                df['STATUS'] = ''
            if 'payment_ID' not in df.columns:
                df['payment_ID'] = ''
            df['STATUS'] = df['STATUS'].astype(str)
            df['payment_ID'] = df['payment_ID'].astype(str)
            df['Credit'] = df['Credit'].fillna('0').astype(str).str.replace(',', '')
            df['FormattedDate'] = df['Posting date'].apply(self.convert_date)
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

            payload = {
                "journalId": self.journal_id,
                "journalDisplayName": "MBB",
                "customerId": customer_info['customerId'],
                "customerNumber": customer_info['customerNumber'],
                "postingDate": posting_date,
                "amount": amount,
                "description": description
            }

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
        base_name = os.path.splitext(os.path.basename(self.csv_file))[0]
        output_file = os.path.join("data/output", f"{base_name}_updated.xlsx")
        os.makedirs("data/output", exist_ok=True)
        df.to_excel(output_file, index=False)
        self.logger.info(f"Saved updated Excel: {output_file}")


def main():
    workflow = FinanceWorkflow('data/temp/MBB_2025_processed.csv')
    workflow.process()

if __name__ == '__main__':
    main()
