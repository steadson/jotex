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
    def __init__(self, csv_file='data/temp/MBB_2025_processed.csv'):
        self.csv_file = csv_file
        self.logger = logger
        self.stats = {'processed': 0, 'failed': 0}
        self.journal_id = os.getenv('JOTEX_PTE_LTD_MBB_JOURNAL_ID')


    def read_csv_file(self):
        try:
            df = pd.read_csv(self.csv_file, quoting=csv.QUOTE_MINIMAL)
            df.columns = [col.strip() for col in df.columns]
            df = normalize_columns(df, ['STATUS', 'payment_ID', 'REMARKS'])
            if 'Credit' in df.columns:
                df['Credit'] = df['Credit'].apply(clean_numeric)
                self.logger.info("Processed 'Credit' column successfully")
            else:
                self.logger.warning("Missing 'Credit' column in CSV")
            df['FormattedDate'] = df['Transaction Date'].apply(convert_date) if 'Transaction Date' in df.columns else ''
            self.logger.info(f"Loaded {len(df)} rows from CSV")
            return df
        except Exception as e:
            self.logger.error(f"CSV read failed: {e}")
            raise

    def process(self):
        df = self.read_csv_file()

        for i, row in df.iterrows():
            try:
                if row.get('STATUS', '').strip().lower() == 'transferred':
                    continue

                name = str(row.get('CUSTOMER_NAME', '')).strip()
                if not name:
                    df.at[i, 'REMARKS'] = 'Missing customer name'
                    self.stats['failed'] += 1
                    continue

                customer_info = bc_client.get_customer_info(name)
                if not customer_info:
                    df.at[i, 'REMARKS'] = f"Customer not found - {name}"
                    self.stats['failed'] += 1
                    continue

                credit = clean_numeric(row.get('Credit'))
                try:
                    amount = float(credit)
                except ValueError:
                    df.at[i, 'REMARKS'] = f"Invalid amount format: {credit}"
                    self.stats['failed'] += 1
                    continue

                posting_date = row.get('FormattedDate')
                if not posting_date:
                    df.at[i, 'REMARKS'] = 'Invalid transaction date'
                    self.stats['failed'] += 1
                    continue

                description = str(row.get('Description', f"Payment for {name}")).strip()

                payload = build_payment_payload(
                    self.journal_id,
                    "CRJ-MBB",
                    customer_info,
                    posting_date,
                    amount,
                    description
                )

                payment_id = bc_client.create_customer_journal_line(payload)
                if payment_id:
                    df.at[i, 'STATUS'] = 'Transferred'
                    df.at[i, 'payment_ID'] = payment_id
                    df.at[i, 'REMARKS'] = 'Successfully transferred'
                    self.stats['processed'] += 1
                else:
                    df.at[i, 'REMARKS'] = 'Payment creation failed'
                    self.stats['failed'] += 1

            except Exception as e:
                df.at[i, 'REMARKS'] = f'Processing error: {e}'
                self.stats['failed'] += 1
                self.logger.error(f"Row {i} failed: {e}")

        self.save_results(df)
        self.logger.info(f"Done - Processed: {self.stats['processed']}, Failed: {self.stats['failed']}")

    def save_results(self, df):
        try:
            output_path = save_excel(df, self.csv_file)
            self.logger.info(f"Saved results to {output_path}")
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
