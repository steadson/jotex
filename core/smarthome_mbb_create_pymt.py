import os
import sys
import logging
import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.access_auth import BusinessCentralAuth
from modules.business_central import BusinessCentralClient
from utils.logger import setup_logging
from utils.payment_utils import normalize_columns, clean_numeric, convert_date, build_payment_payload, save_excel

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

    # Date conversion now uses utils.payment_utils.convert_date

    def read_csv_file(self):
        try:
            import csv
            df = pd.read_csv(self.csv_file, quoting=csv.QUOTE_MINIMAL, keep_default_na=False)
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

        except Exception as e:
            self.logger.error(f"Error reading CSV: {e}")
            raise

    def process_payment(self, row):
        try:
            customer_name = row.get('CUSTOMER_NAME', '').strip()
            if not customer_name:
                return None, 'Missing customer name'

            customer_info = bc_client.get_customer_info(customer_name)
            if not customer_info:
                return None, 'Customer not found'

            amount_str = clean_numeric(row.get('Credit', ''))
            if not amount_str or amount_str == '0':
                return None, 'Empty or zero amount'

            try:
                amount = -abs(float(amount_str))
            except ValueError:
                return None, f'Invalid amount format: {amount_str}'

            formatted_date = row.get('FormattedDate', '').strip()
            if not formatted_date:
                return None, f'Invalid date format: {row.get("Posting date", "")}'

            description = row.get('DESCRIPTION', '').strip() or customer_info.get('customerName', '')
            if not description:
                description = f"Payment from {customer_info.get('customerNumber', 'unknown')}"

            payment_data = build_payment_payload(
                self.journal_id,
                "MBB",
                customer_info,
                formatted_date,
                amount,
                description
            )

            payment_id = bc_client.create_customer_journal_line(payment_data)
            return (payment_id, 'Successfully transferred') if payment_id else (None, 'Payment creation failed')

        except Exception as e:
            self.logger.error(f"Payment processing failed: {e}")
            return None, f'Processing error: {str(e)}'

    def process(self):
        try:
            df = self.read_csv_file()
            for i, row in df.iterrows():
                if row.get('STATUS', '').strip().lower() == 'transferred':
                    continue
                payment_id, message = self.process_payment(row)
                if payment_id:
                    df.at[i, 'STATUS'] = 'Transferred'
                    df.at[i, 'payment_ID'] = str(payment_id)
                    df.at[i, 'REMARKS'] = message
                    self.stats['processed'] += 1
                else:
                    df.at[i, 'REMARKS'] = message
                    self.not_transferred_rows.append(row.to_dict())
                    self.stats['failed'] += 1
            self.save_results(df)
            self.logger.info(f"Processing completed - Processed: {self.stats['processed']}, Failed: {self.stats['failed']}")
        except Exception as e:
            self.logger.error(f"Fatal error in process method: {e}")
            raise

    def save_results(self, df):
        try:
            excel_updated_file = save_excel(df, self.csv_file)
            self.logger.info(f"Saved updated Excel: {excel_updated_file}")
        except Exception as e:
            self.logger.error(f"Error saving updated Excel: {e}")

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
