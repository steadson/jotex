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

    def convert_date(self, date_string):
        if pd.isna(date_string):
            return ""
        try:
            from dateutil import parser
            if 'MY (UTC' in str(date_string):
                date_string = str(date_string).split('MY')[0].strip()
            return parser.parse(str(date_string)).strftime('%Y-%m-%d')
        except Exception as e:
            self.logger.warning(f"Date conversion failed for '{date_string}': {e}")
            return ""

    def read_csv_file(self):
        try:
            import csv
            df = pd.read_csv(self.csv_file, quoting=csv.QUOTE_MINIMAL, keep_default_na=False)
            df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
            self.logger.info(f"CSV columns after stripping spaces: {df.columns.tolist()}")

            for col in ['STATUS', 'payment_ID', 'REMARKS']:
                if col not in df.columns:
                    df[col] = ''
                df[col] = df[col].astype(str).fillna('')

            if 'Credit' in df.columns:
                def clean_amount(val):
                    val_str = str(val).strip().strip('"').strip("'").replace(',', '')
                    return val_str if val_str else '0'

                df['Credit'] = df['Credit'].apply(clean_amount)
                self.logger.info("Processed Credit column successfully")
            else:
                self.logger.warning(f"Credit column not found in CSV. Available columns: {df.columns.tolist()}")

            df['FormattedDate'] = df['Posting date'].apply(self.convert_date) if 'Posting date' in df.columns else ""
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

            amount_str = str(row.get('Credit', '')).strip().replace(',', '')
            if not amount_str or amount_str == '0':
                return None, 'Empty or zero amount'

            try:
                amount = -abs(float(amount_str))
            except ValueError:
                return None, f'Invalid amount format: {amount_str}'

            formatted_date = row.get('FormattedDate', '').strip()
            if not formatted_date:
                try:
                    from dateutil import parser
                    date_str = str(row.get('Posting date', '')).strip()
                    formatted_date = parser.parse(date_str).strftime('%Y-%m-%d') if date_str else ''
                except:
                    return None, f'Invalid date format: {row.get("Posting date", "")}'

            description = row.get('DESCRIPTION', '').strip() or customer_info.get('customerName', '')
            if not description:
                description = f"Payment from {customer_info.get('customerNumber', 'unknown')}"

            payment_data = {
                "journalId": self.journal_id,
                "journalDisplayName": "MBB",
                'customerId': customer_info['customerId'],
                'customerNumber': customer_info['customerNumber'],
                'postingDate': formatted_date,
                'amount': amount,
                'description': description
            }

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
            base_filename = os.path.basename(self.csv_file)
            base_name = os.path.splitext(base_filename)[0]
            excel_updated_file = os.path.join("data/output", f"{base_name}_updated.xlsx")
            os.makedirs("data/output", exist_ok=True)
            df.fillna('').to_excel(excel_updated_file, index=False)
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
