import os
import sys
import pandas as pd
import csv
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.access_auth import BusinessCentralAuth
from modules.business_central import BusinessCentralClient
from modules.logger import setup_logging

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

    def convert_date(self, date_string):
        from dateutil import parser
        import re
        if pd.isna(date_string) or not date_string:
            return None
        try:
            date_str = str(date_string).strip()
            if 'MY (UTC' in date_str:
                date_str = date_str.split('MY')[0].strip()
            return parser.parse(date_str).strftime('%Y-%m-%d')
        except:
            match = re.findall(r'(\d+)[/\-.](\d+)[/\-.](\d+)', date_str)
            if match:
                day, month, year = match[0]
                if len(year) == 2:
                    year = '20' + year
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            return None

    def read_csv(self):
        df = pd.read_csv(self.csv_file, quoting=csv.QUOTE_MINIMAL)
        df.columns = [col.strip() for col in df.columns]
        df['STATUS'] = df.get('STATUS', '').astype(str)
        df['payment_ID'] = df.get('payment_ID', '').astype(str)
        df['REMARKS'] = df.get('REMARKS', '').astype(str)
        df['Posting date'] = df.get('Transaction Date')
        df['FormattedDate'] = df['Transaction Date'].apply(self.convert_date)

        if 'Credit Amount' in df.columns:
            df['Credit Amount'] = df['Credit Amount'].fillna('').apply(
                lambda x: str(x).strip().replace('"', '').replace(',', '') if x else '0'
            )

        self.logger.info(f"CSV loaded with {len(df)} rows.")
        return df

    def process(self):
        df = self.read_csv()
        for i, row in df.iterrows():
            if row.get('STATUS') == 'Transferred':
                continue

            customer_name = row.get('CUSTOMER_NAME')
            if not customer_name or pd.isna(customer_name):
                df.at[i, 'REMARKS'] = 'Missing customer name'
                self.stats['failed'] += 1
                continue

            customer_info = bc_client.get_customer_info(customer_name)
            if not customer_info:
                df.at[i, 'REMARKS'] = 'Customer not found'
                self.stats['failed'] += 1
                continue

            try:
                amount = -abs(float(str(row.get('Credit Amount')).strip()))
            except Exception as e:
                df.at[i, 'REMARKS'] = f"Invalid credit amount: {e}"
                self.stats['failed'] += 1
                continue

            if not row.get('FormattedDate'):
                df.at[i, 'REMARKS'] = 'Missing formatted date'
                self.stats['failed'] += 1
                continue

            description = str(row.get('DESCRIPTION')).strip() if pd.notna(row.get('DESCRIPTION')) else customer_info.get('customerName', '')
            if not description:
                description = f"Payment from {customer_info.get('customerNumber', 'unknown')}"

            payload = {
                "journalId": self.journal_id,
                "journalDisplayName": "PBB",
                "customerId": customer_info['customerId'],
                "customerNumber": customer_info['customerNumber'],
                "postingDate": row['FormattedDate'],
                "amount": amount,
                "description": description
            }

            payment_id = bc_client.create_customer_journal_line(payload)
            if payment_id:
                df.at[i, 'STATUS'] = 'Transferred'
                df.at[i, 'payment_ID'] = payment_id
                df.at[i, 'REMARKS'] = ''
                self.stats['processed'] += 1
            else:
                df.at[i, 'REMARKS'] = 'Payment creation failed'
                self.stats['failed'] += 1

        self.save_results(df)

    def save_results(self, df):
        os.makedirs("data/output", exist_ok=True)
        output_file = os.path.join("data/output", "PBB_2025_updated.xlsx")
        df.to_excel(output_file, index=False)
        self.logger.info(f"Saved updated Excel to: {output_file}")
        self.logger.info(f"Processed: {self.stats['processed']}, Failed: {self.stats['failed']}")

def main():
    workflow = PBBWorkflow('data/temp/PBB_2025_processed.csv')
    workflow.process()

if __name__ == '__main__':
    main()
