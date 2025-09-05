import os
import pandas as pd
from dateutil import parser
import re
from datetime import datetime
from .date_utils import convert_date

def normalize_columns(df, required_cols):
    """Ensure required columns exist and are string type."""
    for col in required_cols:
        if col not in df.columns:
            df[col] = ''
        df[col] = df[col].astype(str).fillna('')
    return df

def clean_numeric(val):
    """Clean numeric string (remove commas, quotes, handle empty)."""
    if pd.isna(val) or str(val).strip() == '':
        return '0'
    return str(val).strip().strip('"\'').replace(',', '')

def build_payment_payload(journal_id, journal_display_name, customer_info, posting_date, amount, description):
    """Builds the payment payload for BusinessCentralClient."""
    return {
        "journalId": journal_id,
        "journalDisplayName": journal_display_name,
        "customerId": customer_info['customerId'],

        "customerNumber": customer_info['customerNumber'],
        "postingDate": posting_date,
        "amount": amount,
        "description": description
    }

def save_excel(df, csv_file, output_dir="data/output"):
    """Save DataFrame to Excel with _updated suffix."""
    base_name = os.path.splitext(os.path.basename(csv_file))[0]
    output_file = os.path.join(output_dir, f"{base_name}_updated.xlsx")
    os.makedirs(output_dir, exist_ok=True)
    df.to_excel(output_file, index=False)
    return output_file