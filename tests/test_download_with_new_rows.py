"""
Mock download script that simulates the output of download_excel_oauth.py
when there are new rows for MBB but not for PBB.
"""
import os
import pandas as pd
from pathlib import Path

# Create downloads directory if it doesn't exist
os.makedirs("downloads", exist_ok=True)

# Print output similar to the real download script
print("Using existing access token!")
print("File MBB 2025.xlsx has changed since last download.")
print("Found 3 new rows. Saved to 'downloads/new_rows_MBB 2025.csv'")
print("File PBB 2025.xlsx has not changed since last download.")
print("\nProcess completed! Downloaded 3 new rows across all files.")

# Create a mock new rows file for MBB
mbb_new_rows_file = Path("downloads/new_rows_MBB 2025.csv")
df = pd.DataFrame({
    'Transaction Date': ['2025-05-01', '2025-05-02', '2025-05-03'],
    'DESCRIPTION': ['Test transaction 1', 'Test transaction 2', 'Test transaction 3'],
    'Credit': [100.00, 200.00, 300.00],
    'CUSTOMER_NAME': ['Customer A', 'Customer B', 'Customer C']
})
df.to_csv(mbb_new_rows_file, index=False)

print(f"Created mock file: {mbb_new_rows_file}")