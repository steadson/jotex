# JOTEX Finance Auto Watch

An automated workflow for processing financial transactions from multiple banks including Malaysian MBB, PBB, Singapore MBB, and Smarthome MBB.

## Project Structure

```
JotexTest_finance_auto_watch/
├── core/                           # Core application functionality
│   ├── workflows.py                # Main workflow orchestrator
│   ├── download_excel_oauth.py     # Excel download functionality
│   ├── MY_mbb_create_pymt.py       # Malaysian MBB payment creation
│   ├── MY_pbb_create_pymt.py       # Malaysian PBB payment creation
│   ├── SG_mbb_create_pymt.py       # Singapore MBB payment creation
│   ├── smarthome_mbb_create_pymt.py # Smarthome MBB payment creation
│   └── upload_to_onedrive.py       # Upload functionality
│
├── parser/                         # Transaction parsers
│   ├── MY_mbb_txn_parser.py        # Malaysian MBB transaction parser
│   ├── MY_pbb_txn_parser.py        # Malaysian PBB transaction parser
│   ├── sg_mbb_txn_parser.py        # Singapore MBB transaction parser
│   └── smarthome_mbb_txn_parser.py # Smarthome MBB transaction parser
│
├── utils/                          # Utility functions
│   ├── update_customer_name.py     # Customer name standardization
│   ├── filter_utils.py             # Data filtering utilities
│   ├── cleanup_utils.py            # File cleanup utilities
│   └── ...                         # Other utility modules
├── train/                          # Training scripts for models
│
├── scripts/                        # Scheduling and automation scripts
├── tests/                          # Test files
├── config/                         # Configuration files
│
├── data/                           # Data directories
│   ├── cache/                      # Cached data
│   ├── customer_db/                # Customer database files
│   ├── downloads/                  # Downloaded files
│   │   └── new_rows/              # Filtered transaction files
│   ├── output/                     # Output files
│   ├── output_backups/            # Backup files
│   └── temp/                       # Temporary processed files
│
├── models/                         # Trained models
├── logs/                           # Log files
```

## Complete Workflow Process

The automated workflow processes transactions from multiple bank sources with the following steps:

### 1. Download & Filter (`core/download_excel_oauth.py`)

- Downloads transaction data from SharePoint
- Filters rows where CUSTOMER_NAME is empty (removes rows with existing customer names)

### 2. Process Each Bank Type (runs in parallel for available files)

**Malaysian MBB (MBB 2025.csv)**:

- Parse transactions → `parser/MY_mbb_txn_parser.py`
- Update customer names → `utils/update_customer_name.py` (using `MY_MBB_CUSTOMER_NAME.csv`)
- Create payments → `core/MY_mbb_create_pymt.py`

**Malaysian PBB (PBB 2025.csv)**:

- Parse transactions → `parser/MY_pbb_txn_parser.py`
- Update customer names → `utils/update_customer_name.py` (using `MY_PBB_CUSTOMER_NAME.csv`)
- Create payments → `core/MY_pbb_create_pymt.py`

**Singapore MBB (JOTEX PTE LTD MAYBANK SG 2025.csv)**:

- Parse transactions → `parser/sg_mbb_txn_parser.py`
- Update customer names → `utils/update_customer_name.py` (using `SG_MBB_customer_name.csv`)
- Create payments → `core/SG_mbb_create_pymt.py`

**Smarthome MBB (Smarthome MBB 2025.csv)**:

- Parse transactions → `parser/smarthome_mbb_txn_parser.py`
- Update customer names → `utils/update_customer_name.py` (using `MY_MBB_CUSTOMER_NAME.csv`)
- Create payments → `core/smarthome_mbb_create_pymt.py`

### 3. Upload Results (`core/upload_to_onedrive.py`)

- Uploads processed results to OneDrive in [TEMP](https://jotexmalaysia-my.sharepoint.com/my?id=%2Fpersonal%2Fstock%5Fjotexfabrics%5Fcom%2FDocuments%2FFINANCE%2FTEMP) folder

### 4. Cleanup (`utils/cleanup_utils.py`)

- Deletes processed files from `data/downloads/new_rows` and `data/temp` directories

## Running the Complete Workflow

### On Windows:

```bash
# From the project root directory
.\scripts\run_workflow.bat
```

### On Linux/Mac:

```bash
# From the project root directory
chmod +x ./scripts/run_workflow.sh  # Make sure it's executable
./scripts/run_workflow.sh
```

### Direct Python Execution:

```bash
# Run the complete workflow
python core/workflows.py
```

## Running Individual Steps for Debugging

If you encounter issues with the workflow, you can run each step individually to identify and fix problems:

### 1. Download Excel Files

```bash
python core/download_excel_oauth.py
```

### 2. Process Malaysian MBB Transactions

```bash
# Parse MBB transactions
python parser/MY_mbb_txn_parser.py

# Update customer names (optional - also runs in workflow)
python utils/update_customer_name.py

# Create MBB payments
python core/MY_mbb_create_pymt.py
```

### 3. Process Malaysian PBB Transactions

```bash
# Parse PBB transactions
python parser/MY_pbb_txn_parser.py

# Update customer names (optional - also runs in workflow)
python utils/update_customer_name.py

# Create PBB payments
python core/MY_pbb_create_pymt.py
```

### 4. Process Singapore MBB Transactions

```bash
# Parse Singapore MBB transactions
python parser/sg_mbb_txn_parser.py

# Update customer names (optional - also runs in workflow)
python utils/update_customer_name.py

# Create Singapore MBB payments
python core/SG_mbb_create_pymt.py
```

### 5. Process Smarthome MBB Transactions

```bash
# Parse Smarthome MBB transactions
python parser/smarthome_mbb_txn_parser.py

# Update customer names (optional - also runs in workflow)
python utils/update_customer_name.py

# Create Smarthome MBB payments
python core/smarthome_mbb_create_pymt.py
```

### 6. Upload Results

```bash
python core/upload_to_onedrive.py
```

## Command-line Options

### Payment Creation Command-line Options

Each payment creation script supports various command-line options for debugging and resuming processing:

**Malaysian MBB**:

```bash
python core/MY_mbb_create_pymt.py --start-row 257
python core/MY_mbb_create_pymt.py --reset-progress
python core/MY_mbb_create_pymt.py --show-progress
python core/MY_mbb_create_pymt.py --reset-progress --start-row 628
```

**Malaysian PBB**:

```bash
python core/MY_pbb_create_pymt.py --start-row 241
python core/MY_pbb_create_pymt.py --reset-progress
python core/MY_pbb_create_pymt.py --show-progress
python core/MY_pbb_create_pymt.py --reset-progress --start-row 384
```

**Singapore MBB**:

```bash
python core/SG_mbb_create_pymt.py --start-row 100
python core/SG_mbb_create_pymt.py --reset-progress
python core/SG_mbb_create_pymt.py --show-progress
```

**Smarthome MBB**:

```bash
python core/smarthome_mbb_create_pymt.py --start-row 50
python core/smarthome_mbb_create_pymt.py --reset-progress
python core/smarthome_mbb_create_pymt.py --show-progress
```

## Troubleshooting Common Issues

### No New Rows Detected

If the workflow stops with "No new rows found", this means:

- No changes were detected in the Excel files
- Or the download process couldn't find new transactions
- Files exist but have no rows with empty CUSTOMER_NAME fields

To force processing of existing files:

```bash
# First, manually copy the Excel files to the data/downloads/new_rows directory
# Then run the parser directly
python parser/MY_mbb_txn_parser.py
python parser/MY_pbb_txn_parser.py
python parser/sg_mbb_txn_parser.py
python parser/smarthome_mbb_txn_parser.py
```

### Authentication Issues

If you encounter authentication errors with SharePoint/OneDrive:

```bash
# Reset the authentication token
rm config/ms_token.json
# Then run the download script to re-authenticate
python core/download_excel_oauth.py
```

### Parser Errors

If the transaction parsers fail to correctly identify transactions:

1. Check the log files in the logs/ directory
2. Run the parsers individually to debug:

```bash
python parser/MY_mbb_txn_parser.py
python parser/MY_pbb_txn_parser.py
python parser/sg_mbb_txn_parser.py
python parser/smarthome_mbb_txn_parser.py
```

### Customer Name Update Issues

If customer name matching fails or produces unexpected results:

1. Check the customer database files in `data/customer_db/`:

   - `MY_MBB_CUSTOMER_NAME.csv`
   - `MY_PBB_CUSTOMER_NAME.csv`
   - `SG_MBB_customer_name.csv`

2. Run the customer name update manually:

```bash
python utils/update_customer_name.py
```

3. Adjust similarity threshold in the script if needed (default: 0.95)

### Payment Creation Errors

If payment creation fails:

1. Check the API credentials in the .env file
2. Verify the customer information in the system
3. Run with verbose logging:

```bash
python core/MY_mbb_create_pymt.py --verbose
python core/MY_pbb_create_pymt.py --verbose
python core/SG_mbb_create_pymt.py --verbose
python core/smarthome_mbb_create_pymt.py --verbose
```

### Sheet Name Configuration Issues

If you encounter errors related to sheet names or month detection:

1. **Verify sheet name format** in `config/files_config.json`:

   - Must be exactly `"MMM'YY"` format (e.g., "Aug'25")
   - Month must be 3-letter abbreviation with first letter capitalized
   - Year must be 2-digit with apostrophe

2. **Check Excel file sheet names**:

   - Open your Excel files and verify the actual sheet names
   - Ensure they match exactly with what's in your config file

3. **Test month detection**:

```bash
python utils/month_config.py  # Shows current month being used
```

4. **Common solutions**:
   - Use the month config utility: `python utils/month_config.py Sep`
   - Manually edit config file to match Excel sheet names exactly
   - Ensure consistent formatting across all files

## Logging

All logs are stored in the `logs/` directory with timestamps. To increase logging verbosity:

```bash
# Set environment variable for debug logging
export LOG_LEVEL=DEBUG  # On Linux/Mac
set LOG_LEVEL=DEBUG     # On Windows CMD
$env:LOG_LEVEL="DEBUG"  # On Windows PowerShell

# Then run the workflow or individual script
```

## Date Utilities

The `utils/date_utils.py` module provides intelligent date parsing that automatically detects the correct date format based on your month configuration:

### Features:

- **Automatic Month Detection**: Reads month from `config/files_config.json`
- **Smart Format Detection**: Handles YYYY-MM-DD, YYYY-DD-MM, DD/MM/YYYY, MM/DD/YYYY
- **Fallback Parsing**: Uses dateutil for complex date strings
- **UTC Suffix Removal**: Automatically removes "MY (UTC..." suffixes

### Usage:

```python
from utils.date_utils import convert_date, get_current_month

# Convert date (automatically uses month from config)
date = convert_date("2025-15-08")  # Returns "2025-08-15"

# Get current month being used
current_month = get_current_month()  # Returns 8 for August
```

## Testing

To test the workflow and individual components:

```bash
# Test the complete workflow
python tests/test_scheduled_task.py

# Test with simulated download data
python tests/test_download_with_new_rows.py

# Test customer name updates
python tests/test_update_customer_name.py

# Test file filtering
python tests/test_filter_empty_rows.py

# Test date utilities
python tests/test_date_utils.py
```

## Month Configuration

The system automatically detects the month for date parsing from your `config/files_config.json` file. This makes it easy to switch between months without editing code.

**⚠️ Important Note:** Currently, the sheet names in `config/files_config.json` need to be manually updated when you receive new Excel files with different month sheets. The system cannot automatically detect the actual sheet names from your Excel files.

### Current Month Detection:

- The system reads the `sheet_name` field from your config (e.g., "Aug'25")
- Automatically converts month names to numbers (Aug → 8)
- Uses this for intelligent date format detection in `utils/date_utils.py`

### Sheet Name Format Requirements:

The `sheet_name` field must follow this exact format:

- **Month**: 3-letter abbreviation (Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec)
- **Year**: 2-digit year with apostrophe (e.g., '25 for 2025)
- **Format**: `"MMM'YY"` (e.g., "Aug'25", "Sep'25", "Oct'25")

**Examples of valid sheet names:**

- `"Aug'25"` ✅ (August 2025)
- `"Sep'25"` ✅ (September 2025)
- `"Oct'25"` ✅ (October 2025)
- `"Jan'26"` ✅ (January 2026)

**⚠️ Common mistakes to avoid:**

- `"August 25"` ❌ (full month name)
- `"Aug 25"` ❌ (missing apostrophe)
- `"Aug'2025"` ❌ (4-digit year)
- `"aug'25"` ❌ (lowercase month)

### Easy Month Changes:

**Change to September:**

```bash
python utils/month_config.py Sep
```

**Change to October:**

```bash
python utils/month_config.py 10
```

**Change to specific month and year:**

```bash
python utils/month_config.py Sep 26  # September 2026
```

**View current configuration:**

```bash
python utils/month_config.py
```

### What Happens:

1. Updates all sheet names in `config/files_config.json`
2. `utils/date_utils.py` automatically uses the new month
3. No need to edit Python files manually
4. All date parsing functions work with the new month

### Manual Configuration Updates:

When you receive new Excel files with different month sheets, you need to manually update the `sheet_name` fields in `config/files_config.json`:

**Option 1: Use the month config utility (Recommended)**

```bash
python utils/month_config.py Sep  # For September
python utils/month_config.py Oct  # For October
```

**Option 2: Edit config file manually**

```json
{
  "files_to_download": [
    {
      "name": "MBB 2025.xlsx",
      "sheet_name": "Sep'25"  # Change this to match your Excel sheet
    }
  ]
}
```

**⚠️ Important:** Always ensure the sheet name exactly matches the actual sheet name in your Excel file, including the exact format (MMM'YY).

## Customer Name Update Feature

The workflow now includes automatic customer name standardization using fuzzy matching:

### How it works:

1. After transaction parsing, the system compares raw customer names against curated customer databases
2. Uses similarity matching (default threshold: 95%) to find the best match
3. Replaces raw bank names with standardized customer names
4. Each bank type uses its specific customer database:
   - Malaysian MBB → `MY_MBB_CUSTOMER_NAME.csv`
   - Malaysian PBB → `MY_PBB_CUSTOMER_NAME.csv`
   - Singapore MBB → `SG_MBB_customer_name.csv`
   - Smarthome MBB → `MY_MBB_CUSTOMER_NAME.csv`

### Customer Database Format:

```csv
CUSTOMER NAME,SPECIAL NAME BANK IN
STANDARDIZED COMPANY NAME,RAW BANK TRANSACTION NAME
```

### Manual Customer Name Update:

```bash
# Update all available processed files
python utils/update_customer_name.py

# The script automatically detects file types and uses appropriate databases
```
