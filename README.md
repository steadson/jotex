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

## Logging

All logs are stored in the `logs/` directory with timestamps. To increase logging verbosity:

```bash
# Set environment variable for debug logging
export LOG_LEVEL=DEBUG  # On Linux/Mac
set LOG_LEVEL=DEBUG     # On Windows CMD
$env:LOG_LEVEL="DEBUG"  # On Windows PowerShell

# Then run the workflow or individual script
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
