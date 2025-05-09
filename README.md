# JotexTest Finance Auto Watch

An automated workflow for processing financial transactions from MBB and PBB banks.

## Project Structure

```
JotexTest_finance_auto_watch/
├── core/                           # Core application functionality
│   ├── workflows.py                # Main workflow orchestrator
│   ├── download_excel_oauth.py     # Excel download functionality
│   ├── create_pymt_mbb.py          # MBB payment creation
│   ├── create_pymt_pbb.py          # PBB payment creation
│   └── upload_to_onedrive.py       # Upload functionality
│
├── parser/                         # Transaction parsers
├── nlp_parser/                     # NLP-based transaction parsers
├── utils/                          # Utility functions
├── train/                          # Training scripts for models
│
├── scripts/                        # Scheduling and automation scripts
├── tests/                          # Test files
├── config/                         # Configuration files
│
├── data/                           # Data directories
│   ├── cache/                      # Cached data
│   ├── training/                   # Training data
│   ├── downloads/                  # Downloaded files
│   ├── output/                     # Output files
│   └── temp/                       # Temporary files
│
├── models/                         # Trained models
├── logs/                           # Log files
```

## Complete Workflow Process

The workflow follows these steps:

1. **Download Excel Files**: Downloads transaction data from SharePoint
2. **Process MBB Transactions**: If new MBB data is available
   - Parse MBB transactions using NLP
   - Create MBB payments in the system
3. **Process PBB Transactions**: If new PBB data is available
   - Parse PBB transactions using NLP
   - Create PBB payments in the system
4. **Upload Results**: Upload processed results to OneDrive

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

## Running Individual Steps for Debugging

If you encounter issues with the workflow, you can run each step individually to identify and fix problems:

### 1. Download Excel Files

```bash
python core/download_excel_oauth.py
```

### 2. Process MBB Transactions

```bash
# Parse MBB transactions
python nlp_parser/mbb_txn_parser_nlp.py

# Create MBB payments
python core/create_pymt_mbb.py
```

### 3. Process PBB Transactions

```bash
# Parse PBB transactions
python nlp_parser/pbb_txn_parser_nlp.py

# Create PBB payments
python core/create_pymt_pbb.py
```

### 4. Upload Results

```bash
python core/upload_to_onedrive.py
```

## Command-line Options

### MBB and PBB Payment Creation

To start processing from a specific row:

```bash
python core/create_pymt_pbb.py --start-row 241
```

```bash
python core/create_pymt_mbb.py --start-row 257
```

To reset progress and start from the beginning:

```bash
python core/create_pymt_pbb.py --reset-progress
```

To display saved progress information:

```bash
python core/create_pymt_pbb.py --show-progress
```

To reset progress but start from a specific row:

```bash
python core/create_pymt_mbb.py --reset-progress --start-row 628
```

```bash
python core/create_pymt_pbb.py --reset-progress --start-row 384
```

## Troubleshooting Common Issues

### No New Rows Detected

If the workflow stops with "No new rows found for both MBB and PBB", this means:

- No changes were detected in the Excel files
- Or the download process couldn't find new transactions

To force processing of existing files:

```bash
# First, manually copy the Excel files to the data/downloads directory
# Then run the parser directly
python nlp_parser/mbb_txn_parser_nlp.py --force
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

If the NLP parser fails to correctly identify transactions:

1. Check the log files in the logs/ directory
2. Run the parser with debug output:

```bash
python nlp_parser/mbb_txn_parser_nlp.py --debug
```

### Payment Creation Errors

If payment creation fails:

1. Check the API credentials in the .env file
2. Verify the customer information in the system
3. Run with verbose logging:

```bash
python core/create_pymt_mbb.py --verbose
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

To test the workflow with simulated data:

```bash
# Test with simulated MBB data
python tests/test_download_with_new_rows.py --bank=mbb

# Test with simulated PBB data
python tests/test_download_with_new_rows.py --bank=pbb

# Test with both banks
python tests/test_download_with_new_rows.py --bank=both
```
