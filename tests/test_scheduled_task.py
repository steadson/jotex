"""
Test script to verify that scheduled tasks are working correctly.
This script simulates the workflow execution without actually running the real workflow steps.
It can be used to test the scheduled tasks without affecting the real data.
"""

import os
import sys
import logging
import datetime
from pathlib import Path

# Set up logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"test_scheduled_task_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def test_scheduled_task():
    """Test function to verify that scheduled tasks are working correctly."""
    logging.info("Starting test scheduled task")
    print("Starting test scheduled task")
    
    # Log environment information
    logging.info(f"Python version: {sys.version}")
    logging.info(f"Current directory: {os.getcwd()}")
    
    # Check if .env file exists
    if os.path.exists(".env"):
        logging.info(".env file exists")
        print(".env file exists")
    else:
        logging.warning(".env file does not exist")
        print(".env file does not exist")
    
    # Check if environment variables are set
    env_vars = [
        'CLIENT_ID',
        'CLIENT_SECRET',
        'TENANT_ID',
        'BC_API_URL'
    ]
    
    for var in env_vars:
        if os.environ.get(var):
            logging.info(f"Environment variable {var} is set")
            print(f"Environment variable {var} is set")
        else:
            logging.warning(f"Environment variable {var} is not set")
            print(f"Environment variable {var} is not set")
    
    # Simulate workflow steps
    workflow_steps = [
        'download_excel_oauth.py',
        'nlp_parser/mbb_txn_parser_nlp.py', 
        'create_pymt_mbb.py',
        'nlp_parser/pbb_txn_parser_nlp.py',
        'create_pymt_pbb.py',
        'upload_to_onedrive.py'
    ]
    
    for step in workflow_steps:
        logging.info(f"Simulating step: {step}")
        print(f"Simulating step: {step}")
        # Sleep for a short time to simulate execution
        import time
        time.sleep(1)
    
    logging.info("Test scheduled task completed successfully")
    print("Test scheduled task completed successfully")
    print(f"Log file created at: {log_file}")

if __name__ == "__main__":
    try:
        test_scheduled_task()
    except Exception as e:
        logging.error(f"Test scheduled task failed: {str(e)}")
        logging.exception("Exception details:")
        print(f"Test scheduled task failed: {str(e)}")
        sys.exit(1)
    sys.exit(0)
