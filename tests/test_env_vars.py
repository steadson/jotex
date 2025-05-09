"""
Test script to verify that environment variables are loaded correctly.
Run this script after running run_workflow.bat or run_workflow.sh to check
if the environment variables were loaded properly.
"""

import os
import sys

def check_env_vars():
    """Check if required environment variables are set."""
    required_vars = [
        'CLIENT_ID',
        'CLIENT_SECRET',
        'TENANT_ID',
        'BC_API_URL',
        'BASE_URL',
        'COMPANY_ID',
        'MBB_JOURNAL_ID',
        'PBB_JOURNAL_ID',
        'SITE_URL',
        'REDIRECT_URI'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"ERROR: The following environment variables are missing: {', '.join(missing_vars)}")
        return False
    
    print("SUCCESS: All required environment variables are set.")
    
    # Print the values of some non-sensitive variables for verification
    print("\nEnvironment variable values:")
    print(f"BC_API_URL: {os.environ.get('BC_API_URL')}")
    print(f"SITE_URL: {os.environ.get('SITE_URL')}")
    print(f"REDIRECT_URI: {os.environ.get('REDIRECT_URI')}")
    
    return True

if __name__ == "__main__":
    print("Testing environment variables...")
    if not check_env_vars():
        sys.exit(1)
    sys.exit(0)
