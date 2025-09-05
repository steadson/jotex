import os
import sys
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.access_auth import BusinessCentralAuth
from modules.business_central import BusinessCentralClient
from utils.logger import setup_logging

# Load environment variables
load_dotenv()

# Setup logger
logger = setup_logging('bc_customer_cache')

def update_bc_customer_cache(company_env_key, cache_file):
    """Update Business Central customer cache (separate from local DB)."""
    try:
        # Initialize authentication and client
        bc_auth = BusinessCentralAuth(
            tenant_id=os.getenv("TENANT_ID"),
            client_id=os.getenv("CLIENT_ID"),
            client_secret=os.getenv("CLIENT_SECRET")
        )
        
        bc_client = BusinessCentralClient(
            url=os.getenv('BASE_URL'),
            company_id=os.getenv(company_env_key),
            access_token=bc_auth.get_access_token(),
            logger=logger
        )
        
        logger.info(f"Updating Business Central customer cache for {company_env_key}")
        
        # Create backup of existing cache
        if os.path.exists(cache_file):
            backup_file = f"{cache_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            os.rename(cache_file, backup_file)
            logger.info(f"Created backup: {backup_file}")
        
        # Export customers from Business Central with contact info
        success = bc_client.export_customers_with_contact_to_csv(cache_file)
        
        if success:
            # Load and verify the exported data
            df = pd.read_csv(cache_file)
            logger.info(f"Successfully cached {len(df)} Business Central customers to {cache_file}")
            logger.info(f"Columns: {list(df.columns)}")
            
            return True
        else:
            logger.error("Failed to update Business Central customer cache")
            return False
            
    except Exception as e:
        logger.error(f"Error during Business Central customer cache update: {e}")
        return False

def main():
    """Main function to update all Business Central customer caches."""
    cache_configs = [
        ('COMPANY_ID', 'data/customer_db/BC_MY_CUSTOMERS.csv'),
        # ('JOTEX_PTE_LTD_COMPANY_ID', 'data/customer_db/BC_SG_CUSTOMERS.csv'),
        # ('SMARTHOME_COMPANY_ID', 'data/customer_db/BC_SMARTHOME_CUSTOMERS.csv')
    ]
    
    success_count = 0
    for company_env_key, cache_file in cache_configs:
        logger.info(f"\n{'='*50}")
        logger.info(f"Updating {cache_file}")
        logger.info(f"{'='*50}")
        
        if update_bc_customer_cache(company_env_key, cache_file):
            success_count += 1
        else:
            logger.error(f"Failed to update {cache_file}")
    
    logger.info(f"\nCache update completed: {success_count}/{len(cache_configs)} caches updated successfully")

if __name__ == "__main__":
    main()