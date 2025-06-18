import os
import logging
import sys
from datetime import datetime

def setup_logging(log_filename):
    current_date = datetime.now().strftime("%d%m%Y_%H%M")
    log_dir = os.path.join('logs', f'{current_date}_{log_filename}.log')
    os.makedirs(os.path.dirname(log_dir), exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(log_dir), logging.StreamHandler(sys.stdout)])
    return logging.getLogger(__name__)
