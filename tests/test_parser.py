import os
from pathlib import Path
import pandas as pd
import sys

# Add the project root directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from parser import MY_pbb_txn_parser_nlp
from parser import MY_mbb_txn_parser_nlp, SG_mbb_txn_parser, smarthome_mbb_txn_parser

def my_mbb_parser():
    file_name = 'MBB_2025_raw_data.csv'
    input_dir = Path('data/test_data/input') / file_name
    output_folder = 'data/test_data/output'
    os.makedirs(output_folder, exist_ok=True)
    output_dir = Path(output_folder) / file_name

    MY_mbb_txn_parser_nlp.main(input_dir, output_dir)


if __name__ == "__main__":
    my_mbb_parser()