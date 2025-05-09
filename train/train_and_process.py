import os
import argparse
import sys
import pandas as pd
from pathlib import Path

# Add parent directory to path to import modules
sys.path.append(str(Path(__file__).parent.parent))

# Import the improved training and processing modules
from train.improved_train_customer_model import train_customer_name_model
from nlp_parser.improved_pbb_txn_parser_nlp import process_transactions

def main():
    parser = argparse.ArgumentParser(description='Train improved model and process transactions')
    parser.add_argument('--training_data', default='data_training/pbb_my_customer_name_training_data.csv',
                        help='Path to training data CSV')
    parser.add_argument('--input_file', default='cache/PBB 2025.xlsx.cached.csv',
                        help='Path to input transaction CSV file')
    parser.add_argument('--output_file', default='OUTPUT/PBB_2025_processed_improved.csv',
                        help='Path to output processed CSV file')
    parser.add_argument('--model_output', default='models/improved_pbb_customer_name_model.pkl',
                        help='Path to save the trained model')
    parser.add_argument('--model_type', default='gradient_boosting',
                        choices=['random_forest', 'gradient_boosting', 'svm'],
                        help='Type of model to use')
    parser.add_argument('--no_augmentation', action='store_true',
                        help='Disable data augmentation')
    parser.add_argument('--encoding', default='utf-8',
                        help='Encoding to use for CSV files')
    parser.add_argument('--skip_training', action='store_true',
                        help='Skip training and use existing model')
    
    args = parser.parse_args()
    
    # Create output directories if they don't exist
    os.makedirs(os.path.dirname(args.model_output), exist_ok=True)
    os.makedirs(os.path.dirname(args.output_file), exist_ok=True)
    
    # Step 1: Train the model (unless skip_training is specified)
    if not args.skip_training:
        print("\n=== Training Improved Customer Name Model ===\n")
        train_customer_name_model(
            args.training_data,
            args.model_output,
            use_augmentation=not args.no_augmentation,
            model_type=args.model_type
        )
    else:
        print(f"\n=== Skipping training, using existing model at {args.model_output} ===\n")
    
    # Step 2: Process the transactions
    print("\n=== Processing Transactions with Improved Model ===\n")
    success = process_transactions(
        args.input_file,
        args.output_file,
        args.model_output,
        args.encoding
    )
    
    if success:
        print("\n=== Processing completed successfully! ===\n")
        
        # Compare results with original output if it exists
        original_output = args.output_file.replace('_improved', '')
        if os.path.exists(original_output):
            try:
                df_original = pd.read_csv(original_output)
                df_improved = pd.read_csv(args.output_file)
                
                # Count non-empty customer names in both files
                original_count = df_original['CUSTOMER_NAME'].notna().sum()
                improved_count = df_improved['CUSTOMER_NAME'].notna().sum()
                
                print(f"Original model extracted {original_count} customer names")
                print(f"Improved model extracted {improved_count} customer names")
                print(f"Improvement: {improved_count - original_count} additional customer names extracted")
                
                # Check for differences in customer names
                if 'CUSTOMER_NAME' in df_original.columns and 'CUSTOMER_NAME' in df_improved.columns:
                    different_names = 0
                    for idx in range(min(len(df_original), len(df_improved))):
                        orig_name = str(df_original.loc[idx, 'CUSTOMER_NAME']).strip()
                        impr_name = str(df_improved.loc[idx, 'CUSTOMER_NAME']).strip()
                        
                        if orig_name != impr_name and orig_name and impr_name:
                            different_names += 1
                    
                    print(f"Found {different_names} differences in customer names between original and improved models")
            except Exception as e:
                print(f"Error comparing results: {e}")
    else:
        print("\n=== Processing failed. Check the error messages above. ===\n")

if __name__ == "__main__":
    main()
