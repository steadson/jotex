import pandas as pd
import os
from pathlib import Path

def prepare_training_data(input_file, output_file):
    """
    Prepare training data for customer name model from processed transactions.
    
    Parameters:
    input_file (str): Path to the processed CSV file (OUTPUT/PBB_2025_processed.csv)
    output_file (str): Path to save the training data CSV file
    
    Returns:
    bool: True if processing was successful, False otherwise
    """
    try:
        print(f"Reading processed transactions from: {input_file}")
        
        # Read the processed transactions
        df = pd.read_csv(input_file)
        
        # Find transaction description column
        txn_desc_col = None
        for col in df.columns:
            if any(term in str(col).lower() for term in ['description', 'particulars', 'details']):
                if 'transaction' in str(col).lower() or 'txn' in str(col).lower():
                    txn_desc_col = col
                    break
        
        if not txn_desc_col:
            print("Error: Could not find Transaction Description column")
            return False
        
        # Ensure CUSTOMER_NAME column exists
        if 'CUSTOMER_NAME' not in df.columns:
            print("Error: CUSTOMER_NAME column not found in processed data")
            return False
        
        print(f"Using columns: '{txn_desc_col}' for raw_name and 'CUSTOMER_NAME' for clean_name")
        
        # Create training data DataFrame
        training_data = df[[txn_desc_col, 'CUSTOMER_NAME']].copy()
        
        # Filter out rows with empty customer names
        training_data = training_data[training_data['CUSTOMER_NAME'].notna() & 
                                    (training_data['CUSTOMER_NAME'] != '')]
        
        # Rename columns to match training script requirements
        training_data = training_data.rename(columns={
            txn_desc_col: 'raw_name',
            'CUSTOMER_NAME': 'clean_name'
        })
        
        # Save training data
        training_data.to_csv(output_file, index=False)
        
        print(f"Training data prepared with {len(training_data)} examples")
        print(f"Saved to {output_file}")
        
        return True
    
    except Exception as e:
        print(f"Error preparing training data: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    input_file = Path('OUTPUT') / 'PBB_2025_processed.csv'
    output_file = Path('data_training') / 'customer_name_training_data.csv'
    
    success = prepare_training_data(input_file, output_file)
    
    if success:
        print("Training data preparation completed successfully!")
        print(f"Next step: Run 'python train/train_customer_model.py --training_data {output_file} --output_model models/pbb_customer_name_model.pkl'")
    else:
        print("Training data preparation failed. Check the error messages above.")