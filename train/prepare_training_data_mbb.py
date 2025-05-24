import pandas as pd
from pathlib import Path

def prepare_training_data(input_csv_path, output_training_path):
    """
    Prepare training data from processed transactions with correct CUSTOMER_NAME values.
    
    Args:
        input_csv_path (str): Path to CSV with processed transactions
        output_training_path (str): Path to save the training data
    """
    # Load the processed transactions
    df = pd.read_csv(input_csv_path)
    
    # Check if required columns exist
    required_cols = ['Transaction Description.1', 'Transaction Description', 'CUSTOMER_NAME']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")
    
    # Create training data
    training_data = []
    
    for _, row in df.iterrows():
        # Get raw name (from Transaction Description.1 or Transaction Description)
        raw_name = row['Transaction Description.1']
        if pd.isna(raw_name) or raw_name == '-':
            raw_name = row['Transaction Description']
            
        # Get clean name
        clean_name = row['CUSTOMER_NAME']
        
        # Add to training data if both values are valid
        if pd.notna(raw_name) and pd.notna(clean_name) and raw_name != '' and clean_name != '':
            training_data.append({
                'raw_name': raw_name,
                'clean_name': clean_name
            })
    
    # Create and save training DataFrame
    training_df = pd.DataFrame(training_data)
    training_df.to_csv(output_training_path, index=False)
    
    print(f"Created training data with {len(training_df)} examples at {output_training_path}")
    
    return training_df

if __name__ == "__main__":
    file_name = 'MBB_2025.csv'
    input_csv = Path('data/input') / file_name
    output_training = Path('data/training') / file_name
    
    # Prepare training data
    prepare_training_data(input_csv, output_training)