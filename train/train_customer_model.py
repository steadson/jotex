import pandas as pd
import pickle
import argparse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from fuzzywuzzy import process

def train_customer_name_model(training_data_path, output_model_path='customer_name_model.pkl'):
    """
    Train a model to predict clean customer names from raw transaction descriptions.
    
    Args:
        training_data_path (str): Path to the CSV file with training data
        output_model_path (str): Path to save the trained model and reference data
    """
    # Load training data
    print(f"Loading training data from {training_data_path}")
    df_train = pd.read_csv(training_data_path)
    
    # Ensure required columns exist
    if 'raw_name' not in df_train.columns or 'clean_name' not in df_train.columns:
        raise ValueError("Training data must contain 'raw_name' and 'clean_name' columns")
    
    # Create reference dictionary for exact matching
    reference_dict = dict(zip(df_train['raw_name'].str.lower(), df_train['clean_name']))
    
    # Feature extraction - convert text to numeric features
    print("Training TF-IDF vectorizer...")
    vectorizer = TfidfVectorizer(
        analyzer='char_wb',  # Character n-grams, respecting word boundaries
        ngram_range=(2, 5),  # Use 2-5 character sequences
        max_features=5000,
        lowercase=True
    )
    
    X = vectorizer.fit_transform(df_train['raw_name'])
    y = df_train['clean_name']
    
    # Train a classifier
    print("Training classifier...")
    classifier = RandomForestClassifier(n_estimators=100, random_state=42)
    classifier.fit(X, y)
    
    # Save the model and reference data
    model_data = {
        'vectorizer': vectorizer,
        'classifier': classifier,
        'reference_dict': reference_dict,
        'training_examples': list(reference_dict.keys())
    }
    
    with open(output_model_path, 'wb') as f:
        pickle.dump(model_data, f)
    
    print(f"Model trained and saved to {output_model_path}")
    print(f"Model contains {len(reference_dict)} reference names")
    
    return model_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Train customer name cleaning model')
    parser.add_argument('--training_data', required=True, help='Path to training data CSV')
    parser.add_argument('--output_model', default='customer_name_model.pkl', help='Path to save model')
    
    args = parser.parse_args()
    
    train_customer_name_model(args.training_data, args.output_model)