import pandas as pd
import pickle
import argparse
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import classification_report, accuracy_score
from sklearn.pipeline import Pipeline
import re
from fuzzywuzzy import process
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

# Download NLTK resources if not already present
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

def preprocess_text(text):
    """
    Preprocess text for better feature extraction
    """
    if not isinstance(text, str):
        return ""
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove special characters but keep spaces between words
    text = re.sub(r'[^\w\s]', ' ', text)
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def create_augmented_data(df):
    """
    Create augmented training data by introducing variations
    """
    augmented_data = []
    
    for _, row in df.iterrows():
        raw_name = row['raw_name']
        clean_name = row['clean_name']
        
        # Skip empty entries
        if not isinstance(raw_name, str) or not raw_name.strip():
            continue
            
        # Original data
        augmented_data.append((raw_name, clean_name))
        
        # Add variations with different spacing
        if ' ' in raw_name:
            # Extra space
            augmented_data.append((re.sub(r'\s', '  ', raw_name), clean_name))
            
            # Remove some spaces
            words = raw_name.split()
            if len(words) > 2:
                joined = words[0] + words[1] + ' ' + ' '.join(words[2:])
                augmented_data.append((joined, clean_name))
        
        # Add variations with common typos
        if len(raw_name) > 5:
            # Swap two adjacent characters
            idx = min(len(raw_name) - 2, len(raw_name) // 2)
            typo = raw_name[:idx] + raw_name[idx+1] + raw_name[idx] + raw_name[idx+2:]
            augmented_data.append((typo, clean_name))
            
            # Remove a character
            idx = min(len(raw_name) - 1, len(raw_name) // 2)
            typo = raw_name[:idx] + raw_name[idx+1:]
            augmented_data.append((typo, clean_name))
            
            # Add a duplicate character
            idx = min(len(raw_name) - 1, len(raw_name) // 2)
            typo = raw_name[:idx] + raw_name[idx] + raw_name[idx:]
            augmented_data.append((typo, clean_name))
    
    # Create a new DataFrame with augmented data
    augmented_df = pd.DataFrame(augmented_data, columns=['raw_name', 'clean_name'])
    
    print(f"Original data size: {len(df)}")
    print(f"Augmented data size: {len(augmented_df)}")
    
    return augmented_df

def extract_custom_features(texts):
    """
    Extract custom features from text
    """
    features = []
    
    for text in texts:
        text_features = {}
        
        # Length features
        text_features['text_length'] = len(text)
        text_features['word_count'] = len(text.split())
        
        # Character type features
        text_features['uppercase_ratio'] = sum(1 for c in text if c.isupper()) / max(len(text), 1)
        text_features['digit_ratio'] = sum(1 for c in text if c.isdigit()) / max(len(text), 1)
        text_features['special_char_ratio'] = sum(1 for c in text if not c.isalnum() and not c.isspace()) / max(len(text), 1)
        
        # Pattern features
        text_features['has_invoice_pattern'] = 1 if re.search(r'inv|invoice|bill|statement', text.lower()) else 0
        text_features['has_date_pattern'] = 1 if re.search(r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}', text) else 0
        text_features['has_amount_pattern'] = 1 if re.search(r'\d+\.\d{2}', text) else 0
        
        features.append(text_features)
    
    return pd.DataFrame(features)

def train_customer_name_model(training_data_path, output_model_path='improved_customer_name_model.pkl', use_augmentation=True, model_type='gradient_boosting'):
    """
    Train an improved model to predict clean customer names from raw transaction descriptions.
    
    Args:
        training_data_path (str): Path to the CSV file with training data
        output_model_path (str): Path to save the trained model and reference data
        use_augmentation (bool): Whether to use data augmentation
        model_type (str): Type of model to use ('random_forest', 'gradient_boosting', or 'svm')
    """
    # Load training data
    print(f"Loading training data from {training_data_path}")
    df_train = pd.read_csv(training_data_path)
    
    # Ensure required columns exist
    if 'raw_name' not in df_train.columns or 'clean_name' not in df_train.columns:
        raise ValueError("Training data must contain 'raw_name' and 'clean_name' columns")
    
    # Preprocess data
    df_train['raw_name'] = df_train['raw_name'].apply(lambda x: str(x) if pd.notna(x) else "")
    df_train['clean_name'] = df_train['clean_name'].apply(lambda x: str(x) if pd.notna(x) else "")
    
    # Filter out empty entries
    df_train = df_train[df_train['raw_name'].str.strip() != ""]
    df_train = df_train[df_train['clean_name'].str.strip() != ""]
    
    # Create augmented data if requested
    if use_augmentation:
        df_train = create_augmented_data(df_train)
    
    # Create reference dictionary for exact matching
    reference_dict = dict(zip(df_train['raw_name'].str.lower(), df_train['clean_name']))
    
    # Split data into training and validation sets
    X_train, X_val, y_train, y_val = train_test_split(
        df_train['raw_name'], 
        df_train['clean_name'], 
        test_size=0.2, 
        random_state=42
    )
    
    # Feature extraction - TF-IDF with improved parameters
    print("Training TF-IDF vectorizer...")
    
    # Character n-grams vectorizer
    char_vectorizer = TfidfVectorizer(
        analyzer='char_wb',  # Character n-grams, respecting word boundaries
        ngram_range=(2, 6),  # Use 2-6 character sequences (increased range)
        max_features=10000,  # Increased from 5000
        lowercase=True,
        min_df=2,           # Ignore terms that appear in less than 2 documents
        max_df=0.95,        # Ignore terms that appear in more than 95% of documents
        sublinear_tf=True   # Apply sublinear tf scaling (1 + log(tf))
    )
    
    # Word n-grams vectorizer
    word_vectorizer = TfidfVectorizer(
        analyzer='word',     # Word n-grams
        ngram_range=(1, 3),  # Use 1-3 word sequences
        max_features=5000,
        lowercase=True,
        min_df=2,
        max_df=0.95,
        sublinear_tf=True,
        stop_words='english'  # Remove English stop words
    )
    
    # Transform training data
    X_char_train = char_vectorizer.fit_transform(X_train)
    X_word_train = word_vectorizer.fit_transform(X_train)
    
    # Transform validation data
    X_char_val = char_vectorizer.transform(X_val)
    X_word_val = word_vectorizer.transform(X_val)
    
    # Combine features
    X_combined_train = np.hstack([
        X_char_train.toarray(), 
        X_word_train.toarray()
    ])
    
    X_combined_val = np.hstack([
        X_char_val.toarray(), 
        X_word_val.toarray()
    ])
    
    # Choose classifier based on model_type
    print(f"Training {model_type} classifier...")
    
    if model_type == 'random_forest':
        classifier = RandomForestClassifier(
            n_estimators=200,       # Increased from 100
            max_depth=None,
            min_samples_split=2,
            min_samples_leaf=1,
            max_features='sqrt',
            bootstrap=True,
            class_weight='balanced',  # Handle class imbalance
            random_state=42,
            n_jobs=-1                 # Use all available cores
        )
    elif model_type == 'gradient_boosting':
        classifier = GradientBoostingClassifier(
            n_estimators=200,
            learning_rate=0.1,
            max_depth=5,
            min_samples_split=2,
            min_samples_leaf=1,
            subsample=0.8,
            max_features='sqrt',
            random_state=42
        )
    elif model_type == 'svm':
        classifier = SVC(
            C=10.0,
            kernel='rbf',
            gamma='scale',
            probability=True,
            class_weight='balanced',
            random_state=42
        )
    else:
        raise ValueError(f"Unknown model type: {model_type}")
    
    # Train the classifier
    classifier.fit(X_combined_train, y_train)
    
    # Evaluate on validation set
    y_pred = classifier.predict(X_combined_val)
    accuracy = accuracy_score(y_val, y_pred)
    print(f"Validation accuracy: {accuracy:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_val, y_pred))
    
    # Save the model and reference data
    model_data = {
        'char_vectorizer': char_vectorizer,
        'word_vectorizer': word_vectorizer,
        'classifier': classifier,
        'reference_dict': reference_dict,
        'training_examples': list(reference_dict.keys()),
        'model_type': model_type,
        'validation_accuracy': accuracy
    }
    
    with open(output_model_path, 'wb') as f:
        pickle.dump(model_data, f)
    
    print(f"Model trained and saved to {output_model_path}")
    print(f"Model contains {len(reference_dict)} reference names")
    
    return model_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Train improved customer name cleaning model')
    parser.add_argument('--training_data', required=True, help='Path to training data CSV')
    parser.add_argument('--output_model', default='improved_customer_name_model.pkl', help='Path to save model')
    parser.add_argument('--no_augmentation', action='store_true', help='Disable data augmentation')
    parser.add_argument('--model_type', default='gradient_boosting', 
                        choices=['random_forest', 'gradient_boosting', 'svm'],
                        help='Type of model to use')
    
    args = parser.parse_args()
    
    train_customer_name_model(
        args.training_data, 
        args.output_model, 
        use_augmentation=not args.no_augmentation,
        model_type=args.model_type
    )
