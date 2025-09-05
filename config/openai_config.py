import os
from dotenv import load_dotenv

load_dotenv()

class OpenAIConfig:
    API_KEY = os.getenv('OPENAI_API_KEY')
    MODEL = 'gpt-4o-mini'  # Cost-effective model
    MAX_TOKENS = 1000
    TEMPERATURE = 0.1  # Low temperature for consistent results
    
    # Retry configuration
    MAX_RETRIES = 3
    RETRY_DELAY = 1  # seconds
    
    # Batch processing
    BATCH_SIZE = 10  # Process 10 transactions at once