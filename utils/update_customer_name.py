from pathlib import Path
import pandas as pd
from difflib import SequenceMatcher
import logging
import json
import os  # Add missing import
from typing import List, Dict, Optional, Tuple
import openai
from .logger import setup_logging

# Setup logger for customer name matching
logger = setup_logging('customer_name_matching')

def similarity(a, b):
    """Calculate similarity ratio between two strings."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def normalize_customer_name(name):
    """Normalize customer name for better matching."""
    if pd.isna(name) or name == "":
        return ""
    
    name = str(name).strip()
    # Handle URL encoding
    name = name.replace('%26', '&')
    name = name.replace('%20', ' ')
    # Remove extra spaces
    name = ' '.join(name.split())
    return name

def get_top_matches_from_bc(possible_names: List[str], bc_df: pd.DataFrame, 
                           name_columns: List[str], top_n: int = 20) -> pd.DataFrame:
    """Get top N matches from Business Central database for possible names."""
    all_matches = []
    
    for possible_name in possible_names:
        for _, row in bc_df.iterrows():
            for col_name in name_columns:
                if col_name not in bc_df.columns:
                    continue
                    
                candidate_name = normalize_customer_name(row[col_name])
                if not candidate_name:
                    continue
                    
                sim_ratio = similarity(possible_name, candidate_name)
                
                if sim_ratio > 0.4:  # Lower threshold for initial search
                    match_record = row.to_dict()
                    match_record['similarity_score'] = sim_ratio
                    match_record['matched_column'] = col_name
                    match_record['search_name'] = possible_name
                    all_matches.append(match_record)
    
    # Convert to DataFrame and sort by similarity
    if all_matches:
        matches_df = pd.DataFrame(all_matches)
        matches_df = matches_df.sort_values('similarity_score', ascending=False)
        # Remove duplicates based on customer name
        matches_df = matches_df.drop_duplicates(subset=['CUSTOMER_NAME'], keep='first')
        return matches_df.head(top_n)
    
    return pd.DataFrame()

def find_best_match_in_dataframe(input_name, df, name_columns, similarity_threshold=0.7):
    """Find best match in a dataframe across multiple name columns."""
    best_match = None
    best_similarity = 0
    best_match_type = None
    
    for _, row in df.iterrows():
        for col_name in name_columns:
            if col_name not in df.columns:
                continue
                
            candidate_name = normalize_customer_name(row[col_name])
            if not candidate_name:
                continue
                
            sim_ratio = similarity(input_name, candidate_name)
            
            if sim_ratio > best_similarity and sim_ratio >= similarity_threshold:
                best_similarity = sim_ratio
                best_match = row
                best_match_type = col_name
    
    return best_match, best_similarity, best_match_type

def update_customer_name_dual_matching(customer_db_path, bc_cache_path, input_file, 
                                     local_threshold=0.95, bc_threshold=0.75):
    """Enhanced customer matching with dual-stage process and separate thresholds."""
    logger.info(f"Starting dual-stage customer name update for {input_file}")
    logger.info(f"Local customer database: {customer_db_path}")
    logger.info(f"Business Central cache: {bc_cache_path}")
    logger.info(f"Local DB threshold: {local_threshold}")
    logger.info(f"Business Central threshold: {bc_threshold}")
    
    try:
        # Load all required files
        customer_df = pd.read_csv(customer_db_path)
        input_df = pd.read_csv(input_file)
        
        # Load Business Central cache if it exists
        bc_df = None
        if os.path.exists(bc_cache_path):
            bc_df = pd.read_csv(bc_cache_path)
            logger.info(f"Loaded {len(bc_df)} Business Central cached customers")
        else:
            logger.warning(f"Business Central cache not found: {bc_cache_path}")
        
        logger.info(f"Loaded {len(customer_df)} local database entries")
        logger.info(f"Processing {len(input_df)} input rows")
        
    except Exception as e:
        logger.error(f"Failed to load files: {e}")
        return
    
    # Check required columns
    local_db_columns = ["SPECIAL NAME BANK IN", "CUSTOMER NAME"]
    bc_cache_columns = ["CUSTOMER_NAME", "CONTACT"]
    
    if "CUSTOMER_NAME" not in input_df.columns:
        logger.error("'CUSTOMER_NAME' column not found in input file")
        return
    
    updated_count = 0
    processed_count = 0
    skipped_count = 0
    local_matches = 0
    bc_matches = 0
    
    for index, row in input_df.iterrows():
        input_customer_name = normalize_customer_name(row["CUSTOMER_NAME"])
        
        # Skip if customer name is empty
        if not input_customer_name:
            logger.debug(f"Row {index+1}: Skipping empty customer name")
            skipped_count += 1
            continue

        processed_count += 1
        logger.debug(f"Row {index+1}: Processing '{input_customer_name}'")
        
        # Stage 1: Try to match against local customer database with local_threshold
        best_match, best_similarity, match_type = find_best_match_in_dataframe(
            input_customer_name, customer_df, local_db_columns, local_threshold
        )
        
        if best_match is not None:
            # Found match in local database
            new_customer_name = normalize_customer_name(best_match["CUSTOMER NAME"])
            input_df.at[index, "CUSTOMER_NAME"] = new_customer_name
            updated_count += 1
            local_matches += 1
            
            logger.info(f"Row {index+1}: LOCAL MATCH via {match_type} - '{row['CUSTOMER_NAME']}' -> '{new_customer_name}' (similarity: {best_similarity:.3f})")
            continue
        
        # Stage 2: Try to match against Business Central cache with bc_threshold
        if bc_df is not None:
            bc_match, bc_similarity, bc_match_type = find_best_match_in_dataframe(
                input_customer_name, bc_df, bc_cache_columns, bc_threshold
            )
            
            if bc_match is not None:
                # Found match in Business Central cache
                new_customer_name = normalize_customer_name(bc_match["CUSTOMER_NAME"])
                input_df.at[index, "CUSTOMER_NAME"] = new_customer_name
                updated_count += 1
                bc_matches += 1
                
                logger.info(f"Row {index+1}: BC MATCH via {bc_match_type} - '{row['CUSTOMER_NAME']}' -> '{new_customer_name}' (similarity: {bc_similarity:.3f})")
                continue
        
        # No match found in either database
        logger.warning(f"Row {index+1}: NO MATCH - '{input_customer_name}'")
    
    # Save the updated dataframe
    input_df.to_csv(input_file, index=False)
    
    # Log final statistics
    logger.info(f"Dual-stage customer name matching completed:")
    logger.info(f"  Total rows: {len(input_df)}")
    logger.info(f"  Processed: {processed_count}")
    logger.info(f"  Skipped (empty): {skipped_count}")
    logger.info(f"  Local DB matches (≥{local_threshold}): {local_matches}")
    logger.info(f"  Business Central matches (≥{bc_threshold}): {bc_matches}")
    logger.info(f"  Total matched: {updated_count}")
    logger.info(f"  No matches found: {processed_count - updated_count}")
    logger.info(f"  Overall match rate: {(updated_count/processed_count*100):.1f}%" if processed_count > 0 else "  Match rate: 0%")
    
    print(f"Updated {updated_count} customer names in {input_file}")
    print(f"Local DB matches: {local_matches}, Business Central matches: {bc_matches}")

def update_customer_name_for_file_dual(processed_file_path, local_threshold=0.95, bc_threshold=0.75, 
                                              use_ai_fallback=True, openai_api_key=None):
    """Enhanced auto-determine databases and perform dual-stage matching with AI fallback supporting multiple description fields."""
    if not openai_api_key:
        openai_api_key = os.getenv('OPENAI_API_KEY')
    processed_file_path = Path(processed_file_path)
    
    if not processed_file_path.exists():
        logger.error(f"Processed file not found: {processed_file_path}")
        return False
    
    # Determine database paths based on file name
    file_name = processed_file_path.name.upper()
    
    database_mapping = {
        "MBB_2025_PROCESSED.CSV": {
            "local_db": "data/customer_db/MY_MBB_CUSTOMER_NAME.csv",
            "bc_cache": "data/customer_db/BC_MY_CUSTOMERS.csv"
        },
        "PBB_2025_PROCESSED.CSV": {
            "local_db": "data/customer_db/MY_PBB_CUSTOMER_NAME.csv", 
            "bc_cache": "data/customer_db/BC_MY_CUSTOMERS.csv"
        },
        "SG_MBB_2025_PROCESSED.CSV": {
            "local_db": "data/customer_db/SG_MBB_customer_name.csv",
            "bc_cache": "data/customer_db/BC_MY_CUSTOMERS.csv"
        },
        "SMARTHOME_MBB_2025_PROCESSED.CSV": {
            "local_db": "data/customer_db/MY_MBB_CUSTOMER_NAME.csv",
            "bc_cache": "data/customer_db/BC_MY_CUSTOMERS.csv"
        }
    }
    
    config = None
    for pattern, db_config in database_mapping.items():
        if pattern in file_name:
            config = db_config
            break
    
    if config is None:
        logger.error(f"No database mapping found for file: {file_name}")
        return False
    
    local_db_path = Path(config["local_db"])
    bc_cache_path = Path(config["bc_cache"])
    
    if not local_db_path.exists():
        logger.error(f"Local customer database not found: {local_db_path}")
        return False
    
    logger.info(f"Starting enhanced matching with multiple description fields for {processed_file_path}")
    logger.info(f"Local DB: {local_db_path} (threshold: {local_threshold})")
    logger.info(f"BC Cache: {bc_cache_path} (threshold: {bc_threshold})")
    logger.info(f"AI Fallback: {'Enabled' if use_ai_fallback else 'Disabled'}")
    
    try:
        # Load all required files
        customer_df = pd.read_csv(local_db_path)
        input_df = pd.read_csv(processed_file_path)
        
        # Debug: Print available columns
        logger.info(f"Available columns in input file: {input_df.columns.tolist()}")
        
        # Load Business Central cache
        bc_df = None
        if bc_cache_path.exists():
            bc_df = pd.read_csv(bc_cache_path)
            logger.info(f"Loaded {len(bc_df)} Business Central cached customers")
        else:
            logger.warning(f"Business Central cache not found: {bc_cache_path}")
            use_ai_fallback = False
        
        logger.info(f"Loaded {len(customer_df)} local database entries")
        logger.info(f"Processing {len(input_df)} input rows")
        
    except Exception as e:
        logger.error(f"Failed to load files: {e}")
        return False
    
    # Initialize AI matcher if enabled and API key provided
    ai_matcher = None
    if use_ai_fallback and openai_api_key and bc_df is not None:
        try:
            ai_matcher = EnhancedAICustomerMatcher(openai_api_key)
            logger.info("Enhanced AI matcher with multiple description fields initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize AI matcher: {e}")
            use_ai_fallback = False
    elif use_ai_fallback and not openai_api_key:
        logger.warning("AI fallback requested but no OpenAI API key provided")
        use_ai_fallback = False
    
    # Check required columns
    local_db_columns = ["SPECIAL NAME BANK IN", "CUSTOMER NAME"]
    bc_cache_columns = ["CUSTOMER_NAME", "CONTACT"]
    
    if "CUSTOMER_NAME" not in input_df.columns:
        logger.error("CUSTOMER_NAME column not found in input file")
        return False
    
    # Statistics tracking
    stats = {
        'processed': 0,
        'skipped_empty': 0,
        'local_matches': 0,
        'bc_matches': 0,
        'ai_matches': 0,
        'no_matches': 0,
        'ai_no_description': 0,
        'ai_multiple_fields_used': 0,
        'ai_single_field_used': 0
    }
    
    # Store analysis results for debugging
    ai_analysis_log = []
    
    for index, row in input_df.iterrows():
        input_customer_name = normalize_customer_name(row["CUSTOMER_NAME"])
        
        # Skip if customer name is empty
        if not input_customer_name:
            logger.debug(f"Row {index+1}: Skipping empty customer name")
            stats['skipped_empty'] += 1
            continue

        stats['processed'] += 1
        logger.debug(f"Row {index+1}: Processing '{input_customer_name}'")
        
        # Stage 1: Try to match against local customer database
        best_match, best_similarity, match_type = find_best_match_in_dataframe(
            input_customer_name, customer_df, local_db_columns, local_threshold
        )
        
        if best_match is not None:
            new_customer_name = normalize_customer_name(best_match["CUSTOMER NAME"])
            input_df.at[index, "CUSTOMER_NAME"] = new_customer_name
            stats['local_matches'] += 1
            logger.info(f"Row {index+1}: LOCAL MATCH - '{row['CUSTOMER_NAME']}' -> '{new_customer_name}' (similarity: {best_similarity:.3f})")
            continue
        
        # Stage 2: Try to match against Business Central cache
        if bc_df is not None:
            bc_match, bc_similarity, bc_match_type = find_best_match_in_dataframe(
                input_customer_name, bc_df, bc_cache_columns, bc_threshold
            )
            
            if bc_match is not None:
                new_customer_name = normalize_customer_name(bc_match["CUSTOMER_NAME"])
                input_df.at[index, "CUSTOMER_NAME"] = new_customer_name
                stats['bc_matches'] += 1
                logger.info(f"Row {index+1}: BC MATCH - '{row['CUSTOMER_NAME']}' -> '{new_customer_name}' (similarity: {bc_similarity:.3f})")
                continue
        
        # Stage 3: Enhanced AI Fallback with Multiple Description Fields
        if use_ai_fallback and ai_matcher:
            # Get all available description fields
            description_fields = get_all_description_fields_info(row, input_df.columns.tolist())
            # Get combined description
            combined_description = get_description_value(row, input_df.columns.tolist(), combine_all=True)
            
            if combined_description and combined_description.strip():
                logger.info(f"Row {index+1}: Traditional methods failed, trying AI fallback with multiple description fields")
                
                # Track statistics about description fields usage
                if len(description_fields) > 1:
                    stats['ai_multiple_fields_used'] += 1
                    logger.debug(f"Row {index+1}: Using {len(description_fields)} description fields: {list(description_fields.keys())}")
                else:
                    stats['ai_single_field_used'] += 1
                    logger.debug(f"Row {index+1}: Using single description field")
                
                logger.debug(f"Row {index+1}: Combined description: '{combined_description[:200]}...'")
                
                # Use the enhanced AI matching function
                ai_result, was_updated, ai_analysis = ai_two_stage_matching(
                    input_customer_name, combined_description, bc_df, ai_matcher, description_fields
                )
                
                # Store detailed analysis for logging
                ai_analysis_log.append({
                    'row': index + 1,
                    'original_name': input_customer_name,
                    'description_fields': description_fields,
                    'combined_description': combined_description,
                    'analysis': ai_analysis
                })
                
                if was_updated and ai_result != input_customer_name:
                    input_df.at[index, "CUSTOMER_NAME"] = ai_result
                    stats['ai_matches'] += 1
                    confidence = ai_analysis.get('stage2', {}).get('confidence_score', 'N/A')
                    logger.info(f"Row {index+1}: AI MATCH - '{row['CUSTOMER_NAME']}' -> '{ai_result}' (confidence: {confidence}%)")
                    continue
                else:
                    logger.debug(f"Row {index+1}: AI analysis completed but no high-confidence match found")
            else:
                logger.debug(f"Row {index+1}: No description available for AI analysis")
                stats['ai_no_description'] += 1
        
        # No match found in any stage
        stats['no_matches'] += 1
        logger.warning(f"Row {index+1}: NO MATCH - '{input_customer_name}'")
    
    # Save the updated dataframe
    input_df.to_csv(processed_file_path, index=False)
    
    # Save detailed AI analysis log
    if ai_analysis_log:
        ai_log_file = processed_file_path.parent / f"{processed_file_path.stem}_ai_analysis_log.json"
        with open(ai_log_file, 'w') as f:
            json.dump(ai_analysis_log, f, indent=2)
        logger.info(f"Saved detailed AI analysis log to {ai_log_file}")
    
    # Calculate totals
    total_updated = stats['local_matches'] + stats['bc_matches'] + stats['ai_matches']
    
    # Log final statistics
    logger.info(f"Enhanced customer name matching with multiple description fields completed:")
    logger.info(f"  Total rows: {len(input_df)}")
    logger.info(f"  Processed: {stats['processed']}")
    logger.info(f"  Skipped (empty): {stats['skipped_empty']}")
    logger.info(f"  Local DB matches (≥{local_threshold}): {stats['local_matches']}")
    logger.info(f"  Business Central matches (≥{bc_threshold}): {stats['bc_matches']}")
    logger.info(f"  AI matches: {stats['ai_matches']}")
    logger.info(f"  AI with multiple description fields: {stats['ai_multiple_fields_used']}")
    logger.info(f"  AI with single description field: {stats['ai_single_field_used']}")
    logger.info(f"  AI no description: {stats['ai_no_description']}")
    logger.info(f"  Total matched: {total_updated}")
    logger.info(f"  No matches found: {stats['no_matches']}")
    if stats['processed'] > 0:
        logger.info(f"  Overall match rate: {(total_updated/stats['processed']*100):.1f}%")
    
    print(f"Updated {total_updated} customer names in {processed_file_path}")
    print(f"Breakdown: Local={stats['local_matches']}, BC={stats['bc_matches']}, AI={stats['ai_matches']}")
    print(f"AI used multiple fields in {stats['ai_multiple_fields_used']} cases, single field in {stats['ai_single_field_used']} cases")
    
    return True

def extract_json_from_response(response_content):
    """
    Extract JSON from OpenAI response that may be wrapped in markdown code blocks.
    
    Args:
        response_content: Raw response content from OpenAI
        
    Returns:
        dict: Parsed JSON object
    """
    # Remove markdown code block markers if present
    content = response_content.strip()
    
    # Check if content is wrapped in ```json ... ```
    if content.startswith('```json') and content.endswith('```'):
        # Extract content between the markers
        content = content[7:-3].strip()  # Remove ```json and ```
    elif content.startswith('```') and content.endswith('```'):
        # Handle generic code blocks
        lines = content.split('\n')
        if len(lines) > 2:
            content = '\n'.join(lines[1:-1])  # Remove first and last line
    
    # Try to parse the JSON
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        # If that fails, try to find JSON within the content
        # Look for content between { and }
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        else:
            raise e

def get_description_value(row, df_columns, combine_all=False):
    """
    Get transaction description from row, checking multiple possible column names.
    
    Args:
        row: DataFrame row
        df_columns: List of column names in the DataFrame
        
    Returns:
        str: Description value or empty string if not found
    """
    # Possible description column names (in order of preference)
    description_columns = [
        'DESCRIPTION',
        'Description', 
        'Transaction Description',
        'Transaction Description.1',
        'Transaction Description.2', 
        'Transaction Ref',
        'TRANSACTION_DESCRIPTION',
        'transaction_description',
        'DESC',
        'REMARKS',
        'Remarks',
        'NARRATIVE',
        'Narrative',
        'DETAILS',
        'Details',
        'MEMO',
        'Memo',
    ]
    
    if not combine_all:
        # Original behavior - return first valid description found
        for col_name in description_columns:
            if col_name in df_columns:
                value = str(row.get(col_name, ""))
                if value and value.strip() and value.lower() != 'nan':
                    return value.strip()
        return ""
    
    # New behavior - combine all available description fields
    found_descriptions = []
    
    for col_name in description_columns:
        if col_name in df_columns:
            value = str(row.get(col_name, ""))
            if value and value.strip() and value.lower() != 'nan':
                # Clean up the value
                cleaned_value = value.strip()
                # Avoid duplicates
                if cleaned_value not in found_descriptions:
                    found_descriptions.append(cleaned_value)
    
    if found_descriptions:
        # Combine all descriptions with clear separators
        combined = " | ".join(found_descriptions)
        return combined
    
    return ""

def get_all_description_fields_info(row, df_columns):
    """
    Get detailed information about all description fields found in the row.
    
    Args:
        row: DataFrame row
        df_columns: List of column names in the DataFrame
        
    Returns:
        dict: Dictionary with field names as keys and their values
    """
    description_columns = [
        'DESCRIPTION',
        'Description', 
        'Transaction Description',
        'Transaction Description.1',
        'Transaction Description.2', 
        'Transaction Ref',
        'TRANSACTION_DESCRIPTION',
        'transaction_description',
        'DESC',
        'REMARKS',
        'Remarks',
        'NARRATIVE',
        'Narrative',
        'DETAILS',
        'Details',
        'MEMO',
        'Memo'
    ]
    
    found_fields = {}
    
    for col_name in description_columns:
        if col_name in df_columns:
            value = str(row.get(col_name, ""))
            if value and value.strip() and value.lower() != 'nan':
                found_fields[col_name] = value.strip()
    
    return found_fields

class EnhancedAICustomerMatcher:
    """Enhanced AI-powered customer name matcher with two-stage analysis."""
    
    def __init__(self, api_key: str, model: str = "gpt-4o"):
        """Initialize AI matcher with OpenAI API key."""
        self.client = openai.OpenAI(api_key=api_key)
        self.model = model
        self.logger = logging.getLogger('ai_customer_matcher')
    
    def stage1_analyze_description(self, customer_name: str, description: str, description_fields: dict = None) -> Dict:
        """
        Stage 1: Analyze customer description to verify parsed name and suggest alternatives.
        
        Args:
            customer_name: Currently parsed customer name
            description: Full transaction description
            
        Returns:
            Dict with analysis results and possible alternative names
        """
        # Build description context for the AI
        description_context = f"Combined Description: {description}"
        
        if description_fields and len(description_fields) > 1:
            description_context += "\n\nIndividual Description Fields:"
            for field_name, field_value in description_fields.items():
                description_context += f"\n- {field_name}: {field_value}"
        prompt = f"""
You are an expert at analyzing banking transaction descriptions to extract accurate customer names.

Current parsed customer name: "{customer_name}"

Transaction Description Information:
{description_context}

Common parsing issues to look for:
1. "MUHAMMAD YOUNAS * YUNAS KHAN" - system parsed "MUHAMMAD YOUNAS" but actual customer might be "YUNAS KHAN"

2. "JOHN DOE / JANE DOE" - could be either person
3. "ABC COMPANY LTD * JOHN SMITH" - could be company or person
4. "AHMAD HASSAN BIN IBRAHIM" - might be parsed as "AHMAD HASSAN" but full name is better
5. Description might contain multiple names separated by *, /, or other delimiters, please take into account all names

6. Different description fields might contain different information - analyze ALL fields carefully
7. One field might have the account holder while another has the actual transaction initiator
8. "MUHAMMAD YOUNAS - YUNAS KHAN" - system parsed "MUHAMMAD YOUNAS" but actual customer might be "YUNAS KHAN"
9. "MUHAMMAD YOUNAS * YUNAS KHAN" - system parsed "MUHAMMAD YOUNAS" but actual customer might be "YUNAS KHAN"

Instructions:
- Analyze if the current parsed customer name is the most appropriate choice from ALL description fields
- Look for alternative names that might be better candidates across ALL available description fields
- Consider context clues in each description field
- Pay special attention to names that appear in multiple fields vs names that appear in only one field
- Be conservative - only suggest alternatives if you see clear evidence

Return your response in JSON format:
{{
    "analysis": "Brief explanation of your analysis across all description fields",
    "is_current_name_appropriate": true/false,
    "confidence_in_current": 85,
    "possible_alternatives": ["alternative1", "alternative2"],
    "recommended_action": "keep_current" or "search_alternatives",
    "reasoning": "Detailed reasoning for your decision based on analysis of all fields",
    "field_analysis": {{
        "field_name": "specific analysis of this field",
        "another_field": "analysis of another field"
    }}
}}

Only recommend "search_alternatives" if you identify potentially better customer names in any of the description fields.
"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a banking transaction analyst expert at identifying the most appropriate customer names from transaction descriptions."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=600
            )
            print(response.choices[0].message.content)
            response_content = response.choices[0].message.content
            self.logger.debug(f"Stage 1 Raw Response: {response_content}")
            
            # Use the improved JSON extraction function
            result = extract_json_from_response(response_content)
            self.logger.info(f"Stage 1 Analysis for '{customer_name}': {result['recommended_action']} - {result['analysis']}")
            return result
            
        except Exception as e:
            self.logger.error(f"Stage 1 AI analysis failed: {e}")
            return {
                "analysis": f"Stage 1 analysis failed: {str(e)}",
                "is_current_name_appropriate": True,
                "confidence_in_current": 50,
                "possible_alternatives": [],
                "recommended_action": "keep_current",
                "reasoning": "AI analysis failed, keeping current name"
            }
    
    def stage2_score_matches(self, original_customer_name: str, description: str, 
                           stage1_analysis: Dict, matches_df: pd.DataFrame,description_fields: dict = None) -> Dict:
        """
        Stage 2: Score potential matches from BC database and select the best one.
        
        Args:
            original_customer_name: Originally parsed customer name
            description: Transaction description
            stage1_analysis: Results from stage 1 analysis
            matches_df: DataFrame of potential matches from BC database
            
        Returns:
            Dict with scoring results and recommendation
        """
        
        if matches_df.empty:
            return {
                "analysis": "No potential matches found in Business Central database",
                "selected_match": None,
                "confidence_score": 0,
                "recommendation": "no_match",
                "reasoning": "No matches found in database to score"
            }
        
        # Prepare matches for AI analysis
        matches_list = []
        for idx, row in matches_df.iterrows():
            matches_list.append({
                "customer_name": row['CUSTOMER_NAME'],
                "contact": row.get('CONTACT', ''),
                "similarity_score": row['similarity_score'],
                "matched_via": row['matched_column'],
                "search_name": row['search_name']
            })
        
        prompt = f"""
You are analyzing potential customer matches for a banking transaction with high accuracy requirements.

Original transaction details:
- Currently parsed customer name: "{original_customer_name}"
- Full transaction description: "{description}"

Stage 1 Analysis Results:
- AI determined current name is appropriate: {stage1_analysis['is_current_name_appropriate']}
- Confidence in current name: {stage1_analysis['confidence_in_current']}%
- Alternative names suggested: {stage1_analysis['possible_alternatives']}
- Reasoning: {stage1_analysis['reasoning']}
-Field-specific analysis: {json.dumps(stage1_analysis.get('field_analysis', {}), indent=2)}

Top potential matches from Business Central database:
{json.dumps(matches_list, indent=2)}

Your task:
1. Analyze each potential match considering:
   - How well it matches against ALL description fields (not just the combined one)
   - Similarity scores from fuzzy matching
   - Which search name it was found through
   - Contact information as additional context
   - Cross-reference names appearing in different description fields
   - Overall likelihood this is the correct customer based on comprehensive analysis

2. Score each match and select the most appropriate one

3. Be very strict - only recommend a match if you're highly confident (≥80%)

Return your response in JSON format:
{{
    "analysis": "Detailed explanation of your analysis process and reasoning across all description fields",
    "selected_match": {{
        "customer_name": "Selected customer name or null if no good match",
        "reasoning": "Why this specific match was selected based on analysis of all fields",
        "supporting_evidence": "What evidence supports this choice across multiple description fields"
    }},
    "confidence_score": 95,
    "recommendation": "update_customer" or "no_match",
    "alternative_candidates": ["other strong candidates if any"],
    "rejection_reasons": "Why other candidates were rejected",
    "field_matching_analysis": "How the selected match relates to different description fields"
}}

IMPORTANT: Only recommend "update_customer" if confidence is ≥80%. Be conservative with uncertain matches.
"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a banking customer matching expert with very high accuracy requirements. You must be conservative and only recommend matches with ≥90% confidence."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=1200
            )
            
            response_content = response.choices[0].message.content
            self.logger.debug(f"Stage 2 Raw Response: {response_content}")
            
            # Use the improved JSON extraction function
            result = extract_json_from_response(response_content)
            
            self.logger.info(f"Stage 2 Scoring for '{original_customer_name}': {result['confidence_score']}% confidence, Recommendation: {result['recommendation']}")
            return result
            
        except Exception as e:
            self.logger.error(f"Stage 2 AI scoring failed: {e}")
            return {
                "analysis": f"Stage 2 scoring failed: {str(e)}",
                "selected_match": None,
                "confidence_score": 0,
                "recommendation": "no_match",
                "reasoning": "AI scoring failed",
                "alternative_candidates": [],
                "rejection_reasons": "AI analysis error",
                "field_matching_analysis": "Error occurred during analysis"
            }

def ai_two_stage_matching(customer_name: str, description: str, bc_df: pd.DataFrame, 
                                  ai_matcher: EnhancedAICustomerMatcher, description_fields: dict = None) -> Tuple[Optional[str], bool, Dict]:
    """
    Enhanced two-stage AI-powered customer matching process with support for multiple description fields.
    
    Args:
        customer_name: Originally parsed customer name
        description: Combined transaction description
        bc_df: Business Central database DataFrame
        ai_matcher: Enhanced AI matcher instance
        description_fields: Dictionary of individual description fields and their values
        
    Returns:
        Tuple of (new_customer_name, was_updated, full_analysis_results)
    """
    
    fields_info = f" with {len(description_fields)} description fields" if description_fields else ""
    ai_matcher.logger.info(f"AI Two-Stage Matching: Analyzing '{customer_name}'{fields_info}")
    
    if description_fields:
        ai_matcher.logger.debug(f"Description fields: {list(description_fields.keys())}")
    
    # Stage 1: AI analyzes description and suggests alternatives
    stage1_result = ai_matcher.stage1_analyze_description(customer_name, description, description_fields)
    
    # If AI thinks current name is fine, return early
    if stage1_result['recommended_action'] == 'keep_current':
        ai_matcher.logger.info(f"Stage 1: AI recommends keeping current name '{customer_name}'")
        return customer_name, False, {
            'stage1': stage1_result,
            'stage2': None,
            'final_decision': 'kept_current',
            'description_fields_used': list(description_fields.keys()) if description_fields else []
        }
    
    # Prepare list of names to search (including original and AI suggestions)
    possible_names = [customer_name]  # Always include original
    possible_names.extend(stage1_result['possible_alternatives'])
    
    # Remove duplicates while preserving order
    possible_names = list(dict.fromkeys(possible_names))
    
    ai_matcher.logger.info(f"Stage 1: AI suggests searching for alternatives: {possible_names}")
    
    # Search BC database for potential matches
    bc_cache_columns = ["CUSTOMER_NAME", "CONTACT"]
    top_matches = get_top_matches_from_bc(possible_names, bc_df, bc_cache_columns, top_n=20)
    
    if top_matches.empty:
        ai_matcher.logger.warning(f"No matches found in BC database for any suggested names")
        return customer_name, False, {
            'stage1': stage1_result,
            'stage2': {'analysis': 'No matches found in BC database'},
            'final_decision': 'no_matches_found',
            'description_fields_used': list(description_fields.keys()) if description_fields else []
        }
    
    ai_matcher.logger.info(f"Found {len(top_matches)} potential matches for AI evaluation")
    
    # Stage 2: AI scores and selects best match
    stage2_result = ai_matcher.stage2_score_matches(customer_name, description, stage1_result, top_matches, description_fields)
    
    # Make final decision based on AI confidence
    if (stage2_result['recommendation'] == 'update_customer' and 
        stage2_result['confidence_score'] >= 90 and 
        stage2_result['selected_match'] and 
        stage2_result['selected_match']['customer_name']):
        
        new_name = stage2_result['selected_match']['customer_name']
        ai_matcher.logger.info(f"AI Two-Stage: MATCH FOUND - '{customer_name}' -> '{new_name}' (confidence: {stage2_result['confidence_score']}%)")
        return new_name, True, {
            'stage1': stage1_result,
            'stage2': stage2_result,
            'final_decision': 'updated_customer',
            'description_fields_used': list(description_fields.keys()) if description_fields else []
        }
    
    else:
        ai_matcher.logger.warning(f"AI Two-Stage: NO MATCH - confidence {stage2_result['confidence_score']}% below 90% threshold")
        return customer_name, False, {
            'stage1': stage1_result,
            'stage2': stage2_result,
            'final_decision': 'insufficient_confidence',
            'description_fields_used': list(description_fields.keys()) if description_fields else []
        }

# Integration function to replace the existing ai_fallback_matching
openai_api_key = os.getenv('OPENAI_API_KEY')
def enhanced_customer_name_update_with_two_stage_ai(customer_db_path, bc_cache_path, input_file, 

                                                   local_threshold=0.95, bc_threshold=0.75,
                                                   openai_api_key=openai_api_key, use_ai_fallback=True):
    """Enhanced customer matching with two-stage AI analysis."""
    logger = logging.getLogger('enhanced_customer_matcher')
    logger.info(f"Starting enhanced two-stage AI customer name update for {input_file}")
    
    try:
        # Load all required files
        customer_df = pd.read_csv(customer_db_path)
        input_df = pd.read_csv(input_file)
        
        # Load Business Central cache
        bc_df = None
        if os.path.exists(bc_cache_path):
            bc_df = pd.read_csv(bc_cache_path)
            logger.info(f"Loaded {len(bc_df)} Business Central cached customers")
        else:
            logger.warning(f"Business Central cache not found: {bc_cache_path}")
            use_ai_fallback = False
        
        logger.info(f"Loaded {len(customer_df)} local database entries")
        logger.info(f"Processing {len(input_df)} input rows")
        
    except Exception as e:
        logger.error(f"Failed to load files: {e}")
        return
    
    # Initialize AI matcher if enabled
    ai_matcher = None
    if use_ai_fallback and openai_api_key:
        try:
            ai_matcher = EnhancedAICustomerMatcher(openai_api_key)
            logger.info("Enhanced AI matcher initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize enhanced AI matcher: {e}")
            use_ai_fallback = False
    
    # Check required columns
    local_db_columns = ["SPECIAL NAME BANK IN", "CUSTOMER NAME"]
    required_columns = ["CUSTOMER_NAME"]
    if use_ai_fallback:
        required_columns.append("DESCRIPTION")
    
    missing_cols = [col for col in required_columns if col not in input_df.columns]
    if missing_cols:
        logger.error(f"Required columns not found in input file: {missing_cols}")
        return
    
    # Statistics tracking
    stats = {
        'processed': 0,
        'skipped_empty': 0,
        'local_matches': 0,
        'bc_matches': 0,
        'ai_stage1_kept_current': 0,
        'ai_stage1_searched_alternatives': 0,
        'ai_stage2_successful_matches': 0,
        'ai_stage2_insufficient_confidence': 0,
        'ai_no_bc_matches': 0,
        'no_matches': 0
    }
    
    # Store detailed AI analysis results
    ai_analysis_log = []
    
    for index, row in input_df.iterrows():
        input_customer_name = normalize_customer_name(row["CUSTOMER_NAME"])
        
        # Skip if customer name is empty
        if not input_customer_name:
            logger.debug(f"Row {index+1}: Skipping empty customer name")
            stats['skipped_empty'] += 1
            continue

        stats['processed'] += 1
        logger.debug(f"Row {index+1}: Processing '{input_customer_name}'")
        
        # Stage 1: Try local customer database (same as before)
        from update_customer_name import find_best_match_in_dataframe
        best_match, best_similarity, match_type = find_best_match_in_dataframe(
            input_customer_name, customer_df, local_db_columns, local_threshold
        )
        
        if best_match is not None:
            new_customer_name = normalize_customer_name(best_match["CUSTOMER NAME"])
            input_df.at[index, "CUSTOMER_NAME"] = new_customer_name
            stats['local_matches'] += 1
            logger.info(f"Row {index+1}: LOCAL MATCH - '{row['CUSTOMER_NAME']}' -> '{new_customer_name}' (similarity: {best_similarity:.3f})")
            continue
        
        # Stage 2: Try Business Central cache (same as before)
        if bc_df is not None:
            bc_match, bc_similarity, bc_match_type = find_best_match_in_dataframe(
                input_customer_name, bc_df, ["CUSTOMER_NAME", "CONTACT"], bc_threshold
            )
            
            if bc_match is not None:
                new_customer_name = normalize_customer_name(bc_match["CUSTOMER_NAME"])
                input_df.at[index, "CUSTOMER_NAME"] = new_customer_name
                stats['bc_matches'] += 1
                logger.info(f"Row {index+1}: BC MATCH - '{row['CUSTOMER_NAME']}' -> '{new_customer_name}' (similarity: {bc_similarity:.3f})")
                continue
        
        # Stage 3: Enhanced Two-Stage AI Fallback
        if use_ai_fallback and ai_matcher and bc_df is not None:
            description = str(row.get("DESCRIPTION",  row.get("Description", "")))
            
            if description and description.strip():
                ai_result, was_updated, ai_analysis = ai_two_stage_matching(
                    input_customer_name, description, bc_df, ai_matcher
                )
                
                # Log detailed AI analysis
                ai_analysis_log.append({
                    'row': index + 1,
                    'original_name': input_customer_name,
                    'description': description,
                    'analysis': ai_analysis
                })
                
                # Update statistics based on AI decision
                if ai_analysis['final_decision'] == 'kept_current':
                    stats['ai_stage1_kept_current'] += 1
                elif ai_analysis['final_decision'] == 'no_matches_found':
                    stats['ai_no_bc_matches'] += 1
                elif ai_analysis['final_decision'] == 'updated_customer':
                    stats['ai_stage2_successful_matches'] += 1
                elif ai_analysis['final_decision'] == 'insufficient_confidence':
                    stats['ai_stage2_insufficient_confidence'] += 1
                    stats['ai_stage1_searched_alternatives'] += 1
                
                if was_updated and ai_result != input_customer_name:
                    input_df.at[index, "CUSTOMER_NAME"] = ai_result
                    logger.info(f"Row {index+1}: AI TWO-STAGE MATCH - '{row['CUSTOMER_NAME']}' -> '{ai_result}' (confidence: {ai_analysis['stage2']['confidence_score']}%)")
                    continue
        
        # No match found anywhere
        stats['no_matches'] += 1
        logger.warning(f"Row {index+1}: NO MATCH - '{input_customer_name}'")
    
    # Save the updated dataframe
    input_df.to_csv(input_file, index=False)
    
    # Save detailed AI analysis log
    if ai_analysis_log:
        ai_log_file = input_file.replace('.csv', '_ai_analysis_log.json')
        with open(ai_log_file, 'w') as f:
            json.dump(ai_analysis_log, f, indent=2)
        logger.info(f"Saved detailed AI analysis log to {ai_log_file}")
    
    # Log comprehensive statistics
    total_updated = stats['local_matches'] + stats['bc_matches'] + stats['ai_stage2_successful_matches']
    logger.info(f"Enhanced two-stage AI customer name matching completed:")
    logger.info(f"  Total rows: {len(input_df)}")
    logger.info(f"  Processed: {stats['processed']}")
    logger.info(f"  Skipped (empty): {stats['skipped_empty']}")
    logger.info(f"  Local DB matches: {stats['local_matches']}")
    logger.info(f"  Business Central matches: {stats['bc_matches']}")
    logger.info(f"  AI Stage 1 - kept current: {stats['ai_stage1_kept_current']}")
    logger.info(f"  AI Stage 1 - searched alternatives: {stats['ai_stage1_searched_alternatives']}")
    logger.info(f"  AI Stage 2 - successful matches: {stats['ai_stage2_successful_matches']}")
    logger.info(f"  AI Stage 2 - insufficient confidence: {stats['ai_stage2_insufficient_confidence']}")
    logger.info(f"  AI - no BC matches found: {stats['ai_no_bc_matches']}")
    logger.info(f"  Total updated: {total_updated}")
    logger.info(f"  No matches: {stats['no_matches']}")
    if stats['processed'] > 0:
        logger.info(f"  Overall match rate: {(total_updated/stats['processed']*100):.1f}%")
    
    print(f"Enhanced AI matching completed!")
    print(f"Updated {total_updated} customer names in {input_file}")
    print(f"Breakdown: Local={stats['local_matches']}, BC={stats['bc_matches']}, AI={stats['ai_stage2_successful_matches']}")
    print(f"AI Analysis: Kept current={stats['ai_stage1_kept_current']}, Searched={stats['ai_stage1_searched_alternatives']}")
    
    return stats


# Add wrapper function for backward compatibility
def update_customer_name_for_file(processed_file_path, local_threshold=0.95, bc_threshold=0.75):
    """Wrapper function for backward compatibility with existing workflow."""
    return update_customer_name_for_file_dual(processed_file_path, local_threshold, bc_threshold)