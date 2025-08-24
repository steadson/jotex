import re
import pandas as pd
from datetime import datetime
from dateutil import parser
import json
from pathlib import Path

def get_current_month_from_config():
    """
    Automatically detect the current month from files_config.json
    Returns the month number (1-12) based on the sheet_name in config
    """
    try:
        config_path = Path(__file__).parent.parent / "config" / "files_config.json"
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Get the first file's sheet_name to determine month
            if config.get("files_to_download") and len(config["files_to_download"]) > 0:
                sheet_name = config["files_to_download"][0].get("sheet_name", "")
                
                # Parse month names to numbers
                month_mapping = {
                    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
                }
                
                for month_name, month_num in month_mapping.items():
                    if month_name in sheet_name:
                        return month_num
                        
        # Fallback to current month if config parsing fails
        return datetime.now().month
    except Exception:
        # Fallback to current month if any error occurs
        return datetime.now().month

def convert_date(date_string, expected_format=None, month_value=None):
    """
    Parse various date formats to YYYY-MM-DD, return '' if invalid.
    Handles:
    - 'YYYY-DD-MM' (day before month)
    - 'YYYY-MM-DD' (month before day) 
    - 'DD/MM/YYYY' (day before month)
    - 'MM/DD/YYYY' (month before day)
    - Removes 'MY (UTC...' suffix if present.
    
    Args:
        date_string: The date string to convert
        expected_format: Optional hint - 'YYYY-DD-MM', 'YYYY-MM-DD', or None for auto-detect
        month_value: Optional - actual month number (1-12). If None, automatically detected from config
    """
    # Auto-detect month from config if not specified
    if month_value is None:
        month_value = get_current_month_from_config()
    
    if pd.isna(date_string) or not str(date_string).strip():
        return ''
    try:
        s = str(date_string).strip()
        if 'MY (UTC' in s:
            s = s.split('MY')[0].strip()
        
        # If month value is specified, use that to determine format
        if month_value is not None and re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', s):
            parts = s.split('-')
            year, first_num, second_num = int(parts[0]), int(parts[1]), int(parts[2])
            
            if first_num == month_value:
                # Month is in 2nd position: YYYY-MM-DD
                try:
                    return datetime.strptime(s, '%Y-%m-%d').strftime('%Y-%m-%d')
                except ValueError:
                    pass
            elif second_num == month_value:
                # Month is in 3rd position: YYYY-DD-MM
                try:
                    return datetime.strptime(s, '%Y-%d-%m').strftime('%Y-%m-%d')
                except ValueError:
                    pass
        
        # Handle XX/XX/YYYY formats with month_value
        if month_value is not None and re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', s):
            parts = s.split('/')
            first_num, second_num, year = int(parts[0]), int(parts[1]), int(parts[2])
            
            if first_num == month_value:
                # Month is in 1st position: MM/DD/YYYY
                try:
                    return datetime.strptime(s, '%m/%d/%Y').strftime('%Y-%m-%d')
                except ValueError:
                    pass
            elif second_num == month_value:
                # Month is in 2nd position: DD/MM/YYYY
                try:
                    return datetime.strptime(s, '%d/%m/%Y').strftime('%Y-%m-%d')
                except ValueError:
                    pass
        
        # If expected format is specified, try that first
        if expected_format == 'YYYY-DD-MM':
            try:
                return datetime.strptime(s, '%Y-%d-%m').strftime('%Y-%m-%d')
            except ValueError:
                pass
        elif expected_format == 'YYYY-MM-DD':
            try:
                return datetime.strptime(s, '%Y-%m-%d').strftime('%Y-%m-%d')
            except ValueError:
                pass
        
        # Auto-detect format for YYYY-XX-YY patterns
        if re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', s):
            parts = s.split('-')
            year, first_num, second_num = int(parts[0]), int(parts[1]), int(parts[2])
            
            # If first number > 12, it must be day (YYYY-DD-MM)
            if first_num > 12:
                try:
                    return datetime.strptime(s, '%Y-%d-%m').strftime('%Y-%m-%d')
                except ValueError:
                    pass
            # If second number > 12, it must be day (YYYY-MM-DD)  
            elif second_num > 12:
                try:
                    return datetime.strptime(s, '%Y-%m-%d').strftime('%Y-%m-%d')
                except ValueError:
                    pass
            # If both <= 12, ambiguous - try both formats
            else:
                # Try YYYY-MM-DD first (more common)
                try:
                    return datetime.strptime(s, '%Y-%m-%d').strftime('%Y-%m-%d')
                except ValueError:
                    pass
                # Fallback to YYYY-DD-MM
                try:
                    return datetime.strptime(s, '%Y-%d-%m').strftime('%Y-%m-%d')
                except ValueError:
                    pass
            
        # Auto-detect format for XX/XX/YYYY patterns  
        if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', s):
            parts = s.split('/')
            first_num, second_num, year = int(parts[0]), int(parts[1]), int(parts[2])
            
            # If first number > 12, it must be day (DD/MM/YYYY)
            if first_num > 12:
                try:
                    return datetime.strptime(s, '%d/%m/%Y').strftime('%Y-%m-%d')
                except ValueError:
                    pass
            # If second number > 12, it must be day (MM/DD/YYYY)
            elif second_num > 12:
                try:
                    return datetime.strptime(s, '%m/%d/%Y').strftime('%Y-%m-%d')
                except ValueError:
                    pass
            # If both <= 12, ambiguous - try DD/MM/YYYY first (more common)
            else:
                try:
                    return datetime.strptime(s, '%d/%m/%Y').strftime('%Y-%m-%d')
                except ValueError:
                    pass
                # Fallback to MM/DD/YYYY
                try:
                    return datetime.strptime(s, '%m/%d/%Y').strftime('%Y-%m-%d')
                except ValueError:
                    pass
            
        # Last fallback: dateutil parser
        return parser.parse(s).strftime('%Y-%m-%d')
    except Exception:
        return ''

# Convenience function to get current month for debugging
def get_current_month():
    """Get the current month number (1-12) being used for date parsing"""
    return get_current_month_from_config() 