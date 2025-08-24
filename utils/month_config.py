#!/usr/bin/env python3
"""
Month Configuration Utility

This script makes it easy to change the month for all your bank transaction files.
Simply run this script and it will update all the sheet names in your config files.
"""

import json
import sys
from pathlib import Path
from datetime import datetime

def get_month_name(month_num):
    """Convert month number to month name abbreviation"""
    month_names = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }
    return month_names.get(month_num, "Jan")

def get_month_number(month_name):
    """Convert month name to month number"""
    month_mapping = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
    }
    return month_mapping.get(month_name, 1)

def update_month_config(new_month, year=None):
    """
    Update the month configuration in files_config.json
    
    Args:
        new_month: Month number (1-12) or month name (Jan, Feb, etc.)
        year: Year (e.g., 25 for 2025). If None, uses current year
    """
    config_path = Path(__file__).parent.parent / "config" / "files_config.json"
    
    if not config_path.exists():
        print(f"Error: Config file not found at {config_path}")
        return False
    
    try:
        # Load current config
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Determine month number and name
        if isinstance(new_month, str):
            month_num = get_month_number(new_month)
            month_name = new_month
        else:
            month_num = new_month
            month_name = get_month_name(new_month)
        
        # Determine year
        if year is None:
            year = datetime.now().year % 100  # Get last 2 digits
        
        # Create new sheet name
        new_sheet_name = f"{month_name}'{year:02d}"
        
        # Update all files in config
        updated_count = 0
        for file_config in config.get("files_to_download", []):
            old_sheet = file_config.get("sheet_name", "")
            file_config["sheet_name"] = new_sheet_name
            updated_count += 1
            print(f"Updated {file_config.get('name', 'Unknown file')}: {old_sheet} → {new_sheet_name}")
        
        # Save updated config
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"\n✅ Successfully updated {updated_count} files to use {new_sheet_name}")
        print(f"📅 Month: {month_name} ({month_num})")
        print(f"📅 Year: 20{year:02d}")
        
        return True
        
    except Exception as e:
        print(f"Error updating config: {e}")
        return False

def show_current_config():
    """Display the current month configuration"""
    config_path = Path(__file__).parent.parent / "config" / "files_config.json"
    
    if not config_path.exists():
        print("Config file not found")
        return
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        print("📋 Current Month Configuration:")
        print("=" * 40)
        
        for file_config in config.get("files_to_download", []):
            sheet_name = file_config.get("sheet_name", "Unknown")
            file_name = file_config.get("name", "Unknown file")
            print(f"📄 {file_name}")
            print(f"   Sheet: {sheet_name}")
            print()
        
        # Show what month number this corresponds to
        if config.get("files_to_download"):
            first_sheet = config["files_to_download"][0].get("sheet_name", "")
            month_num = get_month_number(first_sheet[:3])
            print(f"🔢 Month Number: {month_num}")
            
    except Exception as e:
        print(f"Error reading config: {e}")

def main():
    """Main function for command-line usage"""
    if len(sys.argv) == 1:
        # No arguments - show current config
        show_current_config()
        print("\n💡 Usage:")
        print("  python utils/month_config.py 8          # Set to August")
        print("  python utils/month_config.py Sep        # Set to September")
        print("  python utils/month_config.py 9 26       # Set to September 2026")
        return
    
    # Parse arguments
    month_arg = sys.argv[1]
    year_arg = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    # Convert month argument
    if month_arg.isdigit():
        month_num = int(month_arg)
        if month_num < 1 or month_num > 12:
            print("Error: Month must be between 1 and 12")
            return
    else:
        month_num = get_month_number(month_arg)
        if month_num == 1 and month_arg not in ["Jan", "jan"]:
            print(f"Error: Invalid month name '{month_arg}'. Use: Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec")
            return
    
    # Update configuration
    success = update_month_config(month_num, year_arg)
    
    if success:
        print("\n🔄 The date_utils.py will now automatically use the new month for date parsing!")
        print("💡 No need to manually edit any Python files.")

if __name__ == "__main__":
    main()
