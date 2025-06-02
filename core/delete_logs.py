import os
import datetime
import re

# Path to the logs directory
LOGS_DIR = "logs"
# Date format in filenames
DATE_FORMAT = "%d%m%Y_%H%M"
# Regex pattern to match the starting timestamp
DATE_REGEX = re.compile(r"^(\d{8}_\d{4})")

# Cutoff time: 3 days ago
cutoff_time = datetime.datetime.now() - datetime.timedelta(days=3)

# Ensure the directory exists
if not os.path.exists(LOGS_DIR):
    print(f"Directory {LOGS_DIR} does not exist.")
    exit(1)

# Loop through files in the log directory
for filename in os.listdir(LOGS_DIR):
    match = DATE_REGEX.match(filename)
    if match:
        date_str = match.group(1)
        try:
            # Adjust date format for filenames like 21052025_1132_finance_workflow.log
            file_time = datetime.datetime.strptime(date_str, "%d%m%Y_%H%M")
            if file_time < cutoff_time:
                file_path = os.path.join(LOGS_DIR, filename)
                os.remove(file_path)
                print(f"Deleted: {file_path}")
        except ValueError:
            print(f"Skipping {filename}: invalid date format")
