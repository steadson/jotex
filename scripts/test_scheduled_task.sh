#!/bin/bash
# Shell script to test the scheduled task on Linux/macOS

# Change to the project directory
cd "$(dirname "$0")"

echo "Starting Test Scheduled Task at $(date)"

# Load environment variables from .env file
if [ -f .env ]; then
    echo "Loading environment variables from .env file"
    export $(grep -v '^#' .env | xargs)
else
    echo "Warning: .env file not found"
fi

# Run the test script
python test_scheduled_task.py

echo "Test completed at $(date)"
