#!/bin/bash
# Shell script to run the finance workflow on Linux/macOS

# Change to the project directory
cd "$(dirname "$0")"

echo "Starting Finance Workflow at $(date)"

# Load environment variables from .env file
if [ -f .env ]; then
    echo "Loading environment variables from .env file"
    export $(grep -v '^#' .env | xargs)
else
    echo "Warning: .env file not found"
fi

# Run the workflow
python core/workflows.py
echo "Workflow completed at $(date)"
