# Scheduled Tasks for Finance Workflow

This document explains how to set up scheduled tasks to run the finance workflow at 10am, 2pm, and 6pm daily.

## Files Created

1. **run_workflow.bat** - Batch file that runs the workflow (Windows)
2. **setup_scheduled_tasks.ps1** - PowerShell script to create the scheduled tasks (Windows)
3. **run_workflow.sh** - Shell script that runs the workflow (Linux/macOS)
4. **crontab_config.txt** - Crontab configuration for scheduled execution (Linux/macOS)
5. **logs/** - Directory where workflow logs will be stored

## Setup Instructions for Windows

### Option 1: Automated Setup (Recommended)

1. Right-click on **setup_scheduled_tasks.ps1** and select "Run with PowerShell as Administrator"
2. The script will create three scheduled tasks:
   - Finance Workflow - 10am (runs at 10:00 AM daily)
   - Finance Workflow - 2pm (runs at 2:00 PM daily)
   - Finance Workflow - 6pm (runs at 6:00 PM daily)

### Option 2: Manual Setup

If you prefer to set up the tasks manually using Windows Task Scheduler:

1. Open Task Scheduler (search for it in the Start menu)
2. Click "Create Basic Task"
3. Name it "Finance Workflow - 10am"
4. Select "Daily" trigger
5. Set start time to 10:00 AM
6. Select "Start a program" as the action
7. Browse to the `run_workflow.bat` file
8. Set "Start in" to `C:\Works\JOTEX\Finance\JotexTest_finance_auto_watch`
9. Complete the wizard
10. Repeat steps 2-9 for 2:00 PM and 6:00 PM tasks

## Verifying the Setup

To verify that the tasks have been created:

1. Open Task Scheduler
2. Look for tasks named "Finance Workflow - 10am", "Finance Workflow - 2pm", and "Finance Workflow - 6pm"
3. Or run this command in PowerShell:
   ```
   schtasks /query /fo LIST /v | findstr /i "Finance Workflow"
   ```

## Monitoring Execution

The workflow now includes logging to help monitor execution:

1. Logs are stored in the `logs` directory
2. Each day gets its own log file named `workflow_YYYYMMDD.log`
3. The logs include:
   - When the workflow starts and completes
   - When each step starts and completes
   - Any errors that occur during execution

## Environment Variables

The workflow requires environment variables defined in the `.env` file. Both the batch script (Windows) and shell script (Linux/macOS) have been configured to load these environment variables before running the workflow:

- **Windows**: The batch file reads the `.env` file and sets the environment variables using a for loop.
- **Linux/macOS**: The shell script sources the `.env` file using the export command.

This ensures that all the necessary credentials and configuration settings are available to the workflow when it runs as a scheduled task.

## Testing Scripts

Several test scripts have been provided to help verify that the scheduled tasks are set up correctly:

### 1. Test Environment Variables

This script verifies that environment variables are being loaded correctly:

```bash
# Windows (after running run_workflow.bat)
python test_env_vars.py

# Linux/macOS (after running run_workflow.sh)
python test_env_vars.py
```

This script checks if all required environment variables are set and displays the values of non-sensitive variables for verification.

### 2. Test Scheduled Task Execution

These scripts simulate the workflow execution without actually running the real workflow steps:

```bash
# Windows
test_scheduled_task.bat

# Linux/macOS
./test_scheduled_task.sh
```

This is useful for testing that the scheduled task mechanism works correctly without affecting real data. The test:

- Verifies that the script can run in the scheduled environment
- Checks if environment variables are loaded correctly
- Simulates each step of the workflow
- Creates a log file in the logs directory

You can use these test scripts to verify your scheduled task setup before running the actual workflow.

## Troubleshooting

If the scheduled tasks are not running as expected:

1. Check the Task Scheduler history to see if the task was triggered
2. Check the logs in the `logs` directory for any error messages
3. Try running `run_workflow.bat` (Windows) or `run_workflow.sh` (Linux/macOS) manually to see if it works
4. Run `test_env_vars.py` to verify that environment variables are being loaded correctly
5. Ensure that the user account running the task has the necessary permissions
6. Verify that the `.env` file is present and contains the correct environment variables

## Modifying the Schedule

To change the schedule:

1. Open Task Scheduler
2. Find the task you want to modify
3. Right-click and select "Properties"
4. Go to the "Triggers" tab
5. Select the trigger and click "Edit"
6. Change the time as needed
7. Click "OK" to save the changes

Alternatively, you can modify the times in `setup_scheduled_tasks.ps1` and run it again.

## Setup Instructions for Linux/macOS

### Step 1: Make the shell script executable

```bash
chmod +x run_workflow.sh
```

### Step 2: Edit the crontab configuration

Open `crontab_config.txt` and update the `WORKFLOW_DIR` variable to point to your project directory:

```
WORKFLOW_DIR=/actual/path/to/JotexTest_finance_auto_watch
```

### Step 3: Install the crontab

```bash
crontab crontab_config.txt
```

### Step 4: Verify the crontab installation

```bash
crontab -l
```

You should see the three scheduled tasks for 10am, 2pm, and 6pm.

## Verifying Cron Execution

To verify that the cron jobs are running:

1. Check the log files in the `logs` directory
2. Check the cron log file at `logs/cron_output.log`
3. On some systems, you can check the system cron logs:
   ```bash
   grep CRON /var/log/syslog
   ```

## Modifying the Cron Schedule

To change the schedule:

1. Edit the `crontab_config.txt` file
2. Update the time values (the first two fields in each line)
3. Reinstall the crontab:
   ```bash
   crontab crontab_config.txt
   ```
