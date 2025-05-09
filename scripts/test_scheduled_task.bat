@echo off
echo Starting Test Scheduled Task at %date% %time%
cd /d C:\Works\JOTEX\Finance\JotexTest_finance_auto_watch

:: Load environment variables from .env file
for /F "tokens=*" %%A in (.env) do (
    set %%A
)

:: Run the test script
python test_scheduled_task.py
echo Test completed at %date% %time%
