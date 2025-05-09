@echo off
echo Starting Finance Workflow at %date% %time%
cd /d C:\Works\JOTEX\Finance\JotexTest_finance_auto_watch

:: Load environment variables from .env file
for /F "tokens=*" %%A in (.env) do (
    set %%A
)

:: Run the workflow
python core/workflows.py
echo Workflow completed at %date% %time%
