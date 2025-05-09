# PowerShell script to create scheduled tasks for running the workflow at 10am, 2pm, and 6pm daily
# Run this script as Administrator

$workingDir = "C:\Works\JOTEX\Finance\JotexTest_finance_auto_watch"
$batchFile = "$workingDir\run_workflow.bat"
$taskName = "Finance Workflow"

# Create the scheduled tasks
$times = @("10:00", "14:00", "18:00")
$descriptions = @("10am", "2pm", "6pm")

for ($i = 0; $i -lt $times.Count; $i++) {
    $currentTaskName = "$taskName - $($descriptions[$i])"
    $time = $times[$i]
    
    Write-Host "Creating scheduled task: $currentTaskName to run at $time"
    
    # Delete the task if it already exists
    schtasks /query /tn $currentTaskName 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Task already exists. Deleting it..."
        schtasks /delete /tn $currentTaskName /f
    }
    
    # Create the new task
    schtasks /create /tn $currentTaskName /tr $batchFile /sc daily /st $time /ru "SYSTEM" /f
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Successfully created task: $currentTaskName"
    } else {
        Write-Host "Failed to create task: $currentTaskName"
    }
}

Write-Host "All scheduled tasks have been created."
Write-Host "To view the tasks, open Task Scheduler or run: schtasks /query /fo LIST /v | findstr /i 'Finance Workflow'"
