Get-CimInstance Win32_Process | Where-Object { $_.Name -like 'python*' -or $_.Name -like 'pythonw*' } | Select-Object ProcessId,Name,CommandLine | Format-List
