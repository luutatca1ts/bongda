@echo off
echo [1/3] Stopping all Python processes...
taskkill /IM python.exe /F 2>nul
timeout /t 3 /nobreak >nul

echo [2/3] Clearing Telegram webhook...
C:\Users\Admin\AppData\Local\Programs\Python\Python314\python.exe -c "import requests; requests.post('https://api.telegram.org/bot8293674971:AAEdqhgWqmRYVvymUhWqAGVj2GyVUzbvOkM/deleteWebhook',json={'drop_pending_updates':True}); print('OK')"
timeout /t 1 /nobreak >nul

echo [3/3] Starting bot...
cd /d %~dp0
start /b C:\Users\Admin\AppData\Local\Programs\Python\Python314\python.exe main.py > bot_run.log 2>&1
echo.
echo Bot restarted! Check bot_run.log
echo Press any key to close...
pause >nul
