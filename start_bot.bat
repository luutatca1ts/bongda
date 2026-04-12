@echo off
cd /d "%~dp0"
start "BotRun" /B "" "C:\Users\Admin\AppData\Local\Programs\Python\Python314\python.exe" -m src.bot.telegram_bot > bot.log 2>&1
exit /b 0
