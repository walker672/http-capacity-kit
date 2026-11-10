@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo.
echo  http-capacity-kit selfcheck (offline, no packets sent)
echo.
python selfcheck.py
set RC=%ERRORLEVEL%
echo.
if not "%RC%"=="0" echo  FAILED with exit %RC%
pause
exit /b %RC%
