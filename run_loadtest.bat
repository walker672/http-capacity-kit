@echo off
chcp 65001 >nul
title Load Test
cd /d "%~dp0"

echo ============================================================
echo   Load Test  -  target / concurrency / duration / probe
echo   duration = 0   run forever, Ctrl+C stops and prints stats
echo   probe full     download whole file (real bandwidth test)
echo   segments N     N chunks per round (more data per round)
echo   scan a site    python a2o_loadtest.py --scan --url URL --min-size 1m
echo ============================================================
echo.

set "DEFURL=https://example.com/assets/app.js"
set /p URL=Target URL (comma-separated for multiple) [Enter = example asset]: 
if "%URL%"=="" set "URL=%DEFURL%"

set /p CONC=Concurrency [default 20]: 
if "%CONC%"=="" set CONC=20

set /p DUR=Duration seconds [0 = run until Ctrl+C, default 60]: 
if "%DUR%"=="" set DUR=60

set /p PROBE=Probe bytes [100 = head only, full = whole file, default 100]: 
if "%PROBE%"=="" set PROBE=100

set /p SEG=Segments per round [default 1]: 
if "%SEG%"=="" set SEG=1

echo.
echo Target   : %URL%
echo Load     : concurrency=%CONC%  duration=%DUR%s  ramp-up=10s
echo Probe    : %PROBE% bytes per request, %SEG% segment(s) per round
echo Abort    : Ctrl+C  (stats are printed before exit)
echo.

python a2o_loadtest.py --url "%URL%" --concurrency %CONC% --duration %DUR% --ramp-up 10 --probe-bytes "%PROBE%" --segments %SEG% --authorized
set RC=%ERRORLEVEL%

echo.
if not "%RC%"=="0" echo [exit %RC%] - see message above.
echo Done. Press any key to close.
pause >nul
