@echo off
setlocal
cd /d "%~dp0"
title Reddit Downloader

echo.
echo  Reddit downloader. Safe to close any time: it resumes where it stopped.
echo  To stop cleanly, create a file named STOP in this folder.
echo.
echo  Posts for every chosen subreddit come first, then comments for rich posts only.
echo  Which priority? 1 = categories, 2 = + cohorts, 3 = + general and big cities, 4 = everything
set /p PRI= Priority [1]:
if "%PRI%"=="" set PRI=1
set /p SINCE= Start date [2023-01-01]:
if "%SINCE%"=="" set SINCE=2023-01-01

if exist STOP del STOP
python -u reddit_download.py --priority %PRI% --since %SINCE%
echo.
pause
