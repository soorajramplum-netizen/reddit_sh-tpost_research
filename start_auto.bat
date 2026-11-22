@echo off
setlocal
cd /d "%~dp0"
title Reddit Downloader (auto)

rem Started by Windows at log-in (task "Reddit Downloader"). Same run as start.bat, without the questions:
rem priority 4 (all 103 subreddits) since 2023-01-01. Safe to close any time: it resumes where it stopped.
rem Only one copy can run; a second one exits straight away. A STOP file in this folder is left alone,
rem so if you stopped it on purpose it stays stopped until you delete STOP.

echo.
echo  Reddit downloader (auto-start at log-in). Safe to close any time: it resumes where it stopped.
echo.
if exist STOP (
  echo  STOP file present, so not starting. Delete STOP to let it run again.
  timeout /t 15 >nul
  exit /b
)
python -u reddit_download.py --priority 4 --since 2023-01-01
echo.
echo  Finished or stopped. This window closes in 60 seconds.
timeout /t 60 >nul
