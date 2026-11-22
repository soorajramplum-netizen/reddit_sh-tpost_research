@echo off
cd /d "%~dp0"
title Reddit Downloader - progress
echo.
python reddit_download.py --status
echo.
echo  Safe to check while a download is running.
pause
