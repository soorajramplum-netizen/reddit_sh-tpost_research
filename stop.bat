@echo off
cd /d "%~dp0"
echo stop> STOP
echo.
echo  Stop requested. The downloader will save its place and exit
echo  after the batch it is on (a few seconds, longer if the archive is slow).
echo  Run start.bat to carry on later.
echo.
pause
