@echo off
set /a n=%4
set /a m=7
set /a MEMT=m+n

java -Xmx%MEMT%m -XX:MaxPermSize=8m -jar binaryfilesort-1.0-SNAPSHOT.jar %*