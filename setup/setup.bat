@echo off
set curdir=%~dp0
if "%1" == "dummy" goto setupDummy

for %%i in (1 2 3) do (
    mkdir "%curdir%\server%%i"
    copy /Y init-cluster.json "%curdir%\server%%i\cluster.json"
    echo server.id=%%i> "%curdir%\server%%i\config.properties"
    echo start server%%i
    start "server%%i" /D "%curdir%\server%%i" java -jar %curdir%\dmprinter.jar server "%curdir%\server%%i" 800%%i
)

echo start a client
mkdir client
copy /Y init-cluster.json "%curdir%\client\cluster.json"
copy /Y "%curdir%\server1\config.properties" "%curdir%\client\config.properties"
start "client" /D "%curdir%\client" java -jar %curdir%\dmprinter.jar client "%curdir%\client"
@echo on