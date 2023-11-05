@echo off
if "%*" == "" (
    echo server id is required
    goto exit
)

set curdir=%~dp0
if exist "%curdir%\server%1" (
    echo server %1 is already created
    goto exit
)

mkdir "%curdir%\server%1"
echo server.id=%1 > "%curdir%\server%1\config.properties"
echo {"logIndex":0,"lastLogIndex":0,"servers":[{"id": %1,"endpoint": "tcp://localhost:900%1"}]}> "%curdir%\server%1\cluster.json"
start "server%1" /D "%curdir%\server%1" java -jar %curdir%\kvstore.jar server "%curdir%\server%1" 800%1
@echo on

:exit