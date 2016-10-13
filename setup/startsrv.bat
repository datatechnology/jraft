@echo off
set curdir=%~dp0
if "%*" == "" (
    echo server id is required
    goto exit
)
set curdir=%~dp0
if "%1" == "client" (
    if not exist "%curdir%\%1" (
        echo client does not exist
        goto exit
    )
    
    if not "%2" == "" (
        if exist "%curdir%\server%2" (
            COPY /Y "%curdir%\server%2\cluster.json" "%curdir%\%1"
        )
    )
    
    start "client" /D "%curdir%\%1" java -jar %curdir%\dmprinter.jar client "%curdir%\%1"
    goto exit
)

if not exist "%curdir%\server%1" (
    echo server %1 is not found
    goto exit
)

start "server%1" /D "%curdir%\server%1" java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=600%1 -jar %curdir%\dmprinter.jar server "%curdir%\server%1" 800%1

:exit
@echo on