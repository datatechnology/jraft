@echo off

for /f %%i in ('dir /ad /b') do (
    rmdir /s /q %%i
)

if "%1" == "full" del dmprinter.jar
@echo on