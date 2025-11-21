@echo off
:: Configurar colores
color 0A

:: Mensaje de inicio
echo ============================
echo   Creacion de carpetas
echo ============================

:: Lista de carpetas en vertical usando índices
setlocal enabledelayedexpansion
set count=0

set /a count+=1
set "folder!count!=config"

set /a count+=1
set "folder!count!=docs"

set /a count+=1
set "folder!count!=logs"

set /a count+=1
set "folder!count!=src"

set /a count+=1
set "folder!count!=sql"

set /a count+=1
set "folder!count!=data"

set /a count+=1
set "folder!count!=html"

set /a count+=1
set "folder!count!=controllers"

set /a count+=1
set "folder!count!=data\detalle_agente"

set /a count+=1
set "folder!count!=data\img"

set /a count+=1
set "folder!count!=data\img\pdc"

set /a count+=1
set "folder!count!=data\img\outlook"

set /a count+=1
set "folder!count!=data\img\load_vcdl"

set /a count+=1
set "folder!count!=data\txt"

set /a count+=1
set "folder!count!=data\txt\outlook"

set /a count+=1
set "folder!count!=data\txt\outlook\cos_performance"

set /a count+=1
set "folder!count!=data\txt\pdc"

:: Crear carpetas con validación
for /l %%i in (1,1,!count!) do (
    set "currentFolder=!folder%%i!"
    if exist "!currentFolder!" (
        echo La carpeta "!currentFolder!" ya existe.
    ) else (
        mkdir "!currentFolder!"
        echo Carpeta "!currentFolder!" creada correctamente.
    )
)

:: Verificar carpetas vacías y crear all.txt si están vacías
echo ============================
echo   Verificando carpetas vacias
echo ============================
for /l %%i in (1,1,!count!) do (
    set "currentFolder=!folder%%i!"
    dir /b "!currentFolder!" | findstr . >nul
    if errorlevel 1 (
        echo La carpeta "!currentFolder!" esta vacia. Creando all.txt...
        echo.>"!currentFolder!\all.txt"
    ) else (
        echo La carpeta "!currentFolder!" NO esta vacia.
    )
)

echo ============================
echo Proceso finalizado.
echo ============================

pause
exit /b
