@echo off
REM ─────────────────────────────────────────────────────────────
REM  GameStream — PyInstaller Build Script
REM  Run from the project folder: build.bat
REM ─────────────────────────────────────────────────────────────

echo.
echo  ╔══════════════════════════════════╗
echo  ║   GameStream Builder             ║
echo  ╚══════════════════════════════════╝
echo.

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found in PATH
    pause & exit /b 1
)

REM Install / upgrade PyInstaller
echo [1/3] Installing PyInstaller...
python -m pip install pyinstaller --upgrade -q
if errorlevel 1 (
    echo [ERROR] Could not install PyInstaller
    pause & exit /b 1
)

REM Install all dependencies
echo [2/3] Installing dependencies...
python -m pip install PyQt6 pygame pyaudiowpatch vgamepad pywin32 -q

REM Clean previous build
echo [3/3] Building exe...
if exist build rmdir /s /q build
if exist dist  rmdir /s /q dist

REM Run PyInstaller with the spec file
pyinstaller gamestream.spec --noconfirm --clean

if errorlevel 1 (
    echo.
    echo [ERROR] Build failed — see output above
    pause & exit /b 1
)

echo.
echo  ╔══════════════════════════════════════════════╗
echo  ║   Build complete!                            ║
echo  ║   Output: dist\GameStream.exe                ║
echo  ╚══════════════════════════════════════════════╝
echo.

REM Open output folder
explorer dist

pause
