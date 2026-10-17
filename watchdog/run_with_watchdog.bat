@echo off
chcp 65001 >nul
rem ============================================================
rem  AstrBot 看门狗启动脚本（配合 astrbot_plugin_perf_guard 使用）
rem
rem  用法：
rem   1. 把本文件放到 AstrBot 根目录（与 main.py 同一层）
rem   2. 以后用它启动 AstrBot：双击本文件（或把它的快捷方式放进 shell:startup）
rem   3. 不要再直接用原来的方式启动，否则看门狗无效
rem
rem  作用：AstrBot 退出后自动重新拉起，使插件的“自动重启”功能生效
rem  停止：直接关掉本窗口，或按 Ctrl+C
rem ============================================================
setlocal
cd /d "%~dp0"

rem 标记：告诉插件“我在看门狗里运行”，插件才会执行重启
set "MF_PERF_WATCHDOG=1"
rem 写一个标记文件，防止某些启动方式丢失环境变量
if not exist "data" mkdir "data"
echo %DATE% %TIME% > "data\.perf_guard_watchdog"

:loop
echo.
echo [看门狗] %DATE% %TIME% 正在启动 AstrBot...
python main.py
set "RC=%errorlevel%"

rem 退出码 0 = 插件请求的重启（快速拉起）；非 0 = 崩溃（多等一会儿，避免疯狂重启）
if "%RC%"=="0" (
  set "DELAY=3"
) else (
  set "DELAY=30"
  echo [看门狗] AstrBot 异常退出（代码 %RC%），%DELAY% 秒后重试...
  timeout /t %DELAY% /nobreak >nul
  goto loop
)
echo [看门狗] AstrBot 已退出（重启请求），%DELAY% 秒后重新启动...
timeout /t %DELAY% /nobreak >nul
goto loop
