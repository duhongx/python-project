#!/bin/bash
# 启动数据库结构同步工具桌面应用

cd "$(dirname "$0")"
source venv/bin/activate
python desktop_app.py
