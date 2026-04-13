# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for db-schema-sync-client
# Build on Windows: pyinstaller db_schema_sync_client.spec

import sys
from pathlib import Path

block_cipher = None

a = Analysis(
    ['src/db_schema_sync_client/app.py'],
    pathex=['src'],
    binaries=[],
    datas=[
        ('src/db_schema_sync_client/resources/styles.qss', 'db_schema_sync_client/resources'),
    ],
    hiddenimports=[
        'psycopg2',
        'keyring.backends',
        'keyring.backends.Windows',
        'keyring.backends.fail',
        'PyQt6.sip',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='db-schema-sync-client',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,   # 不显示控制台窗口
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # icon='icon.ico',  # 取消注释并提供 .ico 文件可设置图标
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='db-schema-sync-client',
)
