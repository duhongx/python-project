# 数据库结构同步桌面客户端

面向 PostgreSQL 到 PostgreSQL / PostgreSQL 到 KingBase 的数据库结构比对与安全同步桌面客户端。

## 一期范围

- PyQt6 桌面客户端，不交付 Web/Streamlit 版本。
- 源端数据库：PostgreSQL。
- 目标端数据库：PostgreSQL、KingBase。
- 客户端本地数据库：SQLite。
- 默认管理员账号：`admin / cloudhis@2123`。
- 客户端内维护源端和目标端数据库连接配置，不再依赖手工编辑 `config.yaml`。
- 支持结构比对、SQL 预览、Dry Run、人工确认后同步。
- 一期只自动执行“目标端缺失的表字段”同步。
- 缺失 schema、缺失表、缺失视图、类型差异、可空性差异、默认值差异只展示，不自动执行。

## 开发运行

```bash
cd /Users/duhongx/work/db-schema-sync
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
./scripts/run_desktop.sh
```

如果未安装为 editable 包，也可以用：

```bash
PYTHONPATH=src python -m db_schema_sync_client.app
```

## 文档

- PRD：[docs/PRD-desktop-client-phase-1.md](docs/PRD-desktop-client-phase-1.md)
- 一期实现计划：[docs/plans/2026-04-08-desktop-client-phase-1.md](docs/plans/2026-04-08-desktop-client-phase-1.md)

## 安全边界

- 不在日志、报告、错误提示中输出数据库密码。
- 本地 SQLite 不保存用户明文密码。
- 同步前必须展示 SQL 并由用户二次确认。
- 一期禁止自动删除对象、自动修改已有字段、自动 `DROP VIEW CASCADE`。

## 启动方式

### macOS

```bash
cd /path/to/db-schema-sync
PYTHONPATH=src /Library/Developer/CommandLineTools/usr/bin/python3.9 -m db_schema_sync_client.app
```

或使用封装脚本（需系统 `python3` 为 3.9+，否则通过 `PYTHON=` 指定）：

```bash
PYTHON=/Library/Developer/CommandLineTools/usr/bin/python3.9 bash scripts/run_desktop.sh
```

### Windows

```powershell
# 先安装依赖（只需执行一次）
pip install PyQt6 psycopg2-binary keyring

# 启动应用
cd C:\path\to\db-schema-sync
set PYTHONPATH=src
python -m db_schema_sync_client.app
```

---

## 打包为独立客户端

### macOS（打包为 .app）

```bash
pip install pyinstaller
pyinstaller db_schema_sync_client.spec
# 产物：dist/db-schema-sync-client/
```

### Windows（打包为 .exe）

> .exe 只能在 Windows 机器上编译，不支持跨平台打包。

```powershell
pip install PyQt6 psycopg2-binary keyring pyinstaller
pyinstaller db_schema_sync_client.spec
# 产物：dist\db-schema-sync-client\  （整个目录即发布包）
```

### 自动构建（GitHub Actions）

推送 tag 后自动在 Windows runner 上编译并上传到 Release：

```bash
git tag v1.0.0
git push origin v1.0.0
```

构建配置：[.github/workflows/build-windows.yml](.github/workflows/build-windows.yml)

---

## 旧原型说明

根目录下的旧脚本和原型文件已迁移至 `src/legacy/`（`desktop_app.py`、`database.py`、`comparator.py`、`sync.py`、`report.py` 等），仅供参考。一期正式实现以 `src/db_schema_sync_client/` 和 `scripts/run_desktop.sh` 为准。
