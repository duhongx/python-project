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

## 旧原型说明

仓库根目录中的旧脚本和原型文件仍保留作迁移参考，包括 `desktop_app.py`、`database.py`、`comparator.py`、`sync.py`、`report.py`。Web/Streamlit 相关原型文件已清理。一期正式实现以 `src/db_schema_sync_client/` 和 `scripts/run_desktop.sh` 为准。
