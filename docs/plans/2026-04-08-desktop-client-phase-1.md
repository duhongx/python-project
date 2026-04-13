# Desktop Client Phase 1 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build the first production-oriented PyQt6 desktop client for PostgreSQL -> PostgreSQL and PostgreSQL -> KingBase schema comparison and safe missing-field synchronization.

**Architecture:** Move the project from script/prototype files into a `src/db_schema_sync_client` package. Keep database metadata reading, comparison, SQL generation, sync execution, local SQLite persistence, and PyQt UI as separate layers so the core behavior is testable without launching the GUI. Phase 1 only auto-executes target-side missing table fields; other differences are displayed but not executed.

**Tech Stack:** Python 3.9+, PyQt6, psycopg2, SQLite via stdlib `sqlite3`, password hashing via stdlib `hashlib.pbkdf2_hmac`, optional OS credential storage via `keyring`, tests via `pytest`.

## Current Context

- Current workspace path: `/Users/duhongx/work/db-schema-sync`.
- Current project is not a git repository. If git is initialized before implementation, run the commit steps. If not, skip commit steps and keep changes grouped by task.
- Existing core scripts: `database.py`, `comparator.py`, `sync.py`, `report.py`, `main.py`.
- Existing desktop prototype: `desktop_app.py`.
- Web prototypes have been removed (previously: `web_app*.py`, `start_web.sh`, `test_tree.py`).
- Product requirements are in `docs/PRD-desktop-client-phase-1.md`.

## Target File Layout

```text
db-schema-sync/
├── pyproject.toml
├── README.md
├── config.example.yaml
├── scripts/
│   └── run_desktop.sh
├── src/
│   └── db_schema_sync_client/
│       ├── __init__.py
│       ├── app.py
│       ├── paths.py
│       ├── domain/
│       │   ├── __init__.py
│       │   ├── diff.py
│       │   └── models.py
│       ├── infrastructure/
│       │   ├── __init__.py
│       │   ├── app_store.py
│       │   ├── credentials.py
│       │   ├── db_connection.py
│       │   └── db_metadata.py
│       ├── services/
│       │   ├── __init__.py
│       │   ├── comparator.py
│       │   ├── dialects.py
│       │   ├── report_service.py
│       │   ├── sql_generator.py
│       │   └── sync_executor.py
│       ├── ui/
│       │   ├── __init__.py
│       │   ├── comparison_panel.py
│       │   ├── config_dialog.py
│       │   ├── database_tree.py
│       │   ├── execution_result_dialog.py
│       │   ├── login_dialog.py
│       │   ├── main_window.py
│       │   ├── sql_preview_dialog.py
│       │   └── workers.py
│       └── resources/
│           └── styles.qss
└── tests/
    ├── unit/
    │   ├── test_app_store.py
    │   ├── test_auth.py
    │   ├── test_comparator.py
    │   ├── test_dialects.py
    │   └── test_sql_generator.py
    └── integration/
        └── test_sync_executor.py
```

## Development Rules

- Do not edit or extend Web prototype files for Phase 1.
- Do not let UI classes directly read PostgreSQL metadata, compare schemas, or build SQL.
- Do not auto-execute schema/table/view creation, type changes, nullable changes, default changes, deletes, or `DROP VIEW CASCADE`.
- Treat database passwords as sensitive: never log them, never put them in exported reports, never render them back in UI.
- Prefer TDD for service-layer and persistence-layer behavior before wiring UI.

## Task 1: Project Scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `scripts/run_desktop.sh`
- Create: `src/db_schema_sync_client/__init__.py`
- Create: `src/db_schema_sync_client/app.py`
- Create: `src/db_schema_sync_client/paths.py`
- Create package `__init__.py` files under `domain/`, `infrastructure/`, `services/`, `ui/`
- Create: `src/db_schema_sync_client/resources/styles.qss`
- Modify: `README.md`

**Step 1: Create package directories**

Create the target `src/`, `scripts/`, and `tests/` directory structure.

**Step 2: Add `pyproject.toml`**

Use dependencies:

```toml
[project]
name = "db-schema-sync-client"
version = "0.1.0"
requires-python = ">=3.9"
dependencies = [
  "PyQt6>=6.0.0",
  "psycopg2-binary>=2.9.0",
  "keyring>=25.0.0",
]

[project.optional-dependencies]
dev = [
  "pytest>=8.0.0",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]
```

**Step 3: Add minimal app entry**

Implement `src/db_schema_sync_client/app.py` with a placeholder `main()` that imports PyQt only when invoked.

```python
def main() -> int:
    from PyQt6.QtWidgets import QApplication
    import sys

    app = QApplication(sys.argv)
    return app.exec()
```

**Step 4: Add run script**

Create `scripts/run_desktop.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD/src"
python -m db_schema_sync_client.app
```

Make it executable:

```bash
chmod +x scripts/run_desktop.sh
```

**Step 5: Verify imports**

Run:

```bash
PYTHONPATH=src python -m db_schema_sync_client.app
```

Expected: app starts and exits only when the GUI event loop is closed. If running headless, use:

```bash
PYTHONPATH=src python -c "import db_schema_sync_client.app as app; print(app.__name__)"
```

Expected: prints `db_schema_sync_client.app`.

**Step 6: Commit**

If git is initialized:

```bash
git add pyproject.toml scripts src README.md
git commit -m "chore: scaffold desktop client package"
```

## Task 2: Domain Models

**Files:**
- Create: `src/db_schema_sync_client/domain/models.py`
- Create: `src/db_schema_sync_client/domain/diff.py`
- Test: `tests/unit/test_comparator.py`
- Test: `tests/unit/test_sql_generator.py`

**Step 1: Define core enums and dataclasses**

Create models for database type, object type, connection profile, schema object, column definition, and comparison status.

Minimum model names:

- `DatabaseType`
- `ObjectType`
- `ConnectionRole`
- `ConnectionProfile`
- `ColumnDefinition`
- `TableDefinition`
- `SchemaSnapshot`
- `DiffStatus`
- `DiffCategory`
- `ColumnDiff`
- `ObjectDiff`
- `SchemaDiff`

**Step 2: Add model behavior tests**

Add tests that assert:

- `DatabaseType.POSTGRESQL.value == "postgresql"`.
- `DatabaseType.KINGBASE.value == "kingbase"`.
- `DiffCategory.AUTO_SYNCABLE.value == "auto_syncable"`.
- a missing target column is represented with source column populated and target column `None`.

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_comparator.py tests/unit/test_sql_generator.py -v
```

Expected before implementation: FAIL with import errors or missing names.

**Step 3: Implement minimal domain models**

Use frozen dataclasses where practical. Keep domain models free from PyQt and psycopg2 imports.

**Step 4: Verify tests pass**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_comparator.py tests/unit/test_sql_generator.py -v
```

Expected: PASS for domain-model tests. Comparator and SQL-generator tests can remain skipped or limited until their tasks.

**Step 5: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/domain tests/unit
git commit -m "feat: add schema sync domain models"
```

## Task 3: Local SQLite Store and Authentication

**Files:**
- Create: `src/db_schema_sync_client/infrastructure/app_store.py`
- Create: `src/db_schema_sync_client/infrastructure/credentials.py`
- Modify: `src/db_schema_sync_client/paths.py`
- Test: `tests/unit/test_app_store.py`
- Test: `tests/unit/test_auth.py`

**Step 1: Write failing auth tests**

Add tests for:

- first initialization creates a `users` table.
- first initialization inserts default user `admin`.
- stored password is not equal to `cloudhis@2123`.
- `verify_user("admin", "cloudhis@2123")` returns `True`.
- wrong password returns `False`.
- running initialization twice does not duplicate `admin`.

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_app_store.py tests/unit/test_auth.py -v
```

Expected: FAIL with missing `AppStore`.

**Step 2: Implement password hashing**

Use stdlib PBKDF2:

- hash format: `pbkdf2_sha256$<iterations>$<salt_hex>$<hash_hex>`.
- default iterations: at least `200_000`.
- use `hmac.compare_digest` for verification.

**Step 3: Implement SQLite migrations**

`AppStore.initialize()` must create:

- `users`
- `connection_profiles`
- `compare_tasks`
- `compare_results`
- `sync_runs`
- `sync_statements`

Store timestamps as ISO strings.

**Step 4: Implement default admin seeding**

If username `admin` does not exist, insert it with hash of `cloudhis@2123`.

**Step 5: Verify tests pass**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_app_store.py tests/unit/test_auth.py -v
```

Expected: PASS.

**Step 6: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/infrastructure src/db_schema_sync_client/paths.py tests/unit/test_app_store.py tests/unit/test_auth.py
git commit -m "feat: add local sqlite store and admin login"
```

## Task 4: Connection Profiles and Credential Storage

**Files:**
- Modify: `src/db_schema_sync_client/infrastructure/app_store.py`
- Modify: `src/db_schema_sync_client/infrastructure/credentials.py`
- Test: `tests/unit/test_app_store.py`

**Step 1: Write failing profile tests**

Add tests for:

- saving a source PostgreSQL profile.
- saving a target PostgreSQL profile.
- saving a target KingBase profile.
- rejecting source KingBase profile.
- rejecting target database type outside PostgreSQL/KingBase.
- retrieving profiles without returning plaintext password.
- default source and default target can be set.

**Step 2: Implement credential abstraction**

Implement a `CredentialStore` protocol and two implementations:

- `KeyringCredentialStore` for runtime use.
- `InMemoryCredentialStore` for tests.

SQLite stores only a credential key, not the raw password.

**Step 3: Implement profile persistence**

Persist:

- `name`
- `role`
- `db_type`
- `host`
- `port`
- `database`
- `username`
- `credential_key`
- `schema_prefix`
- `owner_prefix`
- `is_default`
- `last_test_status`
- `last_test_message`

**Step 4: Verify tests pass**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_app_store.py -v
```

Expected: PASS.

**Step 5: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/infrastructure tests/unit/test_app_store.py
git commit -m "feat: add connection profile persistence"
```

## Task 5: Database Connection and Metadata Reader

**Files:**
- Create: `src/db_schema_sync_client/infrastructure/db_connection.py`
- Create: `src/db_schema_sync_client/infrastructure/db_metadata.py`
- Test: `tests/unit/test_metadata_reader.py`

**Step 1: Write metadata row parsing tests**

Avoid real database dependency in unit tests. Test functions that convert rows to domain models:

- table row maps to `ObjectType.TABLE`.
- view row maps to `ObjectType.VIEW`.
- column row maps to `ColumnDefinition`.
- numeric precision and scale are preserved.
- default value is preserved.
- `nextval(...)` default marks the column as sequence-related.

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_metadata_reader.py -v
```

Expected: FAIL with missing reader functions.

**Step 2: Implement `DatabaseConnectionFactory`**

Use psycopg2 for both PostgreSQL and KingBase.

Expose:

- `test_connection(profile, password) -> ConnectionTestResult`
- `connect(profile, password)`

Do not log passwords.

**Step 3: Implement `MetadataReader`**

Expose:

- `load_snapshot(profile, password, filters) -> SchemaSnapshot`

Use `information_schema.tables`, `information_schema.columns`, and `pg_namespace` with owner/schema filters.

**Step 4: Verify unit tests pass**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_metadata_reader.py -v
```

Expected: PASS.

**Step 5: Manual database test**

Only if a safe test database is available:

```bash
PYTHONPATH=src python -m db_schema_sync_client.devtools.test_connection
```

Expected: connection succeeds and prints schema/table counts without printing passwords.

**Step 6: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/infrastructure tests/unit/test_metadata_reader.py
git commit -m "feat: add database metadata reader"
```

## Task 6: Schema Comparator

**Files:**
- Create: `src/db_schema_sync_client/services/comparator.py`
- Test: `tests/unit/test_comparator.py`

**Step 1: Write failing comparator tests**

Cover:

- missing schema produces `ONLY_SOURCE` and `ONLY_HINT`.
- missing table produces `ONLY_SOURCE` and `ONLY_HINT`.
- missing view produces `ONLY_SOURCE` and `ONLY_HINT`.
- missing target table field produces `AUTO_SYNCABLE`.
- target-only field produces `ONLY_HINT`.
- type mismatch produces `MANUAL_REQUIRED`.
- nullable mismatch produces `MANUAL_REQUIRED`.
- default mismatch produces `MANUAL_REQUIRED`.

**Step 2: Run tests to verify failure**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_comparator.py -v
```

Expected: FAIL with missing comparator implementation.

**Step 3: Implement comparator**

Comparator input:

- source `SchemaSnapshot`
- target `SchemaSnapshot`

Comparator output:

- `SchemaDiff` containing object diffs and column diffs.

Important rule:

- Only missing target table fields are `AUTO_SYNCABLE`.
- Missing target view fields are `MANUAL_REQUIRED` or `ONLY_HINT`; do not auto-sync views in Phase 1.

**Step 4: Verify tests pass**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_comparator.py -v
```

Expected: PASS.

**Step 5: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/services/comparator.py tests/unit/test_comparator.py
git commit -m "feat: add phase one schema comparator"
```

## Task 7: Dialects and SQL Generator

**Files:**
- Create: `src/db_schema_sync_client/services/dialects.py`
- Create: `src/db_schema_sync_client/services/sql_generator.py`
- Test: `tests/unit/test_dialects.py`
- Test: `tests/unit/test_sql_generator.py`

**Step 1: Write failing dialect tests**

Cover:

- PostgreSQL quotes identifiers with double quotes.
- KingBase quotes identifiers with double quotes.
- identifiers with embedded quotes are escaped.
- schema/table/column names with Chinese characters are quoted safely.
- varchar length formatting.
- numeric precision/scale formatting.
- NOT NULL without default is downgraded to nullable and warning is produced.
- serial/identity-related default creates high-risk warning.

**Step 2: Write failing SQL generator tests**

Cover:

- PostgreSQL target SQL: `ALTER TABLE "df_test"."users" ADD COLUMN "name" character varying(100);`
- KingBase target SQL uses the KingBase dialect class.
- only `AUTO_SYNCABLE` missing target table fields produce executable SQL.
- manual-required differences do not produce executable SQL.
- generator summary counts executable/manual/hint items.

**Step 3: Run tests to verify failure**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_dialects.py tests/unit/test_sql_generator.py -v
```

Expected: FAIL with missing dialect/generator implementation.

**Step 4: Implement dialects and SQL generator**

Expose:

- `get_dialect(database_type)`
- `Dialect.quote_identifier(name)`
- `Dialect.format_column_type(column)`
- `SqlGenerator.generate(diff, target_type)`

Return a `GeneratedSqlPlan` with:

- executable statements.
- warnings.
- risk level.
- auto-syncable count.
- manual-required count.
- hint-only count.

**Step 5: Verify tests pass**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_dialects.py tests/unit/test_sql_generator.py -v
```

Expected: PASS.

**Step 6: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/services/dialects.py src/db_schema_sync_client/services/sql_generator.py tests/unit/test_dialects.py tests/unit/test_sql_generator.py
git commit -m "feat: add dialect-aware sql generation"
```

## Task 8: Sync Executor and History Persistence

**Files:**
- Create: `src/db_schema_sync_client/services/sync_executor.py`
- Modify: `src/db_schema_sync_client/infrastructure/app_store.py`
- Test: `tests/integration/test_sync_executor.py`

**Step 1: Write executor tests with fake connection**

Use fake connection/cursor objects. Cover:

- each SQL statement is executed.
- success records include statement text and status.
- failing statement rolls back current statement and continues.
- password is never included in result objects.
- execution records are saved to app store.

**Step 2: Run tests to verify failure**

Run:

```bash
PYTHONPATH=src pytest tests/integration/test_sync_executor.py -v
```

Expected: FAIL with missing executor.

**Step 3: Implement sync executor**

Behavior:

- accept `GeneratedSqlPlan`, target profile, and password.
- require an explicit `confirmed=True` flag.
- execute statements one by one.
- commit after each success.
- rollback after each failure.
- yield or return per-statement result.
- persist `sync_runs` and `sync_statements`.

**Step 4: Verify tests pass**

Run:

```bash
PYTHONPATH=src pytest tests/integration/test_sync_executor.py -v
```

Expected: PASS.

**Step 5: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/services/sync_executor.py src/db_schema_sync_client/infrastructure/app_store.py tests/integration/test_sync_executor.py
git commit -m "feat: add safe sync executor"
```

## Task 9: Report Service

**Files:**
- Create: `src/db_schema_sync_client/services/report_service.py`
- Test: `tests/unit/test_report_service.py`

**Step 1: Write failing report tests**

Cover:

- generated Markdown report includes source profile name and target profile name.
- report includes target database type.
- report includes summary counts.
- report includes generated SQL.
- report does not contain database passwords.

**Step 2: Run tests to verify failure**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_report_service.py -v
```

Expected: FAIL with missing report service.

**Step 3: Implement report service**

Expose:

- `render_compare_report(...) -> str`
- `render_sync_report(...) -> str`
- `save_report(text, output_dir) -> Path`

**Step 4: Verify tests pass**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_report_service.py -v
```

Expected: PASS.

**Step 5: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/services/report_service.py tests/unit/test_report_service.py
git commit -m "feat: add report service"
```

## Task 10: PyQt Background Workers

**Files:**
- Create: `src/db_schema_sync_client/ui/workers.py`
- Test: unit-test service orchestration without launching full UI where practical

**Step 1: Define worker responsibilities**

Add QThread or QRunnable-based workers:

- `ConnectionTestWorker`
- `MetadataLoadWorker`
- `CompareWorker`
- `SyncWorker`

Each worker should emit:

- started/progress.
- success payload.
- error message without password.
- finished.

**Step 2: Implement workers**

Keep workers thin. They should call service classes, not implement business logic.

**Step 3: Manual smoke test**

Run:

```bash
PYTHONPATH=src python -c "from db_schema_sync_client.ui.workers import ConnectionTestWorker; print(ConnectionTestWorker.__name__)"
```

Expected: prints `ConnectionTestWorker`.

**Step 4: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/ui/workers.py
git commit -m "feat: add pyqt background workers"
```

## Task 11: Login UI

**Files:**
- Create: `src/db_schema_sync_client/ui/login_dialog.py`
- Modify: `src/db_schema_sync_client/app.py`

**Step 1: Implement login dialog**

Requirements:

- username input.
- password input with echo mode password.
- login button.
- error label.
- calls `AppStore.verify_user`.

**Step 2: Wire startup flow**

`app.py` flow:

1. Resolve local app database path.
2. Initialize `AppStore`.
3. Show login dialog.
4. If accepted, show main window.

**Step 3: Manual smoke test**

Run:

```bash
PYTHONPATH=src python -m db_schema_sync_client.app
```

Expected:

- login dialog opens.
- `admin / cloudhis@2123` accepts login.
- wrong password shows an error.

**Step 4: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/app.py src/db_schema_sync_client/ui/login_dialog.py
git commit -m "feat: add login flow"
```

## Task 12: Connection Configuration UI

**Files:**
- Create: `src/db_schema_sync_client/ui/config_dialog.py`
- Modify: `src/db_schema_sync_client/ui/main_window.py`
- Modify: `src/db_schema_sync_client/infrastructure/app_store.py`

**Step 1: Implement connection config dialog**

Dialog requirements:

- profile list split by source and target.
- add/edit/delete/copy profile.
- source db type locked to PostgreSQL.
- target db type dropdown supports PostgreSQL and KingBase.
- test connection button.
- set default source and target.
- password field does not reveal saved password.

**Step 2: Wire profile validation**

Validation:

- host required.
- port integer 1 to 65535.
- database required.
- username required.
- password required for new profile.
- target type must be PostgreSQL or KingBase.

**Step 3: Manual smoke test**

Run app and verify:

- create source PostgreSQL profile.
- create target PostgreSQL profile.
- create target KingBase profile.
- invalid target type is impossible in UI.
- password is not rendered in the profile list.

**Step 4: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/ui/config_dialog.py src/db_schema_sync_client/ui/main_window.py src/db_schema_sync_client/infrastructure/app_store.py
git commit -m "feat: add connection profile dialog"
```

## Task 13: Main Window, Trees, and Diff Panel

**Files:**
- Create: `src/db_schema_sync_client/ui/main_window.py`
- Create: `src/db_schema_sync_client/ui/database_tree.py`
- Create: `src/db_schema_sync_client/ui/comparison_panel.py`
- Modify: `src/db_schema_sync_client/app.py`

**Step 1: Implement main window shell**

Include:

- toolbar with connection config, refresh, start compare.
- connection selection area for current source and target.
- left source tree.
- right target tree.
- comparison panel.
- bottom actions: selected count, generate SQL, Dry Run, clear, execute, export report.

**Step 2: Implement database tree**

Display:

- Database.
- Schema.
- Table/View.
- Field.

Field row shows:

- name.
- type.
- nullable.
- default summary.
- position.

**Step 3: Implement comparison panel**

Display:

- auto-syncable count.
- manual-required count.
- hint-only count.
- diff table.
- filters for object type, diff status, and only-syncable.

**Step 4: Wire compare flow**

On start compare:

1. confirm both profiles exist.
2. confirm both tested successfully if available.
3. load source snapshot.
4. load target snapshot.
5. run comparator.
6. update trees and diff panel.
7. save compare task/result summary.

**Step 5: Manual smoke test**

Run:

```bash
PYTHONPATH=src python -m db_schema_sync_client.app
```

Expected:

- main window opens after login.
- current source and target can be selected.
- start compare shows progress and then a diff summary.
- UI remains responsive during metadata loading.

**Step 6: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/ui/main_window.py src/db_schema_sync_client/ui/database_tree.py src/db_schema_sync_client/ui/comparison_panel.py src/db_schema_sync_client/app.py
git commit -m "feat: add desktop compare workspace"
```

## Task 14: SQL Preview and Dry Run

**Files:**
- Create: `src/db_schema_sync_client/ui/sql_preview_dialog.py`
- Modify: `src/db_schema_sync_client/ui/main_window.py`

**Step 1: Implement SQL preview dialog**

Dialog shows:

- target connection name.
- target database type.
- SQL count.
- auto-syncable/manual/hint counts.
- risk warnings.
- SQL text.
- copy button.
- save SQL button.
- Dry Run button.
- confirm execute button.
- cancel button.

**Step 2: Wire SQL generation**

On generate SQL:

1. collect selected auto-syncable diffs.
2. use `SqlGenerator` with target type.
3. show `SqlPreviewDialog`.

**Step 3: Wire Dry Run**

Dry Run should:

- save generated SQL/report.
- not call `SyncExecutor`.
- write a local history record that identifies the operation as dry run.

**Step 4: Manual smoke test**

Expected:

- selected missing fields generate real `ALTER TABLE` SQL.
- no placeholder `ADD COLUMN field type` appears.
- Dry Run saves SQL/report and does not connect to target for execution.

**Step 5: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/ui/sql_preview_dialog.py src/db_schema_sync_client/ui/main_window.py
git commit -m "feat: add sql preview and dry run"
```

## Task 15: Sync Execution Result UI

**Files:**
- Create: `src/db_schema_sync_client/ui/execution_result_dialog.py`
- Modify: `src/db_schema_sync_client/ui/main_window.py`
- Modify: `src/db_schema_sync_client/ui/sql_preview_dialog.py`

**Step 1: Implement execution result dialog**

Show:

- total count.
- success count.
- failure count.
- per-SQL status.
- error details.
- export result button.

**Step 2: Wire execution flow**

On confirm execute:

1. show second confirmation.
2. pass `confirmed=True` to `SyncExecutor`.
3. run in `SyncWorker`.
4. update progress.
5. show result dialog.
6. persist sync run and statements.

**Step 3: Manual smoke test**

Use a safe test target database only.

Expected:

- successful statements are marked success.
- failed statements are marked failed and show error text.
- failure does not stop remaining statements.
- no password is displayed in logs or result dialog.

**Step 4: Commit**

If git is initialized:

```bash
git add src/db_schema_sync_client/ui/execution_result_dialog.py src/db_schema_sync_client/ui/main_window.py src/db_schema_sync_client/ui/sql_preview_dialog.py
git commit -m "feat: add sync execution results"
```

## Task 16: Cleanup Old Entrypoints and Documentation

**Files:**
- Modify: `README.md`
- Modify: `.gitignore`
- Modify: `requirements.txt` or replace with `pyproject.toml` guidance
- Web prototypes already deleted: `web_app*.py`, `start_web.sh`, `test_tree.py`
- Consider archive/delete after confirmation: `desktop_app_v2.py`, `desktop_app_v3.py`, `test_pyqt.py`
- Keep temporarily for reference until new app passes: `database.py`, `comparator.py`, `sync.py`, `report.py`, `desktop_app.py`

**Step 1: Update README**

README should describe:

- desktop-only Phase 1 scope.
- supported matrix: PostgreSQL -> PostgreSQL and PostgreSQL -> KingBase.
- default login: `admin / cloudhis@2123`.
- run command: `scripts/run_desktop.sh`.
- Web is not part of Phase 1.
- config is managed in client UI, not by editing `config.yaml`.

**Step 2: Update ignore rules**

Add:

```gitignore
data/
dist/
build/
*.db
*.db-shm
*.db-wal
```

**Step 3: Remove or archive obsolete Web prototypes**

Only do this after the new desktop app can start and pass tests. Web prototypes (`web_app*.py`, `start_web.sh`, `test_tree.py`) have already been deleted.

**Step 4: Verify test suite**

Run:

```bash
PYTHONPATH=src pytest -v
```

Expected: PASS.

**Step 5: Verify import compile**

Run:

```bash
PYTHONPATH=src python -m compileall src tests
```

Expected: completes without syntax errors.

**Step 6: Commit**

If git is initialized:

```bash
git add README.md .gitignore requirements.txt pyproject.toml src tests scripts docs
git commit -m "docs: finalize phase one desktop client plan and entrypoint"
```

## Task 17: Phase 1 Acceptance Checklist

**Files:**
- Create: `docs/acceptance/phase-1-checklist.md`

**Step 1: Create checklist**

Include these checks:

- Fresh start creates local SQLite DB.
- `admin / cloudhis@2123` logs in.
- Wrong password fails.
- Source profile only supports PostgreSQL.
- Target profile supports PostgreSQL and KingBase.
- Saved connection profile does not expose plaintext password in SQLite.
- Connection test works for valid profile.
- Compare runs for PostgreSQL -> PostgreSQL.
- Compare runs for PostgreSQL -> KingBase.
- Missing target table fields are selectable.
- Type/default/nullability differences are visible but not executable.
- Missing schema/table/view differences are visible but not executable.
- Generated SQL contains no placeholder `type`.
- Dry Run creates SQL/report and does not execute target SQL.
- Confirmed sync executes each statement and records per-statement status.
- Logs and reports contain no database password.
- Web entrypoint is not documented as Phase 1 delivery.

**Step 2: Manual acceptance run**

Run through the checklist using safe test databases.

**Step 3: Record results**

Update `docs/acceptance/phase-1-checklist.md` with pass/fail status and notes.

**Step 4: Commit**

If git is initialized:

```bash
git add docs/acceptance/phase-1-checklist.md
git commit -m "test: add phase one acceptance checklist"
```

## Final Verification Commands

Run after all tasks:

```bash
PYTHONPATH=src pytest -v
PYTHONPATH=src python -m compileall src tests
PYTHONPATH=src python -c "from db_schema_sync_client.services.sql_generator import SqlGenerator; print(SqlGenerator.__name__)"
```

Expected:

- all tests pass.
- compileall completes.
- import check prints `SqlGenerator`.

## Implementation Notes

- If PyQt UI testing is too heavy for Phase 1, keep automated tests focused on services and persistence, then use the acceptance checklist for GUI flows.
- Do not remove old prototype files until the new package entrypoint starts successfully and service tests pass.
- Prefer small commits by task if the directory is converted into a git repository.
- Do not use raw production database connections during tests. Use fake connections for unit/integration tests and only use manually approved safe databases for acceptance.
