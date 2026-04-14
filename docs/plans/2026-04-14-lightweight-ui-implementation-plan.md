# Lightweight UI Prototype Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor the current PyQt6 desktop client into a navigation-based shell, preserve the existing schema sync workflow, and add the first local-data-backed PostgreSQL cluster management surfaces described in the 2026-04-14 prototype.

**Architecture:** Keep the existing domain, metadata, compare, SQL generation, sync execution, and history services intact. Move current structure-sync UI logic out of `MainWindow` into a dedicated page widget, then add cluster CRUD, cluster overview, audit, and settings as separate pages backed by new `AppStore` tables and small service adapters.

**Tech Stack:** Python 3.9+, PyQt6, SQLite via `sqlite3`, existing `psycopg2` connection layer, pytest, current `AppStore` migration pattern.

## Current Code Constraints

- The current desktop entry flow is already stable in `src/db_schema_sync_client/app.py`.
- The current schema sync workflow is heavily orchestrated by `src/db_schema_sync_client/ui/main_window.py`.
- Reusable UI pieces already exist:
  - `src/db_schema_sync_client/ui/comparison_panel.py`
  - `src/db_schema_sync_client/ui/config_dialog.py`
  - `src/db_schema_sync_client/ui/history_dialog.py`
  - `src/db_schema_sync_client/ui/sql_preview_dialog.py`
- Existing local persistence only covers users, connection profiles, compare tasks/results, sync runs/statements in `src/db_schema_sync_client/infrastructure/app_store.py`.
- No real cluster-management storage or service layer exists yet.

## Task 1: Split Main Window Into Navigation Shell And Structure Sync Page

**Files:**
- Modify: `src/db_schema_sync_client/ui/main_window.py`
- Create: `src/db_schema_sync_client/ui/structure_sync_page.py`
- Modify: `src/db_schema_sync_client/ui/__init__.py`
- Test: `tests/ui/test_desktop_ui.py`

**Step 1: Write a failing UI test for navigation shell**

Add a UI test that instantiates `MainWindow(app_store=None)` and asserts:
- a left-side navigation widget exists
- a stacked content area exists
- the default page is the structure sync page

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py -k navigation -v
```

Expected: FAIL because the current `MainWindow` has only the old single-page layout.

**Step 2: Create the structure sync page widget**

Create `src/db_schema_sync_client/ui/structure_sync_page.py` and move the current schema-sync-specific UI and orchestration into a widget class, including:
- source/target profile combos
- source/target trees
- compare button
- `ComparisonPanel`
- SQL preview / dry run / execute / export report actions
- progress bar and status updates

Keep existing service usage unchanged:
- `MetadataReader`
- `SqlGenerator`
- `ReportService`
- `SyncExecutor`
- `MetadataWorker`
- `CompareWorker`
- `SyncWorker`

**Step 3: Reduce `MainWindow` to shell responsibilities**

Update `src/db_schema_sync_client/ui/main_window.py` so it only owns:
- top toolbar or top utility bar
- left navigation
- right `QStackedWidget`
- page registration and switching
- global refresh / exit dispatch

Default pages for this task:
- `结构同步`
- placeholder pages for `集群管理`, `历史与审计`, `系统设置`

**Step 4: Preserve existing behavior**

Make sure the structure sync page still supports:
- opening connection config
- opening history
- metadata refresh
- compare
- SQL preview
- dry run
- execute sync
- export report

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py -v
```

Expected: PASS for existing UI tests and the new navigation-shell test.

**Step 5: Commit**

```bash
git add src/db_schema_sync_client/ui tests/ui/test_desktop_ui.py
git commit -m "refactor: split navigation shell from structure sync page"
```

## Task 2: Add Cluster And Settings Persistence To AppStore

**Files:**
- Modify: `src/db_schema_sync_client/infrastructure/app_store.py`
- Modify: `src/db_schema_sync_client/domain/models.py`
- Test: `tests/unit/test_app_store.py`
- Create: `tests/unit/test_cluster_store.py`

**Step 1: Write failing store tests**

Add tests for:
- creating a cluster profile
- updating an existing cluster profile
- listing clusters with enabled/environment filters
- writing and listing cluster audit logs
- saving and reading app settings

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_cluster_store.py tests/unit/test_app_store.py -v
```

Expected: FAIL because cluster tables and store methods do not exist.

**Step 2: Extend the domain model**

Update `src/db_schema_sync_client/domain/models.py` with small frozen dataclasses or enums for:
- `ClusterEnvironment`
- `ClusterProfile`
- `ClusterConnectivityCheck`
- `ClusterAuditRecord`
- `AppSetting` or simple key/value setting shape

Do not mix cluster credentials into `ConnectionProfile`.

**Step 3: Add SQLite tables and migrations in AppStore**

Extend `initialize()` and `_migrate_schema()` in `src/db_schema_sync_client/infrastructure/app_store.py` with tables for:
- `cluster_profiles`
- `cluster_audit_logs`
- `app_settings`

Recommended `cluster_profiles` columns:
- `id`
- `name`
- `environment`
- `description`
- `patroni_endpoints_text`
- `pg_host`
- `pg_port`
- `pg_database`
- `pg_username`
- `pg_credential_key`
- `etcd_endpoints_text`
- `is_enabled`
- `last_health_status`
- `last_health_message`
- `created_at`
- `updated_at`

Use the existing credential-store pattern for cluster PG passwords.

**Step 4: Add store methods**

Implement minimal methods in `AppStore`:
- `save_cluster_profile(...)`
- `list_cluster_profiles(...)`
- `get_cluster_profile(...)`
- `delete_cluster_profile(...)`
- `add_cluster_audit_log(...)`
- `list_cluster_audit_logs(...)`
- `get_setting(...)`
- `set_setting(...)`
- `list_settings(...)`

**Step 5: Verify tests**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_cluster_store.py tests/unit/test_app_store.py -v
```

Expected: PASS.

**Step 6: Commit**

```bash
git add src/db_schema_sync_client/domain/models.py src/db_schema_sync_client/infrastructure/app_store.py tests/unit
git commit -m "feat: add cluster and settings persistence"
```

## Task 3: Implement Cluster List Page And Cluster Edit Dialog

**Files:**
- Create: `src/db_schema_sync_client/ui/cluster_list_page.py`
- Create: `src/db_schema_sync_client/ui/cluster_dialog.py`
- Modify: `src/db_schema_sync_client/ui/main_window.py`
- Modify: `src/db_schema_sync_client/ui/__init__.py`
- Test: `tests/ui/test_desktop_ui.py`

**Step 1: Write failing UI tests**

Add UI tests for:
- cluster list page loads rows from a fake store
- clicking `新增集群` opens the dialog
- dialog validates required fields
- saving refreshes the list

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py -k cluster -v
```

Expected: FAIL because cluster pages do not exist.

**Step 2: Build the dialog first**

Create `src/db_schema_sync_client/ui/cluster_dialog.py` with fields from the prototype:
- cluster name
- environment
- description
- Patroni endpoints
- PG management host/port/database/username/password
- etcd endpoints
- `测试连接`
- `保存`
- `取消`

Validation rules:
- required: name, environment, Patroni endpoints, PG connection, etcd endpoints
- split comma-separated endpoint fields with trim

Keep initial connection testing lightweight:
- Patroni and etcd checks can be placeholder reachability stubs in this task
- PG check should reuse the existing connection factory pattern when possible

**Step 3: Build the list page**

Create `src/db_schema_sync_client/ui/cluster_list_page.py` with:
- environment filter
- keyword filter
- enabled-only checkbox
- refresh button
- add button
- table rows with `详情` and `编辑`

Back it with new `AppStore` cluster methods.

**Step 4: Register the page in the shell**

Update `src/db_schema_sync_client/ui/main_window.py` so the `集群管理` navigation target points to the cluster list page instead of a placeholder.

**Step 5: Verify**

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py -v
```

Expected: PASS for cluster page tests and previous UI tests.

**Step 6: Commit**

```bash
git add src/db_schema_sync_client/ui tests/ui/test_desktop_ui.py
git commit -m "feat: add cluster list page and edit dialog"
```

## Task 4: Add Read-Only Cluster Overview Page

**Files:**
- Create: `src/db_schema_sync_client/services/cluster_service.py`
- Create: `src/db_schema_sync_client/ui/cluster_overview_page.py`
- Modify: `src/db_schema_sync_client/ui/cluster_list_page.py`
- Modify: `src/db_schema_sync_client/ui/main_window.py`
- Create: `tests/unit/test_cluster_service.py`
- Test: `tests/ui/test_desktop_ui.py`

**Step 1: Write failing service tests**

Add tests for a `ClusterService` that normalizes raw Patroni/PG/etcd inputs into a single overview payload:
- primary node name
- replica count
- Patroni healthy count
- etcd healthy count
- PG connection count summary
- node detail rows
- partial-failure state when one source fails

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_cluster_service.py -v
```

Expected: FAIL because the service does not exist.

**Step 2: Implement service with adapter boundaries**

Create `src/db_schema_sync_client/services/cluster_service.py` with pure-Python aggregation logic.

Keep external I/O behind small injectable callables or adapter classes so tests can stay local and deterministic.

For the first pass:
- Patroni: support stubbed JSON input shape
- PG: support summarized connection stats input shape
- etcd: support summarized member-health input shape

Do not wire dangerous actions yet.

**Step 3: Build read-only overview page**

Create `src/db_schema_sync_client/ui/cluster_overview_page.py` with:
- refresh button
- summary cards
- topology summary text
- node detail table
- recent audit summary list

Failure rule:
- if one source fails, show partial-failure state instead of aborting the page

**Step 4: Wire list -> detail navigation**

Update cluster list and main window so clicking `详情` opens the overview page for the selected cluster.

**Step 5: Verify**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_cluster_service.py tests/ui/test_desktop_ui.py -v
```

Expected: PASS.

**Step 6: Commit**

```bash
git add src/db_schema_sync_client/services src/db_schema_sync_client/ui tests
git commit -m "feat: add read-only cluster overview page"
```

## Task 5: Expand History Into History And Audit Workspace

**Files:**
- Modify: `src/db_schema_sync_client/ui/history_dialog.py`
- Modify: `src/db_schema_sync_client/infrastructure/app_store.py`
- Test: `tests/ui/test_desktop_ui.py`
- Test: `tests/unit/test_cluster_store.py`

**Step 1: Write failing tests**

Add tests for:
- history dialog shows a third tab for cluster audit
- audit tab refreshes from `AppStore.list_cluster_audit_logs()`
- filters preserve current tab and inputs on refresh

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py -k history -v
```

Expected: FAIL because audit tab is missing.

**Step 2: Extend the dialog**

Update `src/db_schema_sync_client/ui/history_dialog.py` to include:
- `比对记录`
- `同步记录`
- `集群操作审计`

Audit filters:
- time range
- action type
- result status
- keyword

Keep `重新比对` only on compare history.

**Step 3: Verify**

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py tests/unit/test_cluster_store.py -v
```

Expected: PASS.

**Step 4: Commit**

```bash
git add src/db_schema_sync_client/ui/history_dialog.py src/db_schema_sync_client/infrastructure/app_store.py tests
git commit -m "feat: extend history workspace with cluster audit"
```

## Task 6: Add Minimal Settings Page

**Files:**
- Create: `src/db_schema_sync_client/ui/settings_page.py`
- Modify: `src/db_schema_sync_client/ui/main_window.py`
- Modify: `src/db_schema_sync_client/paths.py`
- Test: `tests/ui/test_desktop_ui.py`
- Test: `tests/unit/test_cluster_store.py`

**Step 1: Write failing tests**

Add tests for:
- settings page loads persisted values
- save writes through `AppStore`
- restore defaults requires confirmation

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py -k settings -v
```

Expected: FAIL because the settings page does not exist.

**Step 2: Build the page**

Create `src/db_schema_sync_client/ui/settings_page.py` with:
- default refresh interval
- timeout seconds
- export directory
- save button
- restore default button

Use `AppStore` key/value settings. Keep defaults centralized, either in:
- `src/db_schema_sync_client/paths.py`, or
- a small settings constant block in the new page module

**Step 3: Register page in shell**

Hook the `系统设置` navigation target to the new page.

**Step 4: Verify**

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py tests/unit/test_cluster_store.py -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add src/db_schema_sync_client/ui src/db_schema_sync_client/paths.py tests
git commit -m "feat: add minimal system settings page"
```

## Task 7: Add Patroni Action Scaffolding Without Dangerous Automation

**Files:**
- Create: `src/db_schema_sync_client/ui/patroni_actions_page.py`
- Modify: `src/db_schema_sync_client/services/cluster_service.py`
- Modify: `src/db_schema_sync_client/infrastructure/app_store.py`
- Create: `tests/unit/test_cluster_actions.py`
- Test: `tests/ui/test_desktop_ui.py`

**Step 1: Write failing tests**

Add tests for:
- switchover action requires explicit confirmation text
- reload action requires node selection
- successful and failed actions both write audit logs

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_cluster_actions.py -v
```

Expected: FAIL because no action page or audit-writing action flow exists.

**Step 2: Implement UI-only guarded flow**

Build `src/db_schema_sync_client/ui/patroni_actions_page.py` with:
- `执行 switchover`
- node-level `reload`
- confirmation dialog for dangerous action
- result summary area

Keep the first iteration behind injected service methods so the UI and auditing are testable before any real network action is enabled.

**Step 3: Write audit records**

Ensure every attempted action writes:
- cluster id
- operator
- action type
- target node if any
- result status
- detail JSON

**Step 4: Verify**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_cluster_actions.py tests/ui/test_desktop_ui.py -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add src/db_schema_sync_client/ui src/db_schema_sync_client/services src/db_schema_sync_client/infrastructure tests
git commit -m "feat: add guarded patroni action scaffolding"
```

## Task 8: Final Verification And Documentation Update

**Files:**
- Modify: `README.md`
- Modify: `docs/plans/2026-04-14-lightweight-ui-prototype.md`
- Optional: `docs/PRD-desktop-client-phase-1.md`

**Step 1: Run focused test suites**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_app_store.py tests/unit/test_cluster_store.py tests/unit/test_cluster_service.py tests/unit/test_cluster_actions.py tests/integration/test_sync_executor.py tests/ui/test_desktop_ui.py -v
```

Expected: PASS.

**Step 2: Run a smoke launch**

Run:

```bash
PYTHONPATH=src python -m db_schema_sync_client.app
```

Expected:
- login dialog still appears
- navigation shell loads
- structure sync still opens by default
- cluster list/history/settings pages switch correctly

**Step 3: Update docs**

Update `README.md` with:
- new navigation structure
- cluster management availability
- settings/audit notes

Update `docs/plans/2026-04-14-lightweight-ui-prototype.md` to mark completed or deferred sections once implementation is done.

**Step 4: Commit**

```bash
git add README.md docs
git commit -m "docs: update lightweight ui workflow documentation"
```

## Recommended Execution Order

1. Do Task 1 first. This contains the highest UI risk and unlocks every later page.
2. Do Task 2 second. Cluster pages should not be built against ad hoc in-memory shapes.
3. Do Task 3 and Task 4 next. That yields the first usable cluster-management slice.
4. Do Task 5 and Task 6 after cluster data exists. They are lower-risk extensions.
5. Do Task 7 only after the read-only cluster path is stable.
6. Finish with Task 8 verification and docs.

## Risk Notes

- The biggest refactor risk is moving schema-sync orchestration out of `MainWindow` without breaking compare/sync side effects.
- The biggest product risk is introducing cluster actions before the read-only overview and audit path are stable.
- Keep cluster-network integration behind adapters so UI and storage work can land without needing live Patroni or etcd in every test run.
- Do not fold cluster PG management credentials into the existing connection-profile table; that will create avoidable coupling.

Plan complete and saved to `docs/plans/2026-04-14-lightweight-ui-implementation-plan.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

Which approach?
