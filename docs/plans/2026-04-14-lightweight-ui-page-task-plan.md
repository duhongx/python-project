# Lightweight UI Page Task Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Break the confirmed 2026-04-14 lightweight UI design into page-level development tasks that can be implemented incrementally without destabilizing the existing schema sync workflow.

**Architecture:** Treat the new UI as a shell-and-pages refactor. First extract the current structure-sync experience into a dedicated page under a new navigation shell, then add cluster, audit, and settings pages on top of new SQLite persistence and small service adapters.

**Tech Stack:** Python 3.9+, PyQt6, SQLite via `sqlite3`, existing `AppStore`, `MetadataReader`, `SchemaComparator`, `SqlGenerator`, `SyncExecutor`, pytest.

## Baseline References

- UI design: `docs/plans/2026-04-14-lightweight-ui-design.md`
- Existing implementation plan: `docs/plans/2026-04-14-lightweight-ui-implementation-plan.md`
- Current shell/UI entrypoint: `src/db_schema_sync_client/ui/main_window.py`
- Current persistence root: `src/db_schema_sync_client/infrastructure/app_store.py`

## Page Delivery Order

1. 主框架页
2. 结构同步页
3. 集群列表页
4. 集群总览页
5. Patroni 动作页
6. 历史与审计页
7. 系统设置页

这个顺序不是视觉顺序，而是依赖顺序。只有先把主框架和结构同步页稳定下来，后面的新增页面才有安全挂载点。

## Task 1: 主框架页

**Files:**
- Modify: `src/db_schema_sync_client/ui/main_window.py`
- Create: `src/db_schema_sync_client/ui/structure_sync_page.py`
- Modify: `src/db_schema_sync_client/ui/__init__.py`
- Test: `tests/ui/test_desktop_ui.py`

**Page Goal:** 把现在的 `MainWindow` 从业务大页改成导航壳，承载左侧导航、顶部全局栏、右侧页面栈和底部状态栏。

**UI Scope:**
- 左侧导航
- 顶部全局栏：环境标识、刷新、退出
- 右侧 `QStackedWidget`
- 页面标题区 / 面包屑
- 全局状态栏

**Step 1: Write the failing shell test**

在 `tests/ui/test_desktop_ui.py` 增加测试，断言：
- `MainWindow` 存在左侧导航区域
- `MainWindow` 存在页面栈
- 默认页面为结构同步页

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py -k navigation -v
```

Expected: FAIL，因为当前 `MainWindow` 还是单页布局。

**Step 2: Extract current schema-sync content into a page widget**

创建 `src/db_schema_sync_client/ui/structure_sync_page.py`，把当前这些能力整体迁出：
- 源端/目标端连接下拉
- 结构树
- 比对按钮
- 差异区
- SQL 预览与执行
- 导出报告
- worker 编排

要求：
- 不改变服务层接口
- 不重写现有比对和同步逻辑

**Step 3: Rebuild MainWindow as a shell**

让 `src/db_schema_sync_client/ui/main_window.py` 只保留：
- 页面注册
- 页面切换
- 全局刷新分发
- 退出确认

初始挂载页面：
- `结构同步`
- `集群管理` 占位页
- `历史与审计` 占位页
- `系统设置` 占位页

**Step 4: Verify the shell still preserves current workflow**

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py -v
```

Expected: PASS。

**Step 5: Commit**

```bash
git add src/db_schema_sync_client/ui tests/ui/test_desktop_ui.py
git commit -m "refactor: build lightweight navigation shell"
```

## Task 2: 结构同步页

**Files:**
- Create: `src/db_schema_sync_client/ui/structure_sync_page.py`
- Modify: `src/db_schema_sync_client/ui/comparison_panel.py`
- Modify: `src/db_schema_sync_client/ui/config_dialog.py`
- Modify: `src/db_schema_sync_client/ui/history_dialog.py`
- Test: `tests/ui/test_desktop_ui.py`

**Page Goal:** 在新主框架中承接现有核心流程，完成“连接与结构 + 差异筛选 + SQL预览与执行”的页面化重排。

**UI Scope:**
- 顶部连接区
- 差异筛选区
- 双结构树
- 差异结果区
- 底部动作区
- 页面级进度反馈

**Step 1: Write a failing layout-focused test**

新增测试，断言结构同步页有：
- 源连接下拉
- 目标连接下拉
- 刷新结构按钮
- 连接配置按钮
- 开始比对按钮
- 底部动作按钮组

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py -k structure_sync -v
```

Expected: FAIL，因为新页面组件还未独立存在。

**Step 2: Rebuild the top filter area**

把页面顶部整理成两行：
- 第一行：连接和范围
- 第二行：差异筛选

要求：
- `连接配置` 移入页内
- 保留 `只看可同步` 和 `忽略仅目标端`
- 不新增新的数据库查询时机

**Step 3: Reuse ComparisonPanel and action flow**

保留当前：
- 差异统计
- 分类筛选
- 勾选差异
- 生成 SQL
- Dry Run
- 执行同步
- 导出报告

如果 `ComparisonPanel` 字段顺序或标题需要优化，只做轻量调整，不改内部核心筛选逻辑。

**Step 4: Verify all existing structure-sync tests**

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py -k "sql_preview or main_window or structure_sync" -v
```

Expected: PASS。

**Step 5: Commit**

```bash
git add src/db_schema_sync_client/ui tests/ui/test_desktop_ui.py
git commit -m "feat: reorganize structure sync page"
```

## Task 3: 集群列表页

**Files:**
- Create: `src/db_schema_sync_client/ui/cluster_list_page.py`
- Create: `src/db_schema_sync_client/ui/cluster_dialog.py`
- Modify: `src/db_schema_sync_client/infrastructure/app_store.py`
- Modify: `src/db_schema_sync_client/domain/models.py`
- Create: `tests/unit/test_cluster_store.py`
- Test: `tests/ui/test_desktop_ui.py`

**Page Goal:** 提供集群管理入口，支持查看、筛选、新增、编辑、启停集群配置。

**UI Scope:**
- 列表表格
- 环境筛选
- 关键字筛选
- 启用状态筛选
- 新增/编辑弹窗

**Step 1: Write failing persistence tests first**

在 `tests/unit/test_cluster_store.py` 写测试，断言：
- 能新增集群
- 能编辑集群
- 能按环境和启用状态筛选

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_cluster_store.py -v
```

Expected: FAIL，因为 `AppStore` 还没有集群表和方法。

**Step 2: Add minimal cluster persistence**

在 `src/db_schema_sync_client/infrastructure/app_store.py` 和 `src/db_schema_sync_client/domain/models.py` 中增加：
- `cluster_profiles` 表
- `ClusterProfile`
- `save_cluster_profile`
- `list_cluster_profiles`
- `get_cluster_profile`
- `delete_cluster_profile`

密码继续使用 credential store，不写入 SQLite 明文。

**Step 3: Build cluster dialog**

创建 `src/db_schema_sync_client/ui/cluster_dialog.py`，字段包括：
- 集群名称
- 环境
- 说明
- Patroni 地址
- PG 管理连接
- etcd 地址

按钮：
- `测试连接`
- `保存`
- `取消`

**Step 4: Build cluster list page**

创建 `src/db_schema_sync_client/ui/cluster_list_page.py`，完成：
- 筛选区
- 表格区
- `新增集群`
- 行级 `详情`、`编辑`、`启用/停用`

**Step 5: Verify**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_cluster_store.py tests/ui/test_desktop_ui.py -k cluster -v
```

Expected: PASS。

**Step 6: Commit**

```bash
git add src/db_schema_sync_client/domain/models.py src/db_schema_sync_client/infrastructure/app_store.py src/db_schema_sync_client/ui tests
git commit -m "feat: add cluster list page"
```

## Task 4: 集群总览页

**Files:**
- Create: `src/db_schema_sync_client/services/cluster_service.py`
- Create: `src/db_schema_sync_client/ui/cluster_overview_page.py`
- Modify: `src/db_schema_sync_client/ui/cluster_list_page.py`
- Create: `tests/unit/test_cluster_service.py`
- Test: `tests/ui/test_desktop_ui.py`

**Page Goal:** 展示集群的只读健康总览，先做稳定展示，不先做危险动作。

**UI Scope:**
- 顶部状态卡片
- 拓扑摘要
- 节点明细表
- 最近操作摘要
- 手动刷新

**Step 1: Write failing service tests**

在 `tests/unit/test_cluster_service.py` 增加测试，断言服务能产出：
- primary
- replica 数
- Patroni 健康计数
- etcd 健康计数
- PG 连接摘要
- 节点明细
- 部分失败状态

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_cluster_service.py -v
```

Expected: FAIL，因为 `cluster_service.py` 不存在。

**Step 2: Implement aggregation service**

创建 `src/db_schema_sync_client/services/cluster_service.py`，只做数据聚合和状态归一化，不直接把网络请求写死在 UI 层。

**Step 3: Build the overview page**

创建 `src/db_schema_sync_client/ui/cluster_overview_page.py`，实现：
- 顶部卡片区
- 文本拓扑摘要
- 节点表格
- 最近操作区

要求：
- 任一数据源失败时，显示“部分失败”
- 仍尽量展示成功返回的数据

**Step 4: Wire list to detail**

在 `cluster_list_page.py` 中，点击 `详情` 进入总览页。

**Step 5: Verify**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_cluster_service.py tests/ui/test_desktop_ui.py -k "cluster and overview" -v
```

Expected: PASS。

**Step 6: Commit**

```bash
git add src/db_schema_sync_client/services src/db_schema_sync_client/ui tests
git commit -m "feat: add cluster overview page"
```

## Task 5: Patroni 动作页

**Files:**
- Create: `src/db_schema_sync_client/ui/patroni_actions_page.py`
- Modify: `src/db_schema_sync_client/services/cluster_service.py`
- Modify: `src/db_schema_sync_client/infrastructure/app_store.py`
- Create: `tests/unit/test_cluster_actions.py`
- Test: `tests/ui/test_desktop_ui.py`

**Page Goal:** 提供最小危险动作入口，但把风险控制和审计放在第一位。

**UI Scope:**
- switchover 区
- reload 区
- 操作说明区
- 最近执行结果区

**Step 1: Write failing action tests**

在 `tests/unit/test_cluster_actions.py` 增加测试，断言：
- switchover 必须确认
- reload 必须选节点
- 成功/失败都写审计

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_cluster_actions.py -v
```

Expected: FAIL。

**Step 2: Add audit persistence**

在 `src/db_schema_sync_client/infrastructure/app_store.py` 增加：
- `cluster_audit_logs` 表
- `add_cluster_audit_log`
- `list_cluster_audit_logs`

**Step 3: Build action page**

创建 `src/db_schema_sync_client/ui/patroni_actions_page.py`，实现：
- 当前 leader 展示
- 候选节点选择
- switchover 按钮
- reload 节点选择和按钮
- 最近执行结果区

**Step 4: Guard dangerous actions**

要求：
- `switchover` 输入确认词
- `reload` 无节点不可点
- 所有动作结果都写审计

**Step 5: Verify**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_cluster_actions.py tests/ui/test_desktop_ui.py -k patroni -v
```

Expected: PASS。

**Step 6: Commit**

```bash
git add src/db_schema_sync_client/infrastructure/app_store.py src/db_schema_sync_client/services src/db_schema_sync_client/ui tests
git commit -m "feat: add patroni actions page"
```

## Task 6: 历史与审计页

**Files:**
- Modify: `src/db_schema_sync_client/ui/history_dialog.py`
- Modify: `src/db_schema_sync_client/ui/main_window.py`
- Modify: `src/db_schema_sync_client/infrastructure/app_store.py`
- Test: `tests/ui/test_desktop_ui.py`
- Test: `tests/unit/test_cluster_store.py`

**Page Goal:** 把现在的历史弹窗扩展为覆盖比对、同步和集群操作审计的统一工作区。

**UI Scope:**
- Tabs：比对记录、同步记录、集群操作审计
- 通用筛选区
- 列表区
- 详情区

**Step 1: Write failing UI test**

新增测试，断言历史界面具备第三个标签页 `集群操作审计`。

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py -k history -v
```

Expected: FAIL。

**Step 2: Extend HistoryDialog**

在 `src/db_schema_sync_client/ui/history_dialog.py` 中增加：
- 审计 tab
- 审计筛选
- 审计列表
- 详情展示

保留现有：
- 比对详情
- SQL 详情
- 重新比对

**Step 3: Optionally mount it as a full page**

如果此阶段决定从弹窗升级为正式页面，则：
- 在 `main_window.py` 中挂载为页面
- 原工具栏入口跳转到该页面

如果暂不升级为正式页面，至少先完成内容结构升级。

**Step 4: Verify**

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py tests/unit/test_cluster_store.py -v
```

Expected: PASS。

**Step 5: Commit**

```bash
git add src/db_schema_sync_client/ui/history_dialog.py src/db_schema_sync_client/ui/main_window.py src/db_schema_sync_client/infrastructure/app_store.py tests
git commit -m "feat: expand history into audit workspace"
```

## Task 7: 系统设置页

**Files:**
- Create: `src/db_schema_sync_client/ui/settings_page.py`
- Modify: `src/db_schema_sync_client/infrastructure/app_store.py`
- Modify: `src/db_schema_sync_client/ui/main_window.py`
- Create: `tests/unit/test_settings_store.py`
- Test: `tests/ui/test_desktop_ui.py`

**Page Goal:** 提供最小本地设置页，承接刷新间隔、超时和导出目录。

**UI Scope:**
- 默认刷新间隔
- 请求超时
- 导出目录
- 保存
- 恢复默认

**Step 1: Write failing settings tests**

在 `tests/unit/test_settings_store.py` 中增加测试，断言：
- 能保存设置
- 能读取设置
- 未设置时返回默认值

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_settings_store.py -v
```

Expected: FAIL。

**Step 2: Add settings persistence**

在 `src/db_schema_sync_client/infrastructure/app_store.py` 中增加：
- `app_settings` 表
- `get_setting`
- `set_setting`
- `list_settings`

**Step 3: Build the settings page**

创建 `src/db_schema_sync_client/ui/settings_page.py`，实现：
- 刷新间隔输入
- 超时输入
- 导出目录选择
- 保存
- 恢复默认

**Step 4: Register page**

在 `main_window.py` 中正式挂载 `系统设置` 页面。

**Step 5: Verify**

Run:

```bash
PYTHONPATH=src pytest tests/unit/test_settings_store.py tests/ui/test_desktop_ui.py -k settings -v
```

Expected: PASS。

**Step 6: Commit**

```bash
git add src/db_schema_sync_client/infrastructure/app_store.py src/db_schema_sync_client/ui tests
git commit -m "feat: add system settings page"
```

## Task 8: Cross-Page Polish And Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/plans/2026-04-14-lightweight-ui-design.md`
- Test: `tests/ui/test_desktop_ui.py`
- Test: `tests/unit/test_app_store.py`
- Test: `tests/unit/test_cluster_store.py`
- Test: `tests/unit/test_cluster_service.py`
- Test: `tests/unit/test_cluster_actions.py`
- Test: `tests/unit/test_settings_store.py`
- Test: `tests/integration/test_sync_executor.py`

**Page Goal:** 确认所有页面挂载正确、导航正常、已有结构同步主流程无回归。

**Step 1: Run full focused verification**

Run:

```bash
PYTHONPATH=src pytest tests/ui/test_desktop_ui.py tests/unit/test_app_store.py tests/unit/test_cluster_store.py tests/unit/test_cluster_service.py tests/unit/test_cluster_actions.py tests/unit/test_settings_store.py tests/integration/test_sync_executor.py -v
```

Expected: PASS。

**Step 2: Manual smoke check**

Run:

```bash
PYTHONPATH=src python -m db_schema_sync_client.app
```

Expected:
- 登录正常
- 默认进入结构同步页
- 导航切换正常
- 集群列表页可打开
- 历史与审计可打开
- 设置页可打开

**Step 3: Update docs**

更新：
- `README.md`
- `docs/plans/2026-04-14-lightweight-ui-design.md`

补充已落地页面和暂缓页面。

**Step 4: Commit**

```bash
git add README.md docs
git commit -m "docs: sync lightweight ui page delivery status"
```

## Recommended Sprint Mapping

### Sprint 1

- Task 1: 主框架页
- Task 2: 结构同步页

目标：不破坏现有主流程的前提下完成新版 UI 外壳。

### Sprint 2

- Task 3: 集群列表页
- Task 4: 集群总览页

目标：形成集群配置和只读查看的最小闭环。

### Sprint 3

- Task 5: Patroni 动作页
- Task 6: 历史与审计页
- Task 7: 系统设置页

目标：补齐危险动作入口、审计链路和本地系统配置。

## Open Decisions

1. `历史与审计` 最终是正式页面还是保留为增强弹窗。
2. 集群总览的数据获取是先用 stub/mock 接口，还是立即接真实 Patroni/etcd。
3. `系统设置` 默认值常量放在 `paths.py` 还是单独 settings 模块。
4. `Patroni 动作页` 是否在第一期只保留 UI 和审计脚手架，不立即接真实执行。

Plan complete and saved to `docs/plans/2026-04-14-lightweight-ui-page-task-plan.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

Which approach?
