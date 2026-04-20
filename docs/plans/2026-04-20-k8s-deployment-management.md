# K8s 部署管理功能设计与实现方案

- **日期**：2026-04-20
- **状态**：已实现（一期）

---

## 1. 背景与目标

数据库结构同步功能完成后，需要在同一个桌面客户端内新增 Kubernetes 部署管理能力。

**使用场景**：  
在发版前，运维人员需要对某个 Namespace 下的部分或全部 Deployment 进行镜像版本备份。若发版后出现问题，可快速利用备份的镜像版本执行回滚，将 Deployment 恢复到上一个已知良好状态。

**一期核心目标**：
- 注册并管理 Kubernetes 集群（上传 kubeconfig 文件 + 指定 namespace 列表）
- 选择集群和 Namespace，查看当前 Deployment 列表及其镜像版本
- 对选中的（或全部）Deployment 创建镜像版本快照（备份）
- 从历史快照中选择一条，执行回滚（将 image 恢复到快照时的版本）

---

## 2. 需求分析

### 2.1 核心对象

| 对象 | 说明 |
|---|---|
| **KubeClusterConfig** | 一个已注册的 K8s 集群，包含 kubeconfig 文件路径、选用的 context、namespace 列表 |
| **DeploymentSnapshot** | 某次备份操作产生的快照，属于某个集群的某个 namespace |
| **DeploymentImageRecord** | 快照内单个 Deployment 的所有容器镜像记录 |
| **ContainerImage** | 单个容器的名称和完整 image（含 tag） |

### 2.2 核心操作

1. **集群管理**：上传 kubeconfig 文件，选择 context，配置 namespace 列表（dev/test/pre/prod 等）
2. **查看 Deployment**：选择集群 + namespace，连接集群，拉取当前 Deployment 列表及其容器镜像
3. **备份（创建快照）**：选中部分或全部 Deployment，将当前容器 image 版本记录为一份带备注的快照
4. **历史快照管理**：查看历史快照列表，删除不需要的快照
5. **回滚**：选择历史快照，预览将恢复的 image 版本，确认后通过 K8s PATCH API 将各 Deployment 恢复

### 2.3 设计决策

| 决策点 | 选择 | 理由 |
|---|---|---|
| 集群连接方式 | 上传 kubeconfig 文件，管理存储到本地目录 | 适合桌面工具，不依赖本机 `~/.kube/config`，支持多集群 |
| Namespace 配置 | 用户手动维护（可添加/删除行），不从集群实时查询 | 离线可配置，减少不必要的网络请求 |
| 镜像标识符 | 记录 K8s 中 `image` 字段的完整原始值，不做转换 | 与集群保持一致，避免 mutable tag 歧义 |
| 回滚实现 | strategic merge patch（`patch_namespaced_deployment`） | 等效于 `kubectl set image`，不依赖 kubectl 命令行 |
| 回滚后验证 | 一期不轮询 rollout 状态，仅展示 patch 结果 | 降低一期复杂度，二期可添加 |
| kubeconfig 存储 | 文件复制到 `data/kubeconfigs/cluster_<id>.yaml`，SQLite 记录路径 | 避免大文本入库，文件独立管理 |

---

## 3. 目录结构

K8s 功能以独立子模块 `k8s/` 组织在 `db_schema_sync_client` 内部，与现有数据库同步代码完全隔离：

```
src/db_schema_sync_client/
├── k8s/
│   ├── __init__.py
│   ├── domain/
│   │   ├── __init__.py
│   │   └── models.py               # KubeClusterConfig, DeploymentSnapshot, ContainerImage 等
│   ├── infrastructure/
│   │   ├── __init__.py
│   │   ├── kubeconfig_store.py     # kubeconfig 文件 copy/存储/删除
│   │   ├── k8s_client.py           # kubernetes SDK 封装（list_deployments, patch_deployment_images）
│   │   └── k8s_store.py            # SQLite CRUD（3 张表）
│   ├── services/
│   │   ├── __init__.py
│   │   ├── snapshot_service.py     # 拉取 Deployment、创建/查询/删除快照
│   │   └── rollback_service.py     # 执行回滚，返回 RollbackResult
│   └── ui/
│       ├── __init__.py
│       ├── workers.py              # QThread Workers（拉取/备份/回滚）
│       ├── cluster_config_dialog.py    # 添加/编辑集群（上传 kubeconfig + namespace 配置）
│       ├── cluster_manager_dialog.py   # 集群 CRUD 管理列表
│       ├── snapshot_dialog.py          # 快照历史查看 + 触发回滚
│       ├── rollback_confirm_dialog.py  # 回滚预览确认
│       └── k8s_page.py                 # 主页面（集群/NS 选择 + Deployment 列表 + 备份/回滚）
```

---

## 4. 数据模型

### SQLite 新增三张表

```sql
-- 已注册的 K8s 集群
CREATE TABLE k8s_cluster_configs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,                       -- 显示名称，例如 "生产集群"
    kubeconfig_path TEXT NOT NULL,            -- 本地存储的 kubeconfig 文件路径
    context_name TEXT NOT NULL,               -- kubeconfig 中选用的 context
    namespaces_json TEXT NOT NULL DEFAULT '[]', -- JSON 数组，例如 ["dev","test","pre","prod"]
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 部署镜像快照（一次备份操作）
CREATE TABLE k8s_deployment_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_config_id INTEGER NOT NULL REFERENCES k8s_cluster_configs(id) ON DELETE CASCADE,
    namespace TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',            -- 用户填写的备注
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 快照内各容器的镜像记录
CREATE TABLE k8s_image_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL REFERENCES k8s_deployment_snapshots(id) ON DELETE CASCADE,
    deployment_name TEXT NOT NULL,
    container_name TEXT NOT NULL,
    image TEXT NOT NULL                       -- 完整 image 字符串，例如 nginx:1.21 或 registry/app@sha256:...
);
```

---

## 5. 关键模块说明

### 5.1 `k8s_client.py` — Kubernetes SDK 封装

- 懒加载 `AppsV1Api`（首次调用时才连接集群）
- `list_contexts(kubeconfig_path)` 静态方法，用于 UI 选 context
- `list_deployments(namespace)` 返回 `List[DeploymentInfo]`
- `patch_deployment_images(namespace, deployment_name, container_images)` 执行 strategic merge patch

### 5.2 `snapshot_service.py` — 快照业务逻辑

- `list_deployments(config, namespace)` — 拉取实时 Deployment 列表
- `create_snapshot(config, namespace, deployment_names, note)` — `deployment_names` 为空时备份全部
- `list_snapshots / get_snapshot / delete_snapshot` — 快照查询与删除

### 5.3 `rollback_service.py` — 回滚业务逻辑

- `execute_rollback(config, snapshot_id)` — 遍历快照内所有 Deployment，依次 patch；部分失败时继续执行其余项，最终返回 `RollbackResult`（total / succeeded / failed / errors）

### 5.4 Workers（异步 QThread）

| Worker | 触发场景 |
|---|---|
| `FetchDeploymentsWorker` | 点击"刷新"，拉取实时 Deployment 列表 |
| `CreateSnapshotWorker` | 点击"备份选中"或"备份全部" |
| `RollbackWorker` | 回滚确认对话框点击"确认回滚" |

---

## 6. UI 交互流程

```
K8s 主页面（k8s_page.py）
├── 顶部工具栏
│   ├── [集群 ▼] → 切换时自动更新 Namespace 下拉
│   ├── [Namespace ▼]
│   ├── [刷新] → 后台拉取 Deployment 列表（FetchDeploymentsWorker）
│   └── [管理集群] → 打开 ClusterManagerDialog
├── Deployment 多选表格（勾选框 / 名称 / 副本数 / 首个容器镜像）
└── 底部操作栏
    ├── [备份选中] → 仅备份勾选的行
    ├── [备份全部] → 不限制 deployment_names
    └── [历史快照…] → 打开 SnapshotDialog

ClusterManagerDialog（集群管理）
├── 集群列表（名称 / Context / Namespaces / 创建时间）
└── [+ 新增集群] → ClusterConfigDialog
    ├── 输入显示名称
    ├── 浏览并上传 kubeconfig 文件 → 自动解析可用 context 列表
    ├── 选择 context
    └── 维护 namespace 列表（预填 dev/test/pre/prod，可增删）

SnapshotDialog（快照历史）
├── 快照列表（ID / 备注 / Deployment 数 / 创建时间）
├── [回滚此快照] → RollbackConfirmDialog
│   ├── 展示所有将被修改的 Deployment + Container + 目标 image
│   └── [确认回滚] → RollbackWorker → 显示结果 → 刷新主页面
└── [删除快照]
```

---

## 7. 集成点

| 位置 | 改动 |
|---|---|
| `paths.py` | 新增 `development_kubeconfigs_dir()` / `kubeconfigs_dir()` |
| `app.py` | 初始化 `K8sStore` + `KubeconfigStore`，传入 `MainWindow` |
| `ui/main_window.py` | `__init__` 接收 `k8s_store`/`kubeconfig_store`，导航新增"K8s 部署管理"条目 |
| `requirements.txt` | 新增 `kubernetes>=29.0.0` |
| 现有数据库同步代码 | **零改动** |

---

## 8. 依赖

| 包 | 版本要求 | 用途 |
|---|---|---|
| `kubernetes` | >=29.0.0 | 官方 Python K8s SDK，操作 AppsV1Api |

---

## 9. 二期规划（未实现）

- 回滚后轮询 Deployment rollout 状态（`readyReplicas` 检查）
- 快照"从集群同步 namespace"按钮（自动填充实际 namespace 列表）
- 快照支持备注编辑
- 多集群批量操作
- 回滚操作审计日志（接入现有 `cluster_audit_logs` 表）
