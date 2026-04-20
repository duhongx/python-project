"""Main K8s deployment management page.

Layout:
  ┌────────────────────────────────────────────────────────────┐
  │  [集群 ▼]  [Namespace ▼]  [刷新]                 [管理集群] │
  ├────────────────────────────────────────────────────────────┤
  │  Deployment 多选表格 (名称 / 副本数 / 镜像摘要)              │
  ├────────────────────────────────────────────────────────────┤
  │  [备份选中]  [备份全部]                  [历史快照…]          │
  └────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDialog,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from db_schema_sync_client.k8s.domain.models import DeploymentInfo, KubeClusterConfig
from db_schema_sync_client.k8s.infrastructure.k8s_store import K8sStore
from db_schema_sync_client.k8s.infrastructure.kubeconfig_store import KubeconfigStore
from db_schema_sync_client.k8s.services.rollback_service import RollbackService
from db_schema_sync_client.k8s.services.snapshot_service import SnapshotService
from db_schema_sync_client.k8s.ui.cluster_config_dialog import ClusterConfigDialog
from db_schema_sync_client.k8s.ui.rollback_confirm_dialog import RollbackConfirmDialog
from db_schema_sync_client.k8s.ui.snapshot_dialog import SnapshotDialog
from db_schema_sync_client.k8s.ui.workers import (
    CreateSnapshotWorker,
    FetchDeploymentsWorker,
    RollbackWorker,
)


class K8sPage(QWidget):
    """K8s deployment backup and rollback management page."""

    def __init__(self, k8s_store: K8sStore, kubeconfig_store: KubeconfigStore, parent=None) -> None:
        super().__init__(parent)
        self._k8s_store = k8s_store
        self._kubeconfig_store = kubeconfig_store
        self._snapshot_service = SnapshotService(k8s_store, kubeconfig_store)
        self._rollback_service = RollbackService(k8s_store)

        self._deployments: List[DeploymentInfo] = []
        self._worker: Optional[QThread] = None

        self._build_ui()
        self._load_clusters()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)

        # ── Top toolbar ──────────────────────────────────────────────
        toolbar = QHBoxLayout()

        toolbar.addWidget(QLabel("集群:"))
        self._cluster_combo = QComboBox()
        self._cluster_combo.setMinimumWidth(200)
        self._cluster_combo.currentIndexChanged.connect(self._on_cluster_changed)
        toolbar.addWidget(self._cluster_combo)

        toolbar.addWidget(QLabel("Namespace:"))
        self._ns_combo = QComboBox()
        self._ns_combo.setMinimumWidth(120)
        toolbar.addWidget(self._ns_combo)

        refresh_btn = QPushButton("刷新")
        refresh_btn.clicked.connect(self.refresh)
        toolbar.addWidget(refresh_btn)

        toolbar.addStretch()

        manage_btn = QPushButton("管理集群")
        manage_btn.clicked.connect(self._manage_clusters)
        toolbar.addWidget(manage_btn)

        layout.addLayout(toolbar)

        # ── Deployment table ─────────────────────────────────────────
        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(["", "Deployment 名称", "副本数", "镜像（首个容器）"])
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        self._table.setAlternatingRowColors(True)
        self._table.verticalHeader().setVisible(False)
        layout.addWidget(self._table, 1)

        # ── Bottom action bar ─────────────────────────────────────────
        action_bar = QHBoxLayout()

        self._backup_selected_btn = QPushButton("备份选中")
        self._backup_selected_btn.clicked.connect(self._backup_selected)
        action_bar.addWidget(self._backup_selected_btn)

        self._backup_all_btn = QPushButton("备份全部")
        self._backup_all_btn.clicked.connect(self._backup_all)
        action_bar.addWidget(self._backup_all_btn)

        action_bar.addStretch()

        self._history_btn = QPushButton("历史快照…")
        self._history_btn.clicked.connect(self._open_snapshot_history)
        action_bar.addWidget(self._history_btn)

        layout.addLayout(action_bar)

        # ── Status bar ────────────────────────────────────────────────
        self._status_label = QLabel("")
        self._status_label.setStyleSheet("color: #6c757d; font-size: 12px;")
        layout.addWidget(self._status_label)

    # ------------------------------------------------------------------
    # Data loading helpers
    # ------------------------------------------------------------------

    def _load_clusters(self) -> None:
        self._cluster_combo.blockSignals(True)
        self._cluster_combo.clear()
        configs = self._k8s_store.list_cluster_configs()
        for cfg in configs:
            self._cluster_combo.addItem(cfg.name, cfg)
        self._cluster_combo.blockSignals(False)
        self._on_cluster_changed()

    def _on_cluster_changed(self) -> None:
        self._ns_combo.clear()
        cfg = self._current_cluster()
        if cfg is None:
            return
        for ns in cfg.namespaces:
            self._ns_combo.addItem(ns)

    def _current_cluster(self) -> Optional[KubeClusterConfig]:
        return self._cluster_combo.currentData()

    def _current_namespace(self) -> Optional[str]:
        ns = self._ns_combo.currentText()
        return ns if ns else None

    # ------------------------------------------------------------------
    # Refresh (fetch live deployments)
    # ------------------------------------------------------------------

    def refresh(self) -> None:
        cfg = self._current_cluster()
        ns = self._current_namespace()
        if cfg is None:
            self._status_label.setText("请先添加并选择一个 K8s 集群。")
            return
        if ns is None:
            self._status_label.setText("请选择 Namespace。")
            return

        self._set_loading(True)
        self._status_label.setText("正在连接集群，加载 Deployment 列表…")
        self._worker = FetchDeploymentsWorker(self._snapshot_service, cfg, ns, parent=self)
        self._worker.finished.connect(self._on_deployments_loaded)
        self._worker.error.connect(self._on_worker_error)
        self._worker.start()

    def _on_deployments_loaded(self, deployments: List[DeploymentInfo]) -> None:
        self._deployments = deployments
        self._populate_table(deployments)
        self._set_loading(False)
        self._status_label.setText(f"共 {len(deployments)} 个 Deployment。")

    def _populate_table(self, deployments: List[DeploymentInfo]) -> None:
        self._table.setRowCount(0)
        for dep in deployments:
            row = self._table.rowCount()
            self._table.insertRow(row)

            # Checkbox column via checkable item
            check_item = QTableWidgetItem()
            check_item.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)
            check_item.setCheckState(Qt.CheckState.Unchecked)
            self._table.setItem(row, 0, check_item)

            self._table.setItem(row, 1, QTableWidgetItem(dep.name))
            self._table.setItem(row, 2, QTableWidgetItem(
                f"{dep.ready_replicas}/{dep.replicas}"
            ))
            first_image = dep.containers[0].image if dep.containers else "—"
            self._table.setItem(row, 3, QTableWidgetItem(first_image))

    def _set_loading(self, loading: bool) -> None:
        self._backup_selected_btn.setEnabled(not loading)
        self._backup_all_btn.setEnabled(not loading)
        self._history_btn.setEnabled(not loading)

    def _on_worker_error(self, msg: str) -> None:
        self._set_loading(False)
        self._status_label.setText(f"错误: {msg}")
        QMessageBox.critical(self, "操作失败", msg)

    # ------------------------------------------------------------------
    # Backup
    # ------------------------------------------------------------------

    def _collect_checked_names(self) -> List[str]:
        names = []
        for row in range(self._table.rowCount()):
            check_item = self._table.item(row, 0)
            name_item = self._table.item(row, 1)
            if (
                check_item is not None
                and check_item.checkState() == Qt.CheckState.Checked
                and name_item is not None
            ):
                names.append(name_item.text())
        return names

    def _do_backup(self, deployment_names: List[str]) -> None:
        cfg = self._current_cluster()
        ns = self._current_namespace()
        if cfg is None or ns is None:
            return

        note, ok = QInputDialog.getText(
            self, "备份备注", "请输入本次备份的备注（可为空）："
        )
        if not ok:
            return

        self._set_loading(True)
        self._status_label.setText("正在创建快照…")
        self._worker = CreateSnapshotWorker(
            self._snapshot_service, cfg, ns, deployment_names, note, parent=self
        )
        self._worker.finished.connect(self._on_snapshot_created)
        self._worker.error.connect(self._on_worker_error)
        self._worker.start()

    def _backup_selected(self) -> None:
        names = self._collect_checked_names()
        if not names:
            QMessageBox.information(self, "提示", "请先勾选要备份的 Deployment。")
            return
        self._do_backup(names)

    def _backup_all(self) -> None:
        if not self._deployments:
            QMessageBox.information(self, "提示", "当前没有可备份的 Deployment，请先刷新。")
            return
        self._do_backup([])  # empty list → all deployments

    def _on_snapshot_created(self, snapshot) -> None:
        self._set_loading(False)
        self._status_label.setText(
            f"快照创建成功（ID={snapshot.id}，覆盖 {len(snapshot.records)} 个 Deployment）。"
        )
        QMessageBox.information(
            self,
            "备份成功",
            f"已创建快照 ID={snapshot.id}，共记录 {len(snapshot.records)} 个 Deployment 的镜像版本。",
        )

    # ------------------------------------------------------------------
    # Snapshot history & rollback
    # ------------------------------------------------------------------

    def _open_snapshot_history(self) -> None:
        cfg = self._current_cluster()
        ns = self._current_namespace()
        if cfg is None or ns is None:
            QMessageBox.information(self, "提示", "请先选择集群和 Namespace。")
            return

        dialog = SnapshotDialog(
            config=cfg,
            namespace=ns,
            snapshot_service=self._snapshot_service,
            on_rollback=self._confirm_rollback,
            parent=self,
        )
        dialog.exec()

    def _confirm_rollback(self, snapshot) -> None:
        # Build live_map from the most recently fetched deployment list.
        # Keys: deployment_name → {container_name → current_image}
        live_map = {
            dep.name: {ci.container_name: ci.image for ci in dep.containers}
            for dep in self._deployments
        }
        confirm = RollbackConfirmDialog(snapshot, live_map=live_map, parent=self)
        if confirm.exec() != QDialog.DialogCode.Accepted:
            return

        cfg = self._current_cluster()
        self._set_loading(True)
        self._status_label.setText("正在执行回滚…")
        self._worker = RollbackWorker(
            self._rollback_service, cfg, snapshot.id, parent=self
        )
        self._worker.finished.connect(self._on_rollback_finished)
        self._worker.progress.connect(self._status_label.setText)
        self._worker.error.connect(self._on_worker_error)
        self._worker.start()

    def _on_rollback_finished(self, result) -> None:
        self._set_loading(False)

        if result.no_changes:
            # All deployments already at snapshot version — nothing was patched
            names = "、".join(result.skipped_names) if result.skipped_names else "（无）"
            msg = (
                f"快照中所有 {result.total} 个 Deployment 的镜像版本与当前集群完全一致，"
                f"无需回滚。\n\n涉及服务：{names}"
            )
            self._status_label.setText("镜像版本未变化，无需回滚。")
            QMessageBox.information(self, "无需回滚", msg)

        elif result.ok:
            # At least one deployment was patched, no failures
            skip_note = ""
            if result.skipped > 0:
                skip_note = f"，{result.skipped} 个镜像已是目标版本（已跳过）"
            msg = f"回滚成功，已更新 {result.succeeded} 个 Deployment{skip_note}。"
            self._status_label.setText(msg)
            QMessageBox.information(self, "回滚成功", msg)

        else:
            details = "\n".join(result.errors)
            skip_note = f"，{result.skipped} 个已跳过" if result.skipped > 0 else ""
            msg = (
                f"回滚部分失败：成功 {result.succeeded} 个{skip_note}，"
                f"失败 {result.failed}/{result.total}。\n\n"
                f"失败详情：\n{details}"
            )
            self._status_label.setText(f"回滚部分失败（{result.failed}/{result.total}）。")
            QMessageBox.warning(self, "回滚部分失败", msg)

        self.refresh()

    # ------------------------------------------------------------------
    # Cluster management
    # ------------------------------------------------------------------

    def _manage_clusters(self) -> None:
        from db_schema_sync_client.k8s.ui.cluster_manager_dialog import ClusterManagerDialog

        dialog = ClusterManagerDialog(
            k8s_store=self._k8s_store,
            kubeconfig_store=self._kubeconfig_store,
            parent=self,
        )
        dialog.exec()
        self._load_clusters()
