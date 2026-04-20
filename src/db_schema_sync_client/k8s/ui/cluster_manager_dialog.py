"""Cluster manager dialog — list, add, edit and delete K8s cluster configs."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from PyQt6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from db_schema_sync_client.k8s.domain.models import KubeClusterConfig
from db_schema_sync_client.k8s.infrastructure.k8s_store import K8sStore
from db_schema_sync_client.k8s.infrastructure.kubeconfig_store import KubeconfigStore
from db_schema_sync_client.k8s.ui.cluster_config_dialog import ClusterConfigDialog


class ClusterManagerDialog(QDialog):
    """CRUD management for registered K8s clusters."""

    def __init__(
        self,
        k8s_store: K8sStore,
        kubeconfig_store: KubeconfigStore,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._k8s_store = k8s_store
        self._kubeconfig_store = kubeconfig_store

        self.setWindowTitle("K8s 集群管理")
        self.setMinimumSize(680, 400)
        self._build_ui()
        self._load()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(["ID", "名称", "Context", "Namespaces", "创建时间"])
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setAlternatingRowColors(True)
        self._table.verticalHeader().setVisible(False)
        self._table.doubleClicked.connect(self._edit)
        layout.addWidget(self._table)

        btn_row = QHBoxLayout()
        add_btn = QPushButton("+ 新增集群")
        add_btn.clicked.connect(self._add)
        btn_row.addWidget(add_btn)

        edit_btn = QPushButton("编辑")
        edit_btn.clicked.connect(self._edit)
        btn_row.addWidget(edit_btn)

        del_btn = QPushButton("删除")
        del_btn.clicked.connect(self._delete)
        btn_row.addWidget(del_btn)

        btn_row.addStretch()

        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.accept)
        btn_row.addWidget(close_btn)

        layout.addLayout(btn_row)

    def _load(self) -> None:
        self._configs = self._k8s_store.list_cluster_configs()
        self._table.setRowCount(0)
        for cfg in self._configs:
            row = self._table.rowCount()
            self._table.insertRow(row)
            self._table.setItem(row, 0, QTableWidgetItem(str(cfg.id)))
            self._table.setItem(row, 1, QTableWidgetItem(cfg.name))
            self._table.setItem(row, 2, QTableWidgetItem(cfg.context_name))
            self._table.setItem(row, 3, QTableWidgetItem(", ".join(cfg.namespaces)))
            self._table.setItem(row, 4, QTableWidgetItem(cfg.created_at or ""))

    def _selected_config(self) -> Optional[KubeClusterConfig]:
        row = self._table.currentRow()
        if row < 0 or row >= len(self._configs):
            return None
        return self._configs[row]

    def _add(self) -> None:
        dialog = ClusterConfigDialog(parent=self)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return

        kubeconfig_path = dialog.get_kubeconfig_path()
        if kubeconfig_path is None:
            return

        # Persist a placeholder first to get an ID
        config = KubeClusterConfig(
            name=dialog.get_name(),
            kubeconfig_path=str(kubeconfig_path),
            context_name=dialog.get_context(),
            namespaces=dialog.get_namespaces(),
        )
        saved = self._k8s_store.save_cluster_config(config)

        # Copy the kubeconfig file into the managed store
        dest = self._kubeconfig_store.save(saved.id, kubeconfig_path)

        # Update the record with the managed path
        updated = KubeClusterConfig(
            id=saved.id,
            name=saved.name,
            kubeconfig_path=str(dest),
            context_name=saved.context_name,
            namespaces=saved.namespaces,
        )
        self._k8s_store.save_cluster_config(updated)
        self._load()

    def _edit(self) -> None:
        cfg = self._selected_config()
        if cfg is None:
            QMessageBox.information(self, "提示", "请先选中一个集群。")
            return

        dialog = ClusterConfigDialog(config=cfg, parent=self)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return

        new_path = dialog.get_kubeconfig_path()
        kubeconfig_path_str = cfg.kubeconfig_path
        if new_path is not None:
            dest = self._kubeconfig_store.save(cfg.id, new_path)
            kubeconfig_path_str = str(dest)

        updated = KubeClusterConfig(
            id=cfg.id,
            name=dialog.get_name(),
            kubeconfig_path=kubeconfig_path_str,
            context_name=dialog.get_context(),
            namespaces=dialog.get_namespaces(),
        )
        self._k8s_store.save_cluster_config(updated)
        self._load()

    def _delete(self) -> None:
        cfg = self._selected_config()
        if cfg is None:
            QMessageBox.information(self, "提示", "请先选中一个集群。")
            return
        reply = QMessageBox.question(
            self,
            "确认删除",
            f"确定要删除集群「{cfg.name}」吗？相关快照也会一并删除，此操作不可撤销。",
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        self._k8s_store.delete_cluster_config(cfg.id)
        self._kubeconfig_store.delete(cfg.id)
        self._load()
