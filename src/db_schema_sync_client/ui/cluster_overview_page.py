"""Cluster overview page."""

from __future__ import annotations

from typing import Optional

from PyQt6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from db_schema_sync_client.services.cluster_service import ClusterService


class ClusterOverviewPage(QWidget):
    def __init__(self, app_store, cluster_service: ClusterService, cluster=None, parent=None) -> None:
        super().__init__(parent)
        self.app_store = app_store
        self.cluster_service = cluster_service
        self.cluster = cluster
        self.on_back = None
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        top_row = QHBoxLayout()
        self.back_button = QPushButton("返回列表")
        self.back_button.clicked.connect(self._handle_back)
        top_row.addWidget(self.back_button)

        self.refresh_button = QPushButton("手动刷新")
        self.refresh_button.clicked.connect(self.refresh)
        top_row.addStretch()
        top_row.addWidget(self.refresh_button)
        layout.addLayout(top_row)

        self.summary_label = QLabel("未选择集群")
        self.summary_label.setStyleSheet(
            "background-color: #f8fafc; border: 1px solid #d0d7de; padding: 10px; font-weight: 600;"
        )
        layout.addWidget(self.summary_label)

        self.topology_label = QLabel("")
        self.topology_label.setWordWrap(True)
        self.topology_label.setStyleSheet("background-color: #ffffff; border: 1px solid #d0d7de; padding: 10px;")
        layout.addWidget(self.topology_label)

        self.node_table = QTableWidget(0, 7)
        self.node_table.setHorizontalHeaderLabels(
            ["节点", "角色", "状态", "timeline", "lag", "pending_restart", "last_seen"]
        )
        self.node_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.node_table)

        self.audit_table = QTableWidget(0, 5)
        self.audit_table.setHorizontalHeaderLabels(["时间", "操作人", "动作", "结果", "详情"])
        self.audit_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.audit_table)

    def set_cluster(self, cluster) -> None:
        self.cluster = cluster
        self.refresh()

    def refresh(self) -> None:
        if self.cluster is None:
            self.summary_label.setText("未选择集群")
            self.topology_label.setText("")
            self.node_table.setRowCount(0)
            self.audit_table.setRowCount(0)
            return

        overview = self.cluster_service.load_overview(self.cluster, self.app_store)
        self.summary_label.setText(
            f"Primary: {overview.primary_node} | Replica: {overview.replica_count} | "
            f"Patroni: {overview.patroni_healthy_count}/{overview.patroni_total_count} | "
            f"etcd: {overview.etcd_healthy_count}/{overview.etcd_total_count} | "
            f"PG连接: {overview.total_connections} / 活跃: {overview.active_connections}"
        )
        self.topology_label.setText("\n".join(overview.topology_lines) if overview.topology_lines else "暂无拓扑摘要")

        self.node_table.setRowCount(len(overview.nodes))
        for row_index, node in enumerate(overview.nodes):
            values = [
                node.name,
                node.role,
                node.status,
                node.timeline,
                node.lag,
                "true" if node.pending_restart else "false",
                node.last_seen,
            ]
            for column_index, value in enumerate(values):
                self.node_table.setItem(row_index, column_index, QTableWidgetItem(str(value)))

        self.audit_table.setRowCount(len(overview.recent_operations))
        for row_index, record in enumerate(overview.recent_operations):
            values = [
                record.get("created_at", ""),
                record.get("operator", ""),
                record.get("action", ""),
                record.get("status", ""),
                record.get("detail", ""),
            ]
            for column_index, value in enumerate(values):
                self.audit_table.setItem(row_index, column_index, QTableWidgetItem(str(value)))

    def _handle_back(self) -> None:
        if callable(self.on_back):
            self.on_back()
