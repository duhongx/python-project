"""Cluster list management page."""

from __future__ import annotations

from typing import Optional

from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from db_schema_sync_client.domain.models import ClusterEnvironment, ClusterProfile
from db_schema_sync_client.ui.cluster_dialog import ClusterDialog


class ClusterListPage(QWidget):
    def __init__(self, app_store, parent=None, open_cluster_callback=None) -> None:
        super().__init__(parent)
        self.app_store = app_store
        self.open_cluster_callback = open_cluster_callback
        self._build_ui()
        self.refresh()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("环境"))

        self.environment_filter = QComboBox()
        self.environment_filter.addItem("全部", None)
        for environment in ClusterEnvironment:
            self.environment_filter.addItem(environment.value, environment)
        self.environment_filter.currentIndexChanged.connect(self.refresh)
        filter_row.addWidget(self.environment_filter)

        filter_row.addWidget(QLabel("关键字"))
        self.keyword_input = QLineEdit()
        self.keyword_input.setPlaceholderText("集群名称")
        self.keyword_input.textChanged.connect(self.refresh)
        filter_row.addWidget(self.keyword_input)

        self.enabled_only_checkbox = QCheckBox("仅启用")
        self.enabled_only_checkbox.toggled.connect(self.refresh)
        filter_row.addWidget(self.enabled_only_checkbox)

        filter_row.addStretch()

        add_button = QPushButton("+新增集群")
        add_button.clicked.connect(self._create_cluster)
        filter_row.addWidget(add_button)

        detail_button = QPushButton("查看总览")
        detail_button.clicked.connect(self._open_selected_cluster)
        filter_row.addWidget(detail_button)

        refresh_button = QPushButton("刷新")
        refresh_button.clicked.connect(self.refresh)
        filter_row.addWidget(refresh_button)

        layout.addLayout(filter_row)

        self.cluster_table = QTableWidget(0, 7)
        self.cluster_table.setHorizontalHeaderLabels(
            ["ID", "集群名称", "环境", "Patroni", "PG连接", "健康状态", "启用"]
        )
        self.cluster_table.horizontalHeader().setStretchLastSection(True)
        self.cluster_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.cluster_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.cluster_table.setAlternatingRowColors(True)
        self.cluster_table.verticalHeader().setVisible(False)
        self.cluster_table.doubleClicked.connect(self._edit_selected_cluster)
        layout.addWidget(self.cluster_table)

    def refresh(self) -> None:
        if self.app_store is None:
            self.cluster_table.setRowCount(0)
            return

        clusters = self.app_store.list_cluster_profiles(
            environment=self.environment_filter.currentData(),
            keyword=self.keyword_input.text().strip() or None,
            enabled_only=self.enabled_only_checkbox.isChecked(),
        )

        self.cluster_table.setRowCount(len(clusters))
        for row_index, cluster in enumerate(clusters):
            self._set_row(row_index, cluster)

    def _set_row(self, row_index: int, cluster: ClusterProfile) -> None:
        values = [
            "" if cluster.id is None else str(cluster.id),
            cluster.name,
            cluster.environment.value,
            str(len(cluster.patroni_endpoints)),
            "已配置" if cluster.pg_host and cluster.pg_username else "未配置",
            cluster.last_health_status or "未检查",
            "是" if cluster.is_enabled else "否",
        ]
        for column_index, value in enumerate(values):
            self.cluster_table.setItem(row_index, column_index, QTableWidgetItem(value))

    def _selected_cluster_id(self) -> Optional[int]:
        row = self.cluster_table.currentRow()
        if row < 0:
            return None
        item = self.cluster_table.item(row, 0)
        if item is None or not item.text():
            return None
        return int(item.text())

    def _create_cluster(self) -> None:
        dialog = ClusterDialog(self.app_store, parent=self)
        dialog.start_create()
        if dialog.exec():
            self.refresh()

    def _open_selected_cluster(self) -> None:
        cluster_id = self._selected_cluster_id()
        if cluster_id is None:
            return
        if callable(self.open_cluster_callback):
            self.open_cluster_callback(cluster_id)

    def _edit_selected_cluster(self, *_args) -> None:
        if self.app_store is None:
            return
        cluster_id = self._selected_cluster_id()
        if cluster_id is None:
            return
        cluster = self.app_store.get_cluster_profile(cluster_id)
        if cluster is None:
            return

        dialog = ClusterDialog(self.app_store, parent=self)
        dialog.start_edit(cluster)
        if dialog.exec():
            self.refresh()
