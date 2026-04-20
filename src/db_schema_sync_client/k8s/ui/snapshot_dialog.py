"""Snapshot history dialog.

Lists all snapshots for a given cluster + namespace and allows the user to
view details or trigger a rollback.
"""

from __future__ import annotations

from typing import Callable, List, Optional

from PyQt6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from db_schema_sync_client.k8s.domain.models import DeploymentSnapshot, KubeClusterConfig
from db_schema_sync_client.k8s.services.snapshot_service import SnapshotService


class SnapshotDialog(QDialog):
    """Browse snapshot history and trigger rollbacks."""

    def __init__(
        self,
        config: KubeClusterConfig,
        namespace: str,
        snapshot_service: SnapshotService,
        on_rollback: Callable[[DeploymentSnapshot], None],
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._config = config
        self._namespace = namespace
        self._service = snapshot_service
        self._on_rollback = on_rollback

        self.setWindowTitle(f"快照历史 — {config.name} / {namespace}")
        self.setMinimumSize(720, 420)
        self._build_ui()
        self._load_snapshots()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        header = QHBoxLayout()
        header.addWidget(QLabel(f"集群: <b>{self._config.name}</b>  Namespace: <b>{self._namespace}</b>"))
        header.addStretch()
        refresh_btn = QPushButton("刷新")
        refresh_btn.clicked.connect(self._load_snapshots)
        header.addWidget(refresh_btn)
        layout.addLayout(header)

        self._table = QTableWidget(0, 4)
        self._table.setHorizontalHeaderLabels(["ID", "备注", "Deployment 数", "创建时间"])
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setAlternatingRowColors(True)
        self._table.verticalHeader().setVisible(False)
        layout.addWidget(self._table)

        btn_row = QHBoxLayout()
        btn_row.addStretch()

        self._rollback_btn = QPushButton("回滚此快照")
        self._rollback_btn.setEnabled(False)
        self._rollback_btn.clicked.connect(self._trigger_rollback)
        btn_row.addWidget(self._rollback_btn)

        delete_btn = QPushButton("删除快照")
        delete_btn.clicked.connect(self._delete_snapshot)
        btn_row.addWidget(delete_btn)

        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.reject)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)

        self._table.selectionModel().selectionChanged.connect(
            lambda: self._rollback_btn.setEnabled(len(self._table.selectedItems()) > 0)
        )

    def _load_snapshots(self) -> None:
        self._snapshots = self._service.list_snapshots(
            self._config.id, self._namespace
        )
        self._table.setRowCount(0)
        for snap in self._snapshots:
            row = self._table.rowCount()
            self._table.insertRow(row)
            self._table.setItem(row, 0, QTableWidgetItem(str(snap.id)))
            self._table.setItem(row, 1, QTableWidgetItem(snap.note or ""))
            self._table.setItem(row, 2, QTableWidgetItem(str(len(snap.records))))
            self._table.setItem(row, 3, QTableWidgetItem(snap.created_at or ""))

    def _selected_snapshot(self) -> Optional[DeploymentSnapshot]:
        row = self._table.currentRow()
        if row < 0 or row >= len(self._snapshots):
            return None
        return self._snapshots[row]

    def _trigger_rollback(self) -> None:
        snap = self._selected_snapshot()
        if snap is None:
            return
        self._on_rollback(snap)

    def _delete_snapshot(self) -> None:
        snap = self._selected_snapshot()
        if snap is None:
            QMessageBox.information(self, "提示", "请先选中一条快照。")
            return
        reply = QMessageBox.question(
            self,
            "确认删除",
            f"确定要删除快照「{snap.note or snap.id}」吗？此操作不可撤销。",
        )
        if reply == QMessageBox.StandardButton.Yes:
            self._service.delete_snapshot(snap.id)
            self._load_snapshots()
