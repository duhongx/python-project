"""Rollback confirmation dialog.

Shows a comparison table: current live image vs rollback target image for each
container, so the user can verify exactly what will change before confirming.
"""

from __future__ import annotations

from typing import Dict

from PyQt6.QtGui import QBrush, QColor
from PyQt6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QLabel,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from db_schema_sync_client.k8s.domain.models import DeploymentSnapshot

# live_map type: {deployment_name: {container_name: current_image}}
LiveMap = Dict[str, Dict[str, str]]

_COLOR_CHANGED = QColor("#fff3cd")   # amber — image will be updated
_COLOR_SAME    = QColor("#f0f0f0")   # gray  — already at target, will be skipped


def _image_tag(image: str) -> str:
    """Return the tag part of an image string (after the last ':').

    '192.168.1.1:8888/repo/name:v1.2.3'  →  'v1.2.3'
    If there is no ':' after the last '/', returns the full image string.
    """
    # Strip registry/path: find ':' that appears after the last '/'
    slash_pos = image.rfind("/")
    colon_pos = image.find(":", slash_pos + 1)
    if colon_pos != -1:
        return image[colon_pos + 1:]
    return image


class RollbackConfirmDialog(QDialog):
    """Preview the rollback targets and ask for confirmation."""

    def __init__(
        self,
        snapshot: DeploymentSnapshot,
        live_map: LiveMap | None = None,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._snapshot = snapshot
        self._live_map: LiveMap = live_map or {}
        self.setWindowTitle("确认回滚")
        self.setMinimumWidth(900)
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setSpacing(12)

        summary = QLabel(
            f"快照：<b>{self._snapshot.note or '（无备注）'}</b>  "
            f"Namespace：<b>{self._snapshot.namespace}</b>  "
            f"时间：{self._snapshot.created_at}"
        )
        summary.setWordWrap(True)
        layout.addWidget(summary)

        changed_count = self._count_changed()

        if changed_count == 0:
            hint = QLabel("所有 Deployment 镜像版本与快照完全一致，执行回滚不会产生任何变更。")
            hint.setStyleSheet("color: #6c757d; font-weight: 600;")
        else:
            hint = QLabel(
                f"黄色行：镜像将被回滚（{changed_count} 行）。"
                f"灰色行：镜像与快照一致，回滚时会跳过。"
            )
            hint.setStyleSheet("color: #d97706; font-weight: 600;")
        layout.addWidget(hint)

        # Build flat row list — one row per deployment (container column removed)
        rows = []
        for record in self._snapshot.records:
            live_containers = self._live_map.get(record.deployment_name, {})
            # Show only the image tag (strip registry + repo prefix)
            target_images = ", ".join(_image_tag(ci.image) for ci in record.containers)
            current_images = ", ".join(
                _image_tag(live_containers.get(ci.container_name, "未知"))
                for ci in record.containers
            )
            changed = any(
                live_containers.get(ci.container_name, "").strip() != ci.image.strip()
                for ci in record.containers
            )
            rows.append((record.deployment_name, current_images, target_images, changed))

        table = QTableWidget(len(rows), 3)
        table.setHorizontalHeaderLabels(["Deployment", "当前镜像", "回滚目标镜像"])
        table.horizontalHeader().setStretchLastSection(True)
        table.setColumnWidth(0, 200)
        table.setColumnWidth(1, 320)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.verticalHeader().setVisible(False)

        for i, (dep, current, target, changed) in enumerate(rows):
            bg = _COLOR_CHANGED if changed else _COLOR_SAME
            for col, text in enumerate([dep, current, target]):
                item = QTableWidgetItem(text)
                item.setBackground(QBrush(bg))
                table.setItem(i, col, item)

        layout.addWidget(table)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.button(QDialogButtonBox.StandardButton.Ok).setText("确认回滚")
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _count_changed(self) -> int:
        """Return the number of deployment rows where any image will actually change."""
        count = 0
        for record in self._snapshot.records:
            live_containers = self._live_map.get(record.deployment_name, {})
            if any(
                live_containers.get(ci.container_name, "").strip() != ci.image.strip()
                for ci in record.containers
            ):
                count += 1
        return count
