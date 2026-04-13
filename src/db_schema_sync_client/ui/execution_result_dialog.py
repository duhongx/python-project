"""Execution result dialog showing per-statement sync outcomes."""

from __future__ import annotations

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QBrush, QColor
from PyQt6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from db_schema_sync_client.services.sync_executor import SyncExecutionResult

_COLOR_FAIL = QColor("#fff3cd")


class ExecutionResultDialog(QDialog):
    """Display the results of a sync execution run."""

    def __init__(self, result: SyncExecutionResult, parent=None) -> None:
        super().__init__(parent)
        self.result = result
        self.setWindowTitle("执行结果")
        self.setModal(True)
        self.resize(1000, 540)
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        summary = QLabel(
            f"成功: {self.result.success_count}  |  "
            f"失败: {self.result.failure_count}  |  "
            f"总计: {self.result.success_count + self.result.failure_count}"
        )
        summary.setStyleSheet("font-weight: bold; padding: 4px 0;")
        layout.addWidget(summary)

        table = QTableWidget(len(self.result.results), 3)
        table.setHorizontalHeaderLabels(["SQL", "状态", "错误信息"])
        table.horizontalHeader().setStretchLastSection(True)
        table.setColumnWidth(0, 480)
        table.setColumnWidth(1, 70)
        table.setWordWrap(True)
        table.setTextElideMode(Qt.TextElideMode.ElideNone)
        table.verticalHeader().setDefaultSectionSize(22)

        fail_brush = QBrush(_COLOR_FAIL)

        for row_index, item in enumerate(self.result.results):
            sql_item = QTableWidgetItem(item.statement)
            sql_item.setFlags(sql_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            sql_item.setToolTip(item.statement)
            table.setItem(row_index, 0, sql_item)

            status_item = QTableWidgetItem(item.status)
            status_item.setFlags(status_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            status_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            if item.status == "failed":
                status_item.setForeground(Qt.GlobalColor.red)
                for c in range(3):
                    it = table.item(row_index, c)
                    if it:
                        it.setBackground(fail_brush)
            else:
                status_item.setForeground(Qt.GlobalColor.darkGreen)
            table.setItem(row_index, 1, status_item)

            error_item = QTableWidgetItem(item.error_message or "")
            error_item.setFlags(error_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            error_item.setToolTip(item.error_message or "")
            if item.status == "failed":
                error_item.setBackground(fail_brush)
                error_item.setForeground(Qt.GlobalColor.red)
            table.setItem(row_index, 2, error_item)

        table.resizeRowsToContents()
        # Click on a failed row → show full error in message box
        table.cellClicked.connect(lambda row, _col: self._on_cell_clicked(row))
        self._table = table
        layout.addWidget(table)

        buttons = QHBoxLayout()
        buttons.addStretch()
        close_button = QPushButton("关闭")
        close_button.clicked.connect(self.accept)
        buttons.addWidget(close_button)
        layout.addLayout(buttons)

    def _on_cell_clicked(self, row: int) -> None:
        item = self.result.results[row]
        if item.status == "failed" and item.error_message:
            msg = QMessageBox(self)
            msg.setWindowTitle(f"错误详情 — 第 {row + 1} 条 SQL")
            msg.setIcon(QMessageBox.Icon.Warning)
            msg.setText("<b>SQL:</b>")
            msg.setInformativeText(item.statement)
            msg.setDetailedText(item.error_message)
            msg.exec()
