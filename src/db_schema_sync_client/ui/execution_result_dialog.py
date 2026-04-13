"""Execution result dialog showing per-statement sync outcomes."""

from __future__ import annotations

from html import escape
from pathlib import Path

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QBrush, QColor
from PyQt6.QtWidgets import (
    QComboBox,
    QDialog,
    QFileDialog,
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

        self._summary = QLabel()
        self._summary.setStyleSheet("font-weight: bold; padding: 4px 0;")
        layout.addWidget(self._summary)

        toolbar = QHBoxLayout()
        toolbar.addWidget(QLabel("显示:"))
        self._filter_combo = QComboBox()
        self._filter_combo.addItem("全部 SQL", "all")
        self._filter_combo.addItem("仅失败 SQL", "failed")
        self._filter_combo.currentIndexChanged.connect(self._apply_filter)
        toolbar.addWidget(self._filter_combo)

        self._export_failed_button = QPushButton("导出失败 SQL (.xls)")
        self._export_failed_button.clicked.connect(self._export_failed_sql_xls)
        toolbar.addWidget(self._export_failed_button)
        toolbar.addStretch()
        layout.addLayout(toolbar)

        table = QTableWidget(0, 3)
        table.setHorizontalHeaderLabels(["SQL", "状态", "错误信息"])
        table.horizontalHeader().setStretchLastSection(True)
        table.setColumnWidth(0, 480)
        table.setColumnWidth(1, 70)
        table.setWordWrap(True)
        table.setTextElideMode(Qt.TextElideMode.ElideNone)
        table.verticalHeader().setDefaultSectionSize(22)

        # Click on a failed row → show full error in message box
        table.cellClicked.connect(lambda row, _col: self._on_cell_clicked(row))
        self._table = table
        self._visible_results = tuple(self.result.results)
        layout.addWidget(table)

        buttons = QHBoxLayout()
        buttons.addStretch()
        close_button = QPushButton("关闭")
        close_button.clicked.connect(self.accept)
        buttons.addWidget(close_button)
        layout.addLayout(buttons)

        self._apply_filter()

    def _apply_filter(self) -> None:
        mode = self._filter_combo.currentData()
        if mode == "failed":
            visible_results = tuple(item for item in self.result.results if item.status == "failed")
        else:
            visible_results = tuple(self.result.results)

        self._visible_results = visible_results
        self._summary.setText(
            f"成功: {self.result.success_count}  |  "
            f"失败: {self.result.failure_count}  |  "
            f"总计: {self.result.success_count + self.result.failure_count}  |  "
            f"当前显示: {len(visible_results)}"
        )
        self._populate_table(visible_results)
        self._export_failed_button.setEnabled(self.result.failure_count > 0)

    def _populate_table(self, results) -> None:
        fail_brush = QBrush(_COLOR_FAIL)
        table = self._table
        table.setRowCount(len(results))

        for row_index, item in enumerate(results):
            sql_item = QTableWidgetItem(item.statement)
            sql_item.setFlags(sql_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            sql_item.setToolTip(item.statement)
            table.setItem(row_index, 0, sql_item)

            status_text = "失败" if item.status == "failed" else "成功"
            status_item = QTableWidgetItem(status_text)
            status_item.setFlags(status_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            status_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            if item.status == "failed":
                status_item.setForeground(Qt.GlobalColor.red)
            else:
                status_item.setForeground(Qt.GlobalColor.darkGreen)
            table.setItem(row_index, 1, status_item)

            error_item = QTableWidgetItem(item.error_message or "")
            error_item.setFlags(error_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            error_item.setToolTip(item.error_message or "")
            if item.status == "failed":
                error_item.setForeground(Qt.GlobalColor.red)
            table.setItem(row_index, 2, error_item)

            if item.status == "failed":
                for col in range(3):
                    table.item(row_index, col).setBackground(fail_brush)

        table.resizeRowsToContents()

    def _export_failed_sql_xls(self) -> None:
        failed_results = [item for item in self.result.results if item.status == "failed"]
        if not failed_results:
            QMessageBox.information(self, "导出失败 SQL", "当前没有失败 SQL 可导出")
            return

        default_path = str(Path.home() / "failed_sql_results.xls")
        path, _ = QFileDialog.getSaveFileName(
            self,
            "导出失败 SQL",
            default_path,
            "Excel 文件 (*.xls)",
        )
        if not path:
            return
        if not path.lower().endswith(".xls"):
            path += ".xls"

        rows = []
        for index, item in enumerate(failed_results, start=1):
            rows.append(
                "<tr>"
                f"<td>{index}</td>"
                f"<td>{escape(item.statement)}</td>"
                f"<td>{escape(item.error_message or '')}</td>"
                "</tr>"
            )

        html = (
            "<html><head><meta charset=\"utf-8\"></head><body>"
            "<table border=\"1\">"
            "<tr><th>序号</th><th>失败SQL</th><th>错误信息</th></tr>"
            + "".join(rows)
            + "</table></body></html>"
        )

        try:
            Path(path).write_text(html, encoding="utf-8")
        except Exception as exc:
            QMessageBox.warning(self, "导出失败 SQL", f"导出失败: {exc}")
            return

        QMessageBox.information(self, "导出失败 SQL", f"已导出到: {path}")

    def _on_cell_clicked(self, row: int) -> None:
        item = self._visible_results[row]
        if item.status == "failed" and item.error_message:
            msg = QMessageBox(self)
            msg.setWindowTitle(f"错误详情 — 第 {row + 1} 条 SQL")
            msg.setIcon(QMessageBox.Icon.Warning)
            msg.setText("<b>SQL:</b>")
            msg.setInformativeText(item.statement)
            msg.setDetailedText(item.error_message)
            msg.exec()
