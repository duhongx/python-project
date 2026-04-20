"""History panel for viewing past comparisons and sync runs."""

from __future__ import annotations

import json
from typing import Optional

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QComboBox,
    QDateEdit,
    QDialog,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from db_schema_sync_client.infrastructure.app_store import AppStore


class HistoryDialog(QDialog):
    """Dialog showing comparison and sync history with filtering."""

    # Signal-like attribute: callers may set this to a callback ``(source_id, target_id) -> None``
    # which will be invoked when the user clicks "重新比对" on a compare task.
    regenerate_callback = None

    def __init__(self, app_store: AppStore, parent=None) -> None:
        super().__init__(parent)
        self.app_store = app_store
        self.setWindowTitle("历史记录")
        self.setModal(True)
        self.resize(880, 560)

        # Preload profile names for display
        self._profile_names: dict[int, str] = {}
        for p in self.app_store.list_profiles():
            if p.id is not None:
                self._profile_names[p.id] = p.name

        self._build_ui()
        self._refresh()

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        self.tabs = QTabWidget()

        # ---- Sync runs tab ----
        sync_tab = QWidget()
        sync_layout = QVBoxLayout(sync_tab)

        # Filters
        sync_filter_row = QHBoxLayout()
        sync_filter_row.addWidget(QLabel("目标连接:"))
        self.sync_profile_filter = QComboBox()
        self.sync_profile_filter.addItem("全部", None)
        for pid, pname in self._profile_names.items():
            self.sync_profile_filter.addItem(pname, pid)
        self.sync_profile_filter.currentIndexChanged.connect(self._refresh_sync)
        sync_filter_row.addWidget(self.sync_profile_filter)

        sync_filter_row.addWidget(QLabel("状态:"))
        self.sync_status_filter = QComboBox()
        self.sync_status_filter.addItem("全部", None)
        self.sync_status_filter.addItem("success", "success")
        self.sync_status_filter.addItem("partial_failure", "partial_failure")
        self.sync_status_filter.addItem("running", "running")
        self.sync_status_filter.currentIndexChanged.connect(self._refresh_sync)
        sync_filter_row.addWidget(self.sync_status_filter)

        sync_filter_row.addStretch()
        sync_layout.addLayout(sync_filter_row)

        self.sync_table = QTableWidget(0, 5)
        self.sync_table.setHorizontalHeaderLabels(["ID", "目标连接", "类型", "状态", "时间"])
        self.sync_table.horizontalHeader().setStretchLastSection(True)
        self.sync_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.sync_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.sync_table.setAlternatingRowColors(True)
        self.sync_table.verticalHeader().setVisible(False)
        sync_layout.addWidget(self.sync_table)

        sync_btn_row = QHBoxLayout()
        detail_btn = QPushButton("查看 SQL 详情")
        detail_btn.clicked.connect(self._show_sync_detail)
        sync_btn_row.addWidget(detail_btn)
        sync_btn_row.addStretch()
        sync_layout.addLayout(sync_btn_row)

        self.tabs.addTab(sync_tab, "同步记录")

        # ---- Compare tasks tab ----
        compare_tab = QWidget()
        compare_layout = QVBoxLayout(compare_tab)

        # Filters
        cmp_filter_row = QHBoxLayout()
        cmp_filter_row.addWidget(QLabel("源端:"))
        self.cmp_source_filter = QComboBox()
        self.cmp_source_filter.addItem("全部", None)
        for pid, pname in self._profile_names.items():
            self.cmp_source_filter.addItem(pname, pid)
        self.cmp_source_filter.currentIndexChanged.connect(self._refresh_compare)
        cmp_filter_row.addWidget(self.cmp_source_filter)

        cmp_filter_row.addWidget(QLabel("目标:"))
        self.cmp_target_filter = QComboBox()
        self.cmp_target_filter.addItem("全部", None)
        for pid, pname in self._profile_names.items():
            self.cmp_target_filter.addItem(pname, pid)
        self.cmp_target_filter.currentIndexChanged.connect(self._refresh_compare)
        cmp_filter_row.addWidget(self.cmp_target_filter)

        cmp_filter_row.addWidget(QLabel("状态:"))
        self.cmp_status_filter = QComboBox()
        self.cmp_status_filter.addItem("全部", None)
        self.cmp_status_filter.addItem("completed", "completed")
        self.cmp_status_filter.addItem("failed", "failed")
        self.cmp_status_filter.currentIndexChanged.connect(self._refresh_compare)
        cmp_filter_row.addWidget(self.cmp_status_filter)

        cmp_filter_row.addStretch()
        compare_layout.addLayout(cmp_filter_row)

        self.compare_table = QTableWidget(0, 5)
        self.compare_table.setHorizontalHeaderLabels(["ID", "源端连接", "目标连接", "状态", "时间"])
        self.compare_table.horizontalHeader().setStretchLastSection(True)
        self.compare_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.compare_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.compare_table.setAlternatingRowColors(True)
        self.compare_table.verticalHeader().setVisible(False)
        compare_layout.addWidget(self.compare_table)

        cmp_btn_row = QHBoxLayout()
        cmp_detail_btn = QPushButton("查看比对详情")
        cmp_detail_btn.clicked.connect(self._show_compare_detail)
        cmp_btn_row.addWidget(cmp_detail_btn)

        regenerate_btn = QPushButton("重新比对")
        regenerate_btn.clicked.connect(self._regenerate_comparison)
        cmp_btn_row.addWidget(regenerate_btn)

        cmp_btn_row.addStretch()
        compare_layout.addLayout(cmp_btn_row)

        self.tabs.addTab(compare_tab, "比对记录")

        layout.addWidget(self.tabs)

        # Bottom buttons
        buttons = QHBoxLayout()
        refresh_btn = QPushButton("刷新")
        refresh_btn.clicked.connect(self._refresh)
        buttons.addWidget(refresh_btn)
        buttons.addStretch()
        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.accept)
        buttons.addWidget(close_btn)
        layout.addLayout(buttons)

    # ------------------------------------------------------------------
    # Data refresh
    # ------------------------------------------------------------------

    def _refresh(self) -> None:
        self._refresh_sync()
        self._refresh_compare()

    def _profile_display(self, profile_id) -> str:
        if profile_id is None:
            return ""
        pid = int(profile_id)
        return self._profile_names.get(pid, str(pid))

    def _refresh_sync(self) -> None:
        target_pid = self.sync_profile_filter.currentData()
        status = self.sync_status_filter.currentData()
        sync_runs = self.app_store.list_sync_runs(
            target_profile_id=target_pid,
            status=status,
        )
        self.sync_table.setRowCount(len(sync_runs))
        for i, run in enumerate(sync_runs):
            self.sync_table.setItem(i, 0, QTableWidgetItem(str(run["id"])))
            self.sync_table.setItem(i, 1, QTableWidgetItem(self._profile_display(run.get("target_profile_id"))))
            self.sync_table.setItem(i, 2, QTableWidgetItem(run.get("run_type", "")))
            self.sync_table.setItem(i, 3, QTableWidgetItem(run.get("status", "")))
            self.sync_table.setItem(i, 4, QTableWidgetItem(run.get("created_at", "")))

    def _refresh_compare(self) -> None:
        source_pid = self.cmp_source_filter.currentData()
        target_pid = self.cmp_target_filter.currentData()
        status = self.cmp_status_filter.currentData()
        tasks = self.app_store.list_compare_tasks(
            source_profile_id=source_pid,
            target_profile_id=target_pid,
            status=status,
        )
        self.compare_table.setRowCount(len(tasks))
        for i, task in enumerate(tasks):
            self.compare_table.setItem(i, 0, QTableWidgetItem(str(task["id"])))
            self.compare_table.setItem(i, 1, QTableWidgetItem(self._profile_display(task.get("source_profile_id"))))
            self.compare_table.setItem(i, 2, QTableWidgetItem(self._profile_display(task.get("target_profile_id"))))
            self.compare_table.setItem(i, 3, QTableWidgetItem(task.get("status", "")))
            self.compare_table.setItem(i, 4, QTableWidgetItem(task.get("created_at", "")))

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def _show_sync_detail(self) -> None:
        row = self.sync_table.currentRow()
        if row < 0:
            return
        run_id_item = self.sync_table.item(row, 0)
        if run_id_item is None:
            return
        run_id = int(run_id_item.text())
        statements = self.app_store.list_sync_statements(run_id)

        detail = SyncDetailDialog(run_id, statements, parent=self)
        detail.exec()

    def _show_compare_detail(self) -> None:
        row = self.compare_table.currentRow()
        if row < 0:
            return
        task_id_item = self.compare_table.item(row, 0)
        if task_id_item is None:
            return
        task_id = int(task_id_item.text())
        tasks = self.app_store.list_compare_tasks()
        task = next((t for t in tasks if t["id"] == task_id), None)
        if task is None:
            return

        detail = CompareDetailDialog(task_id, task, parent=self)
        detail.exec()

    def _regenerate_comparison(self) -> None:
        row = self.compare_table.currentRow()
        if row < 0:
            return
        task_id_item = self.compare_table.item(row, 0)
        if task_id_item is None:
            return
        task_id = int(task_id_item.text())
        tasks = self.app_store.list_compare_tasks()
        task = next((t for t in tasks if t["id"] == task_id), None)
        if task is None:
            return

        source_pid = task.get("source_profile_id")
        target_pid = task.get("target_profile_id")
        if self.regenerate_callback is not None:
            self.accept()  # close history dialog first
            self.regenerate_callback(source_pid, target_pid)


class SyncDetailDialog(QDialog):
    """Show individual SQL statements of a sync run."""

    def __init__(self, run_id: int, statements: list[dict], parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle(f"同步详情 — Run #{run_id}")
        self.setModal(True)
        self.resize(700, 400)

        layout = QVBoxLayout(self)
        table = QTableWidget(len(statements), 3)
        table.setHorizontalHeaderLabels(["SQL", "状态", "错误信息"])
        table.horizontalHeader().setStretchLastSection(True)
        table.setColumnWidth(0, 380)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)

        for i, st in enumerate(statements):
            table.setItem(i, 0, QTableWidgetItem(st.get("statement_text", "")))
            status_item = QTableWidgetItem(st.get("status", ""))
            if st.get("status") == "failed":
                status_item.setForeground(Qt.GlobalColor.red)
            table.setItem(i, 1, status_item)
            table.setItem(i, 2, QTableWidgetItem(st.get("error_message") or ""))

        layout.addWidget(table)
        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn)


class CompareDetailDialog(QDialog):
    """Show detailed results of a compare task."""

    def __init__(self, task_id: int, task: dict, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle(f"比对详情 — Task #{task_id}")
        self.setModal(True)
        self.resize(750, 500)

        layout = QVBoxLayout(self)

        info_label = QLabel(
            f"源端 Profile ID: {task.get('source_profile_id', 'N/A')}  |  "
            f"目标 Profile ID: {task.get('target_profile_id', 'N/A')}  |  "
            f"状态: {task.get('status', '')}  |  "
            f"时间: {task.get('created_at', '')}"
        )
        layout.addWidget(info_label)

        result_json_str = task.get("result_json") or ""
        if result_json_str:
            try:
                result_data = json.loads(result_json_str)
            except (json.JSONDecodeError, TypeError):
                result_data = None
        else:
            result_data = None

        if result_data:
            obj_diffs = result_data.get("object_diffs", [])
            col_diffs = result_data.get("column_diffs", [])

            layout.addWidget(QLabel(f"对象级差异: {len(obj_diffs)}  |  字段级差异: {len(col_diffs)}"))

            if obj_diffs:
                layout.addWidget(QLabel("对象级差异:"))
                obj_table = QTableWidget(len(obj_diffs), 4)
                obj_table.setHorizontalHeaderLabels(["Schema", "对象名", "状态", "分类"])
                obj_table.horizontalHeader().setStretchLastSection(True)
                obj_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
                for i, od in enumerate(obj_diffs):
                    obj_table.setItem(i, 0, QTableWidgetItem(od.get("schema", "")))
                    obj_table.setItem(i, 1, QTableWidgetItem(od.get("object", "")))
                    obj_table.setItem(i, 2, QTableWidgetItem(od.get("status", "")))
                    obj_table.setItem(i, 3, QTableWidgetItem(od.get("category", "")))
                layout.addWidget(obj_table)

            if col_diffs:
                layout.addWidget(QLabel("字段级差异:"))
                col_table = QTableWidget(len(col_diffs), 5)
                col_table.setHorizontalHeaderLabels(["Schema", "对象名", "字段名", "状态", "分类"])
                col_table.horizontalHeader().setStretchLastSection(True)
                col_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
                for i, cd in enumerate(col_diffs):
                    col_table.setItem(i, 0, QTableWidgetItem(cd.get("schema", "")))
                    col_table.setItem(i, 1, QTableWidgetItem(cd.get("object", "")))
                    col_table.setItem(i, 2, QTableWidgetItem(cd.get("column", "")))
                    col_table.setItem(i, 3, QTableWidgetItem(cd.get("status", "")))
                    col_table.setItem(i, 4, QTableWidgetItem(cd.get("category", "")))
                layout.addWidget(col_table)
        else:
            no_data = QLabel("无详细比对数据（历史记录未存储或格式不兼容）")
            layout.addWidget(no_data)

        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn)
