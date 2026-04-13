"""Main application window."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Optional

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSplitter,
    QStatusBar,
    QToolBar,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from db_schema_sync_client.domain.diff import DiffCategory, SchemaDiff
from db_schema_sync_client.domain.models import ConnectionProfile, ConnectionRole, ObjectType, SchemaSnapshot
from db_schema_sync_client.infrastructure.app_store import AppStore
from db_schema_sync_client.infrastructure.db_connection import DatabaseConnectionFactory
from db_schema_sync_client.infrastructure.db_metadata import MetadataReader
from db_schema_sync_client.services.report_service import ReportService
from db_schema_sync_client.services.sql_generator import GeneratedSqlPlan, SqlGenerator
from db_schema_sync_client.services.sync_executor import SyncExecutor
from db_schema_sync_client.ui.comparison_panel import ComparisonPanel
from db_schema_sync_client.ui.config_dialog import ConnectionConfigDialog, ProfileManagerDialog
from db_schema_sync_client.ui.execution_result_dialog import ExecutionResultDialog
from db_schema_sync_client.ui.history_dialog import HistoryDialog
from db_schema_sync_client.ui.sql_preview_dialog import SqlPreviewDialog
from db_schema_sync_client.ui.workers import CompareWorker, MetadataWorker, SyncWorker


class MainWindow(QMainWindow):
    def __init__(self, app_store: Optional[AppStore] = None, parent=None) -> None:
        super().__init__(parent)
        self.app_store = app_store
        self.connection_factory = DatabaseConnectionFactory()
        self.metadata_reader = MetadataReader(self.connection_factory)
        self.sql_generator = SqlGenerator()
        self.report_service = ReportService()

        self.current_source_profile: Optional[ConnectionProfile] = None
        self.current_target_profile: Optional[ConnectionProfile] = None
        self.current_diff: Optional[SchemaDiff] = None
        self._source_snapshot: Optional[SchemaSnapshot] = None
        self._target_snapshot: Optional[SchemaSnapshot] = None
        self._workers: list = []  # active QThread workers – prevent GC

        self.setWindowTitle("数据库结构同步客户端")
        self.resize(1200, 750)
        self._build_ui()
        self._refresh_profile_combos()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        toolbar = QToolBar("工具栏")
        toolbar.setMovable(False)
        self.addToolBar(toolbar)
        toolbar.addAction("连接配置").triggered.connect(self._open_config_dialog)
        toolbar.addAction("历史记录").triggered.connect(self._open_history_dialog)

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(8, 8, 8, 4)
        main_layout.setSpacing(6)

        # ── Top: dual tree panels (horizontal splitter) ───────────────────
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # ── Left: source panel ───────────────────────────────────────────
        source_panel = QFrame()
        source_panel.setFrameShape(QFrame.Shape.StyledPanel)
        src_layout = QVBoxLayout(source_panel)
        src_layout.setContentsMargins(4, 4, 4, 4)
        src_layout.setSpacing(4)

        _bold = QFont()
        _bold.setBold(True)

        src_header = QHBoxLayout()
        src_title = QLabel("源端")
        src_title.setFont(_bold)
        src_header.addWidget(src_title)
        self.source_combo = QComboBox()
        self.source_combo.setMinimumWidth(160)
        self.source_combo.currentIndexChanged.connect(self._on_source_combo_changed)
        src_header.addWidget(self.source_combo, 1)
        self.source_db_label = QLabel("")
        self.source_db_label.setStyleSheet("color: #6c757d; font-size: 11px;")
        src_header.addWidget(self.source_db_label)
        src_layout.addLayout(src_header)

        src_btn_row = QHBoxLayout()
        src_select_all_btn = QPushButton("全选")
        src_select_all_btn.setFixedWidth(52)
        src_select_all_btn.clicked.connect(self._source_select_all)
        src_btn_row.addWidget(src_select_all_btn)
        src_deselect_btn = QPushButton("全不选")
        src_deselect_btn.setFixedWidth(58)
        src_deselect_btn.clicked.connect(self._source_deselect_all)
        src_btn_row.addWidget(src_deselect_btn)
        src_btn_row.addStretch()
        src_refresh_btn = QPushButton("↺ 刷新")
        src_refresh_btn.setFixedWidth(64)
        src_refresh_btn.clicked.connect(self._refresh_source)
        src_btn_row.addWidget(src_refresh_btn)
        src_layout.addLayout(src_btn_row)

        self._source_tree = QTreeWidget()
        self._source_tree.setColumnCount(2)
        self._source_tree.setHeaderLabels(["对象", "类型"])
        self._source_tree.setAlternatingRowColors(True)
        self._source_tree.setUniformRowHeights(True)
        self._source_tree.setColumnWidth(0, 220)
        self._source_tree.itemChanged.connect(self._on_src_tree_item_changed)
        src_layout.addWidget(self._source_tree)
        splitter.addWidget(source_panel)

        # ── Right: target panel ──────────────────────────────────────────
        target_panel = QFrame()
        target_panel.setFrameShape(QFrame.Shape.StyledPanel)
        tgt_layout = QVBoxLayout(target_panel)
        tgt_layout.setContentsMargins(4, 4, 4, 4)
        tgt_layout.setSpacing(4)

        tgt_header = QHBoxLayout()
        tgt_title = QLabel("目标端")
        tgt_title.setFont(_bold)
        tgt_header.addWidget(tgt_title)
        self.target_combo = QComboBox()
        self.target_combo.setMinimumWidth(160)
        self.target_combo.currentIndexChanged.connect(self._on_target_combo_changed)
        tgt_header.addWidget(self.target_combo, 1)
        self.target_db_label = QLabel("")
        self.target_db_label.setStyleSheet("color: #6c757d; font-size: 11px;")
        tgt_header.addWidget(self.target_db_label)
        tgt_layout.addLayout(tgt_header)

        tgt_btn_row = QHBoxLayout()
        tgt_btn_row.addStretch()
        tgt_refresh_btn = QPushButton("↺ 刷新")
        tgt_refresh_btn.setFixedWidth(64)
        tgt_refresh_btn.clicked.connect(self._refresh_target)
        tgt_btn_row.addWidget(tgt_refresh_btn)
        tgt_layout.addLayout(tgt_btn_row)

        self._target_tree = QTreeWidget()
        self._target_tree.setColumnCount(2)
        self._target_tree.setHeaderLabels(["对象", "类型"])
        self._target_tree.setAlternatingRowColors(True)
        self._target_tree.setUniformRowHeights(True)
        self._target_tree.setColumnWidth(0, 220)
        tgt_layout.addWidget(self._target_tree)
        splitter.addWidget(target_panel)

        main_layout.addWidget(splitter, stretch=2)

        # ── Compare strip ─────────────────────────────────────────────────
        compare_strip = QHBoxLayout()
        compare_strip.addStretch()
        self._compare_btn = QPushButton("⇄  开始比对")
        self._compare_btn.setMinimumSize(130, 40)
        self._compare_btn.setStyleSheet(
            "QPushButton { font-size: 13px; font-weight: bold; "
            "background-color: #0d6efd; color: white; "
            "border-radius: 6px; padding: 6px 24px; } "
            "QPushButton:hover { background-color: #0b5ed7; } "
            "QPushButton:pressed { background-color: #0a58ca; }"
        )
        self._compare_btn.clicked.connect(self._run_comparison)
        compare_strip.addWidget(self._compare_btn)
        compare_strip.addStretch()
        main_layout.addLayout(compare_strip)

        # ── Info label (hidden until compare runs) ────────────────────────
        self.info_label = QLabel("")
        self.info_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.info_label.setStyleSheet(
            "QLabel { padding: 6px; color: #495057; background-color: #e9f3ff;"
            " border-bottom: 1px solid #bee5eb; font-size: 12px; }"
        )
        self.info_label.hide()
        main_layout.addWidget(self.info_label)

        # ── Comparison results (hidden until compare runs) ────────────────
        self.comparison_panel = ComparisonPanel()
        self.comparison_panel.hide()
        main_layout.addWidget(self.comparison_panel, stretch=1)

        # ── Action bar (hidden until compare runs) ────────────────────────
        self._action_widget = QWidget()
        self._action_widget.setStyleSheet(
            "QWidget { background-color: #f8f9fa; border-top: 1px solid #ced4da; }"
        )
        action_bar = QHBoxLayout(self._action_widget)
        action_bar.setContentsMargins(10, 6, 10, 6)
        action_bar.setSpacing(8)

        self.selected_label = QLabel("已选择: 0")
        self.comparison_panel.selection_changed.connect(self._update_selected_label)
        action_bar.addWidget(self.selected_label)
        action_bar.addSpacing(8)

        _btn_style = (
            "QPushButton { background-color: #0d6efd; color: white;"
            " border-radius: 6px; padding: 4px 16px; } "
            "QPushButton:hover { background-color: #0b5ed7; } "
            "QPushButton:pressed { background-color: #0a58ca; } "
            "QPushButton:disabled { background-color: #6ea8fe; }"
        )

        self._gen_sql_btn = QPushButton("生成 SQL")
        self._gen_sql_btn.setStyleSheet(_btn_style)
        self._gen_sql_btn.clicked.connect(self._open_sql_preview)
        action_bar.addWidget(self._gen_sql_btn)

        dry_run_btn = QPushButton("Dry Run")
        dry_run_btn.setStyleSheet(_btn_style)
        dry_run_btn.clicked.connect(self._handle_direct_dry_run)
        action_bar.addWidget(dry_run_btn)

        clear_btn = QPushButton("清空选择")
        clear_btn.setStyleSheet(_btn_style)
        clear_btn.clicked.connect(self._clear_selection)
        action_bar.addWidget(clear_btn)

        self._execute_btn = QPushButton("执行同步")
        self._execute_btn.setStyleSheet(_btn_style)
        self._execute_btn.clicked.connect(self._handle_direct_execute)
        action_bar.addWidget(self._execute_btn)

        action_bar.addStretch()

        export_btn = QPushButton("导出报告")
        export_btn.setStyleSheet(_btn_style)
        export_btn.clicked.connect(self._export_report)
        action_bar.addWidget(export_btn)

        self._action_widget.hide()
        main_layout.addWidget(self._action_widget)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(True)
        self.progress_bar.hide()
        main_layout.addWidget(self.progress_bar)

        self.setStatusBar(QStatusBar())

    # ------------------------------------------------------------------
    # Profile combo management
    # ------------------------------------------------------------------

    def _refresh_profile_combos(self) -> None:
        if self.app_store is None:
            return

        self.source_combo.blockSignals(True)
        self.target_combo.blockSignals(True)

        prev_source_id = self.source_combo.currentData()
        prev_target_id = self.target_combo.currentData()

        self.source_combo.clear()
        self.target_combo.clear()

        source_profiles = self.app_store.list_profiles(ConnectionRole.SOURCE)
        for p in source_profiles:
            self.source_combo.addItem(p.name, p.id)

        target_profiles = self.app_store.list_profiles(ConnectionRole.TARGET)
        for p in target_profiles:
            self.target_combo.addItem(f"{p.name} ({p.db_type.value})", p.id)

        # Restore previous selection or pick default
        self._restore_combo(self.source_combo, prev_source_id, ConnectionRole.SOURCE)
        self._restore_combo(self.target_combo, prev_target_id, ConnectionRole.TARGET)

        self.source_combo.blockSignals(False)
        self.target_combo.blockSignals(False)

        self._on_source_combo_changed()
        self._on_target_combo_changed()

    def _restore_combo(self, combo: QComboBox, prev_id, role: ConnectionRole) -> None:
        if prev_id is not None:
            idx = combo.findData(prev_id)
            if idx >= 0:
                combo.setCurrentIndex(idx)
                return

        if self.app_store is None:
            return
        default = self.app_store.get_default_profile(role)
        if default is not None:
            idx = combo.findData(default.id)
            if idx >= 0:
                combo.setCurrentIndex(idx)

    def _on_source_combo_changed(self) -> None:
        profile_id = self.source_combo.currentData()
        if self.app_store is not None and profile_id is not None:
            self.current_source_profile = self.app_store.get_profile(profile_id)
            p = self.current_source_profile
            self.source_db_label.setText(f"{p.db_type.value} · {p.host}:{p.port}" if p else "")
            self._source_snapshot = None
            self._source_tree.clear()
            self._load_source_metadata()
        else:
            self.current_source_profile = None
            self.source_db_label.setText("")
            self._source_tree.clear()
            self._source_snapshot = None

    def _on_target_combo_changed(self) -> None:
        profile_id = self.target_combo.currentData()
        if self.app_store is not None and profile_id is not None:
            self.current_target_profile = self.app_store.get_profile(profile_id)
            p = self.current_target_profile
            self.target_db_label.setText(f"{p.db_type.value} · {p.host}:{p.port}" if p else "")
            self._target_snapshot = None
            self._target_tree.clear()
            self._load_target_metadata()
        else:
            self.current_target_profile = None
            self.target_db_label.setText("")
            self._target_tree.clear()
            self._target_snapshot = None

    # ------------------------------------------------------------------
    # Config dialog
    # ------------------------------------------------------------------

    def _open_config_dialog(self) -> None:
        dialog = ProfileManagerDialog(self.app_store, parent=self)
        dialog.exec()
        self._refresh_profile_combos()

    def _open_history_dialog(self) -> None:
        if self.app_store is None:
            return
        dialog = HistoryDialog(self.app_store, parent=self)
        dialog.regenerate_callback = self._regenerate_from_history
        dialog.exec()

    def _regenerate_from_history(self, source_profile_id, target_profile_id) -> None:
        """Triggered by HistoryDialog to re-run a comparison from history."""
        if self.app_store is None:
            return
        # Select the profiles in combos
        if source_profile_id is not None:
            idx = self.source_combo.findData(int(source_profile_id))
            if idx >= 0:
                self.source_combo.setCurrentIndex(idx)
        if target_profile_id is not None:
            idx = self.target_combo.findData(int(target_profile_id))
            if idx >= 0:
                self.target_combo.setCurrentIndex(idx)
        # Run comparison
        self._run_comparison()

    # ------------------------------------------------------------------
    # Metadata loading
    # ------------------------------------------------------------------

    def _load_metadata(self) -> None:
        """Refresh both sides — reset cached snapshots first."""
        self._source_snapshot = None
        self._target_snapshot = None
        self._source_tree.clear()
        self._target_tree.clear()
        self._load_source_metadata()
        self._load_target_metadata()

    def _build_metadata_filters(
        self,
        profile: Optional[ConnectionProfile],
        *,
        filter_owner_prefix: bool,
    ) -> "MetadataFilters":
        """根据连接配置构建 MetadataFilters。"""
        from db_schema_sync_client.infrastructure.db_metadata import MetadataFilters

        prefix_text = profile.schema_prefix if profile else ""
        base = MetadataFilters.from_prefix_text(prefix_text)

        schema_filter = profile.schema_names_filter if profile else ""
        if schema_filter:
            exclude = tuple(
                n.strip()
                for n in schema_filter.replace("/", ",").split(",")
                if n.strip()
            )
            return MetadataFilters(
                schema_prefixes=base.schema_prefixes,
                owner_prefixes=base.owner_prefixes,
                exclude_schema_names=exclude,
                filter_owner_prefix=filter_owner_prefix,
            )
        return MetadataFilters(
            schema_prefixes=base.schema_prefixes,
            owner_prefixes=base.owner_prefixes,
            filter_owner_prefix=filter_owner_prefix,
        )

    def _build_source_metadata_filters(self) -> "MetadataFilters":
        return self._build_metadata_filters(self.current_source_profile, filter_owner_prefix=True)

    def _build_target_metadata_filters(self) -> "MetadataFilters":
        return self._build_metadata_filters(self.current_target_profile, filter_owner_prefix=False)

    def _get_password_for_profile(self, profile: ConnectionProfile) -> Optional[str]:
        """Return the stored password; if missing, prompt user to re-enter and persist it."""
        if self.app_store is None:
            return None
        try:
            return self.app_store.get_profile_password(profile)
        except (KeyError, Exception):
            pass

        # Password not found — ask user (only happens for profiles migrated from old keyring store)
        password, ok = QInputDialog.getText(
            self,
            "请输入密码",
            f'连接 "{profile.name}" 的密码未找到，请重新输入：',
            QLineEdit.EchoMode.Password,
        )
        if not ok or not password:
            return None

        # Persist so we don't ask again
        try:
            self.app_store.save_profile(profile, password)
        except Exception:
            pass
        return password

    def _load_source_metadata(self) -> None:
        """Load source schema snapshot in a background thread."""
        profile = self.current_source_profile
        if profile is None:
            self.statusBar().showMessage("请先选择源端连接", 3000)
            return
        password = self._get_password_for_profile(profile)
        if password is None:
            return
        self._source_snapshot = None
        self._show_progress()
        self.statusBar().showMessage("正在加载源端结构…")

        filters = self._build_source_metadata_filters()
        worker = MetadataWorker(self.metadata_reader, profile, password, filters=filters)

        def _on_done(snapshot: SchemaSnapshot) -> None:
            self._source_snapshot = snapshot
            self._populate_source_tree(snapshot)
            self._worker_done(worker)
            schemas = sorted({t.schema for t in snapshot.tables})
            self.statusBar().showMessage(
                f"源端结构加载完成（{len(schemas)} 个 Schema，{len(snapshot.tables)} 个对象）", 4000
            )

        def _on_error(msg: str) -> None:
            self._worker_done(worker)
            QMessageBox.warning(self, "加载失败", f"源端加载失败: {msg}")

        worker.finished.connect(_on_done)
        worker.error.connect(_on_error)
        worker.progress.connect(self._update_progress)
        self._workers.append(worker)
        worker.start()

    def _load_target_metadata(self) -> None:
        """Load target schema snapshot in a background thread."""
        profile = self.current_target_profile
        if profile is None:
            self.statusBar().showMessage("请先选择目标端连接", 3000)
            return
        password = self._get_password_for_profile(profile)
        if password is None:
            return
        self._target_snapshot = None
        self._show_progress()
        self.statusBar().showMessage("正在加载目标端结构…")

        filters = self._build_target_metadata_filters()
        worker = MetadataWorker(self.metadata_reader, profile, password, filters=filters)

        def _on_done(snapshot: SchemaSnapshot) -> None:
            self._target_snapshot = snapshot
            self._populate_target_tree(snapshot)
            self._worker_done(worker)
            schemas = sorted({t.schema for t in snapshot.tables})
            self.statusBar().showMessage(
                f"目标端结构加载完成（{len(schemas)} 个 Schema，{len(snapshot.tables)} 个对象）", 4000
            )

        def _on_error(msg: str) -> None:
            self._worker_done(worker)
            QMessageBox.warning(self, "加载失败", f"目标端加载失败: {msg}")

        worker.finished.connect(_on_done)
        worker.error.connect(_on_error)
        worker.progress.connect(self._update_progress)
        self._workers.append(worker)
        worker.start()

    def _refresh_target_after_sync_and_recompare(self) -> None:
        """Invalidate target snapshot, reload it, then rerun comparison."""
        target = self.current_target_profile
        if target is None:
            return

        password = self._get_password_for_profile(target)
        if password is None:
            return

        self._target_snapshot = None
        self._target_tree.clear()
        self._compare_btn.setEnabled(False)
        self._show_progress()
        self.statusBar().showMessage("同步完成，正在刷新目标端结构并重新比对…")

        filters = self._build_target_metadata_filters()
        worker = MetadataWorker(self.metadata_reader, target, password, filters=filters)

        def _on_done(snapshot: SchemaSnapshot) -> None:
            self._target_snapshot = snapshot
            self._populate_target_tree(snapshot, checked_schemas=self._get_source_selected_schemas())
            self._worker_done(worker)
            self._run_comparison()

        def _on_error(msg: str) -> None:
            self._worker_done(worker)
            self._compare_btn.setEnabled(True)
            QMessageBox.warning(self, "刷新失败", f"同步后刷新目标端失败: {msg}")

        worker.finished.connect(_on_done)
        worker.error.connect(_on_error)
        worker.progress.connect(self._update_progress)
        self._workers.append(worker)
        worker.start()

    # ------------------------------------------------------------------
    # Comparison
    # ------------------------------------------------------------------

    def _run_comparison(self) -> None:
        source = self.current_source_profile
        target = self.current_target_profile
        if source is None or target is None:
            QMessageBox.warning(self, "比对", "请先选择源端和目标端连接")
            return
        if self.app_store is None:
            return

        if self._profiles_are_same_instance(source, target):
            QMessageBox.warning(
                self, "比对",
                "源端和目标端连接配置完全相同，不允许对同一实例执行比对同步。"
            )
            return

        source_pw = self._get_password_for_profile(source)
        if source_pw is None:
            return
        target_pw = self._get_password_for_profile(target)
        if target_pw is None:
            return

        self._compare_btn.setEnabled(False)
        self._show_progress()

        # ── helpers ────────────────────────────────────────────────────
        def _abort(worker, msg: str) -> None:
            self._worker_done(worker)
            self._compare_btn.setEnabled(True)
            QMessageBox.warning(self, "操作失败", msg)

        def _do_compare() -> None:
            sel = self._get_source_selected_schemas()
            if not sel:
                sel = {t.schema for t in self._source_snapshot.tables}
            f_src = SchemaSnapshot(
                database_name=self._source_snapshot.database_name,
                tables=tuple(t for t in self._source_snapshot.tables if t.schema in sel),
                role_hashes=self._source_snapshot.role_hashes,
            )
            f_tgt = SchemaSnapshot(
                database_name=self._target_snapshot.database_name,
                tables=tuple(t for t in self._target_snapshot.tables if t.schema in sel),
            )
            n_schemas = len(sel)
            n_objs = len(f_src.tables)
            cw = CompareWorker(f_src, f_tgt)

            def _on_cmp_done(diff: SchemaDiff) -> None:
                self._worker_done(cw)
                self.current_diff = diff
                self.comparison_panel.set_diff(diff)
                total = len(diff.object_diffs) + len(diff.column_diffs)
                self.info_label.setText(
                    f"比对完成：{n_schemas} 个 Schema · {n_objs} 个对象 · 发现 {total} 处差异"
                )
                self.info_label.setStyleSheet(
                    "QLabel { padding: 6px; color: #155724; background-color: #d4edda;"
                    " border-bottom: 1px solid #c3e6cb; font-size: 12px; }"
                )
                self.info_label.show()
                self.comparison_panel.show()
                self._action_widget.show()
                self._compare_btn.setEnabled(True)
                self._persist_compare_task(source, target, diff)
                self.statusBar().showMessage(
                    f"比对完成（{n_schemas} 个 Schema，共 {total} 处差异）", 5000
                )
                # 目标端树镜像源端勾选状态
                self._populate_target_tree(self._target_snapshot, checked_schemas=sel)

            cw.finished.connect(_on_cmp_done)
            cw.error.connect(lambda msg: _abort(cw, msg))
            cw.progress.connect(self._update_progress)
            self._workers.append(cw)
            self.statusBar().showMessage("正在比对结构…")
            cw.start()

        def _load_target_then_compare() -> None:
            if self._target_snapshot is not None:
                _do_compare()
                return
            tw = MetadataWorker(self.metadata_reader, target, target_pw, filters=self._build_target_metadata_filters())

            def _on_tgt(snap: SchemaSnapshot) -> None:
                self._target_snapshot = snap
                self._populate_target_tree(snap)
                self._worker_done(tw)
                _do_compare()

            tw.finished.connect(_on_tgt)
            tw.error.connect(lambda msg: _abort(tw, f"目标端加载失败: {msg}"))
            tw.progress.connect(self._update_progress)
            self._workers.append(tw)
            self.statusBar().showMessage("正在加载目标端结构…")
            tw.start()

        # ── kick off ───────────────────────────────────────────────────
        if self._source_snapshot is None:
            sw = MetadataWorker(self.metadata_reader, source, source_pw, filters=self._build_source_metadata_filters())

            def _on_src(snap: SchemaSnapshot) -> None:
                self._source_snapshot = snap
                self._populate_source_tree(snap)
                self._worker_done(sw)
                _load_target_then_compare()

            sw.finished.connect(_on_src)
            sw.error.connect(lambda msg: _abort(sw, f"源端加载失败: {msg}"))
            sw.progress.connect(self._update_progress)
            self._workers.append(sw)
            self.statusBar().showMessage("正在加载源端结构…")
            sw.start()
        else:
            _load_target_then_compare()

    # ------------------------------------------------------------------
    # SQL generation
    # ------------------------------------------------------------------

    def generate_sql_plan_for_selected(self) -> GeneratedSqlPlan:
        """Generate a SQL plan from the currently selected diffs."""
        selected_columns = self.comparison_panel.selected_auto_syncable_diffs()
        selected_schemas = self.comparison_panel.selected_schema_syncable_diffs()
        selected_tables = self.comparison_panel.selected_table_syncable_diffs()
        selected_view_rebuild_diffs = self.comparison_panel.selected_view_rebuild_diffs()

        target_type = (
            self.current_target_profile.db_type
            if self.current_target_profile
            else None
        )
        if target_type is None:
            return GeneratedSqlPlan(target_type=target_type, statements=[], auto_syncable_count=0)

        column_items = [
            (diff.schema, diff.object_name, diff.source_column)
            for diff in selected_columns
            if diff.source_column is not None
        ]

        rebuild_view_pairs = sorted({(d.schema, d.object_name) for d in selected_view_rebuild_diffs})
        rebuild_views = []
        if self._source_snapshot is not None and rebuild_view_pairs:
            source_map = {
                (t.schema, t.name): t
                for t in self._source_snapshot.tables
                if t.object_type == ObjectType.VIEW
            }
            rebuild_views = [source_map[pair] for pair in rebuild_view_pairs if pair in source_map]

        manual_count = 0
        hint_count = 0
        if self.current_diff:
            for cd in self.current_diff.column_diffs:
                if cd.category == DiffCategory.MANUAL_REQUIRED:
                    manual_count += 1
                elif cd.category == DiffCategory.ONLY_HINT:
                    hint_count += 1
            for od in self.current_diff.object_diffs:
                if od.category == DiffCategory.MANUAL_REQUIRED:
                    manual_count += 1
                elif od.category == DiffCategory.ONLY_HINT:
                    hint_count += 1

        role_hashes = self._source_snapshot.role_hashes if self._source_snapshot else {}
        owner_fix_schemas: list[str] = []
        if self._target_snapshot is not None:
            for schema_name in sorted({od.schema for od in selected_tables}):
                owner = self._target_snapshot.schema_owners.get(schema_name)
                if owner and owner != schema_name:
                    owner_fix_schemas.append(schema_name)
        risk_levels = {"low": 0, "medium": 1, "high": 2}

        plans = []
        if selected_schemas:
            plans.append(self.sql_generator.generate_schema_creates(
                schema_object_diffs=selected_schemas,
                target_type=target_type,
                role_hashes=role_hashes,
            ))
        if selected_tables:
            plans.append(self.sql_generator.generate_object_creates(
                object_diffs=selected_tables,
                target_type=target_type,
                existing_schema_owner_fixes=owner_fix_schemas,
                role_hashes=role_hashes,
            ))
        if rebuild_views:
            plans.append(self.sql_generator.generate_view_rebuilds(
                views=rebuild_views,
                target_type=target_type,
            ))
        if column_items:
            plans.append(self.sql_generator.generate_missing_columns(
                items=column_items,
                target_type=target_type,
            ))

        if not plans:
            return GeneratedSqlPlan(
                target_type=target_type, statements=[],
                auto_syncable_count=0,
                manual_required_count=manual_count,
                hint_only_count=hint_count,
            )

        all_stmts: list[str] = []
        all_warnings: list[str] = []
        for p in plans:
            all_stmts.extend(p.statements)
            all_warnings.extend(p.warnings)
        combined_risk = max(
            (p.risk_level for p in plans),
            key=lambda r: risk_levels.get(r, 0),
        )
        return GeneratedSqlPlan(
            target_type=target_type,
            statements=all_stmts,
            warnings=all_warnings,
            risk_level=combined_risk,
            auto_syncable_count=len(all_stmts),
            manual_required_count=manual_count,
            hint_only_count=hint_count,
        )

    def _open_sql_preview(self) -> None:
        if self.current_target_profile is None:
            QMessageBox.warning(self, "SQL 预览", "请先选择目标端连接")
            return

        plan = self.generate_sql_plan_for_selected()
        if not plan.statements:
            QMessageBox.information(self, "SQL 预览", self._no_syncable_message())
            return

        dialog = SqlPreviewDialog(
            self.current_target_profile,
            plan,
            report_service=self.report_service,
            parent=self,
        )
        if dialog.exec() == SqlPreviewDialog.DialogCode.Accepted:
            if dialog.selected_action == "execute":
                self._execute_sync(plan)
            elif dialog.selected_action == "dry_run":
                self._handle_dry_run(dialog, plan)

    def _handle_dry_run(self, dialog: SqlPreviewDialog, plan: GeneratedSqlPlan) -> None:
        from db_schema_sync_client.paths import development_data_dir

        output_dir = development_data_dir() / "reports"
        try:
            path = dialog.save_sql_and_report(output_dir)
            QMessageBox.information(self, "Dry Run", f"报告已保存: {path}")
        except Exception as exc:
            QMessageBox.warning(self, "保存失败", str(exc))

    # ------------------------------------------------------------------
    # Sync execution
    # ------------------------------------------------------------------

    def _execute_sync(self, plan: GeneratedSqlPlan) -> None:
        target = self.current_target_profile
        if target is None or self.app_store is None:
            return

        reply = QMessageBox.warning(
            self,
            "确认同步",
            f"即将对 {target.name} ({target.db_type.value}) 执行 {len(plan.statements)} 条 SQL。\n"
            f"风险等级: {plan.risk_level}\n\n"
            "确定执行？",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        try:
            password = self._get_password_for_profile(target)
            if password is None:
                return
        except Exception as exc:
            QMessageBox.warning(self, "执行失败", str(exc))
            return

        selected_diffs = self.comparison_panel.selected_auto_syncable_diffs()
        selected_fields = [
            {"schema": d.schema, "object": d.object_name, "column": d.column_name}
            for d in selected_diffs
        ]

        executor = SyncExecutor(self.app_store, self.connection_factory)
        worker = SyncWorker(executor, plan, target, password, selected_fields=selected_fields)

        self._execute_btn.setEnabled(False)
        self._show_progress()
        self.statusBar().showMessage(f"正在执行同步（共 {len(plan.statements)} 条 SQL）…")

        def _on_done(result) -> None:
            self._worker_done(worker)
            self._execute_btn.setEnabled(True)
            self.statusBar().showMessage(
                f"同步完成: 成功 {result.success_count}, 失败 {result.failure_count}", 5000
            )
            ExecutionResultDialog(result, parent=self).exec()
            self._refresh_target_after_sync_and_recompare()

        def _on_error(msg: str) -> None:
            self._worker_done(worker)
            self._execute_btn.setEnabled(True)
            QMessageBox.critical(self, "执行失败", msg)

        worker.finished.connect(_on_done)
        worker.error.connect(_on_error)
        worker.progress.connect(self._update_progress)
        self._workers.append(worker)
        worker.start()

    # ------------------------------------------------------------------
    # Report export
    # ------------------------------------------------------------------

    def _export_report(self) -> None:
        if self.current_diff is None:
            QMessageBox.information(self, "导出报告", "请先执行比对")
            return

        source = self.current_source_profile
        target = self.current_target_profile
        if source is None or target is None:
            return

        plan = self.generate_sql_plan_for_selected()
        report_text = self.report_service.render_compare_report(source, target, self.current_diff, plan)

        dir_path = QFileDialog.getExistingDirectory(self, "选择报告保存目录")
        if not dir_path:
            return

        try:
            path = self.report_service.save_report(report_text, Path(dir_path))
            QMessageBox.information(self, "导出报告", f"报告已保存: {path}")
        except Exception as exc:
            QMessageBox.warning(self, "保存失败", str(exc))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _update_selected_label(self) -> None:
        col_count = len(self.comparison_panel.selected_auto_syncable_diffs())
        schema_count = len(
            {od.schema for od in self.comparison_panel.selected_schema_syncable_diffs()}
        )
        parts = []
        if col_count:
            parts.append(f"{col_count} 个字段")
        if schema_count:
            parts.append(f"{schema_count} 个 Schema")
        self.selected_label.setText(f"已选择: {'、'.join(parts) if parts else '0'}")

    def _worker_done(self, worker) -> None:
        """Remove finished worker from tracking list; hide progress if none remain."""
        try:
            self._workers.remove(worker)
        except ValueError:
            pass
        if not self._workers:
            self._hide_progress()

    def _clear_selection(self) -> None:
        """Uncheck all auto-syncable column items in the tree."""
        tree = self.comparison_panel.tree
        tree.blockSignals(True)
        root = tree.invisibleRootItem()
        self._uncheck_tree(root)
        tree.blockSignals(False)
        self.comparison_panel.selection_changed.emit()

    def _uncheck_tree(self, parent) -> None:
        for i in range(parent.childCount()):
            item = parent.child(i)
            if item.flags() & Qt.ItemFlag.ItemIsUserCheckable:
                item.setCheckState(0, Qt.CheckState.Unchecked)
            self._uncheck_tree(item)

    def _handle_direct_dry_run(self) -> None:
        if self.current_target_profile is None:
            QMessageBox.warning(self, "Dry Run", "请先选择目标端连接")
            return
        plan = self.generate_sql_plan_for_selected()
        if not plan.statements:
            QMessageBox.information(self, "Dry Run", self._no_syncable_message())
            return
        from db_schema_sync_client.paths import development_data_dir

        output_dir = development_data_dir() / "reports"
        dialog = SqlPreviewDialog(
            self.current_target_profile, plan, report_service=self.report_service, parent=self,
        )
        try:
            path = dialog.save_sql_and_report(output_dir)
            QMessageBox.information(self, "Dry Run", f"报告已保存: {path}")
        except Exception as exc:
            QMessageBox.warning(self, "保存失败", str(exc))

    def _handle_direct_execute(self) -> None:
        if self.current_target_profile is None:
            QMessageBox.warning(self, "执行同步", "请先选择目标端连接")
            return
        plan = self.generate_sql_plan_for_selected()
        if not plan.statements:
            QMessageBox.information(self, "执行同步", self._no_syncable_message())
            return
        self._execute_sync(plan)

    def _no_syncable_message(self) -> str:
        if self.current_diff is None:
            return "没有可同步的项目"

        auto_count = sum(
            1 for d in self.current_diff.column_diffs if d.category == DiffCategory.AUTO_SYNCABLE
        )
        view_rebuild_count = sum(
            1 for d in self.current_diff.column_diffs if d.category == DiffCategory.VIEW_REBUILD_SYNCABLE
        )
        schema_sync_count = sum(
            1 for d in self.current_diff.object_diffs if d.category == DiffCategory.SCHEMA_SYNCABLE
        )
        table_sync_count = sum(
            1 for d in self.current_diff.object_diffs if d.category == DiffCategory.TABLE_SYNCABLE
        )
        manual_count = (
            sum(1 for d in self.current_diff.column_diffs if d.category == DiffCategory.MANUAL_REQUIRED)
            + sum(1 for d in self.current_diff.object_diffs if d.category == DiffCategory.MANUAL_REQUIRED)
        )
        hint_count = (
            sum(1 for d in self.current_diff.column_diffs if d.category == DiffCategory.ONLY_HINT)
            + sum(1 for d in self.current_diff.object_diffs if d.category == DiffCategory.ONLY_HINT)
        )

        if auto_count + schema_sync_count + table_sync_count + view_rebuild_count > 0:
            return (
                "当前有可同步项，但可能未勾选。\n"
                f"可自动同步字段: {auto_count}，可同步Schema: {schema_sync_count}，"
                f"可同步对象: {table_sync_count}，可重建视图: {view_rebuild_count}。"
            )
        return (
            "当前差异均为仅提示或需人工处理，暂无可自动生成 SQL 的项目。\n"
            f"需人工处理: {manual_count}，仅提示: {hint_count}。"
        )

    # ------------------------------------------------------------------
    # Source / target tree helpers
    # ------------------------------------------------------------------

    _CHECKABLE = Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled

    def _populate_source_tree(self, snapshot: SchemaSnapshot) -> None:
        """Populate source tree with checkable database/schema/object nodes."""
        from db_schema_sync_client.domain.models import ObjectType

        self._source_tree.blockSignals(True)
        self._source_tree.clear()

        db_item = QTreeWidgetItem([snapshot.database_name, "数据库"])
        db_item.setFlags(db_item.flags() | self._CHECKABLE)
        db_item.setCheckState(0, Qt.CheckState.Checked)
        self._source_tree.addTopLevelItem(db_item)

        schemas: dict[str, list] = defaultdict(list)
        for table in snapshot.tables:
            schemas[table.schema].append(table)

        for schema_name in sorted(schemas):
            schema_item = QTreeWidgetItem([schema_name, "Schema"])
            schema_item.setFlags(schema_item.flags() | self._CHECKABLE)
            schema_item.setCheckState(0, Qt.CheckState.Checked)
            db_item.addChild(schema_item)
            for table in sorted(schemas[schema_name], key=lambda t: t.name):
                obj_type = "Table" if table.object_type == ObjectType.TABLE else "View"
                table_item = QTreeWidgetItem([table.name, obj_type])
                table_item.setFlags(table_item.flags() | self._CHECKABLE)
                table_item.setCheckState(0, Qt.CheckState.Checked)
                schema_item.addChild(table_item)

        db_item.setExpanded(True)
        self._source_tree.blockSignals(False)

    # 目标端树节点 flags：可见复选框但不可操作（置灰）
    _TGT_FLAGS = Qt.ItemFlag.ItemIsUserCheckable

    def _populate_target_tree(self, snapshot: SchemaSnapshot, checked_schemas: set | None = None) -> None:
        """Populate target tree with grayed-out checkboxes mirroring source structure.

        checked_schemas: set of schema names that should appear checked.
                         Pass None to check all. Pass empty set to uncheck all.
        """
        from db_schema_sync_client.domain.models import ObjectType

        self._target_tree.clear()

        db_checked = (
            Qt.CheckState.Checked
            if (checked_schemas is None or len(checked_schemas) > 0)
            else Qt.CheckState.Unchecked
        )
        db_item = QTreeWidgetItem([snapshot.database_name, "数据库"])
        db_item.setFlags(self._TGT_FLAGS)
        db_item.setCheckState(0, db_checked)
        self._target_tree.addTopLevelItem(db_item)

        schemas: dict[str, list] = defaultdict(list)
        for table in snapshot.tables:
            schemas[table.schema].append(table)

        for schema_name in sorted(schemas):
            is_checked = checked_schemas is None or schema_name in checked_schemas
            state = Qt.CheckState.Checked if is_checked else Qt.CheckState.Unchecked
            schema_item = QTreeWidgetItem([schema_name, "Schema"])
            schema_item.setFlags(self._TGT_FLAGS)
            schema_item.setCheckState(0, state)
            db_item.addChild(schema_item)
            for table in sorted(schemas[schema_name], key=lambda t: t.name):
                obj_type = "Table" if table.object_type == ObjectType.TABLE else "View"
                table_item = QTreeWidgetItem([table.name, obj_type])
                table_item.setFlags(self._TGT_FLAGS)
                table_item.setCheckState(0, state)
                schema_item.addChild(table_item)

        db_item.setExpanded(True)

    def _get_source_selected_schemas(self) -> set[str]:
        """Return schema names that are at least partially checked in the source tree."""
        result = set()
        root = self._source_tree.invisibleRootItem()
        for i in range(root.childCount()):          # database level
            db_node = root.child(i)
            for j in range(db_node.childCount()):   # schema level
                schema_node = db_node.child(j)
                if schema_node.checkState(0) != Qt.CheckState.Unchecked:
                    result.add(schema_node.text(0))
        return result

    def _on_src_tree_item_changed(self, item: QTreeWidgetItem, column: int) -> None:
        """Cascade check state to children when a node is toggled."""
        if column != 0:
            return
        self._source_tree.blockSignals(True)
        self._cascade_check_state(item, item.checkState(0))
        self._source_tree.blockSignals(False)
        self._sync_target_tree_to_selection()

    def _cascade_check_state(self, item: QTreeWidgetItem, state: Qt.CheckState) -> None:
        for i in range(item.childCount()):
            child = item.child(i)
            if child.flags() & Qt.ItemFlag.ItemIsUserCheckable:
                child.setCheckState(0, state)
                self._cascade_check_state(child, state)

    def _source_select_all(self) -> None:
        """Check all items in the source tree."""
        root = self._source_tree.invisibleRootItem()
        self._source_tree.blockSignals(True)
        for i in range(root.childCount()):
            db_node = root.child(i)
            if db_node.flags() & Qt.ItemFlag.ItemIsUserCheckable:
                db_node.setCheckState(0, Qt.CheckState.Checked)
            self._cascade_check_state(db_node, Qt.CheckState.Checked)
        self._source_tree.blockSignals(False)
        self._sync_target_tree_to_selection()

    def _source_deselect_all(self) -> None:
        """Uncheck all items in the source tree."""
        root = self._source_tree.invisibleRootItem()
        self._source_tree.blockSignals(True)
        for i in range(root.childCount()):
            db_node = root.child(i)
            if db_node.flags() & Qt.ItemFlag.ItemIsUserCheckable:
                db_node.setCheckState(0, Qt.CheckState.Unchecked)
            self._cascade_check_state(db_node, Qt.CheckState.Unchecked)
        self._source_tree.blockSignals(False)
        self._sync_target_tree_to_selection()

    def _sync_target_tree_to_selection(self) -> None:
        """Mirror source check states to target tree (same list, same check states)."""
        if self._target_snapshot is None:
            return
        sel = self._get_source_selected_schemas()
        self._populate_target_tree(self._target_snapshot, checked_schemas=sel)

    def _refresh_source(self) -> None:
        """Force-reload source metadata."""
        self._source_snapshot = None
        self._source_tree.clear()
        self._load_source_metadata()

    def _refresh_target(self) -> None:
        """Force-reload target metadata."""
        self._target_snapshot = None
        self._target_tree.clear()
        self._load_target_metadata()

    @staticmethod
    def _profiles_are_same_instance(a: ConnectionProfile, b: ConnectionProfile) -> bool:
        return (
            a.host == b.host
            and a.port == b.port
            and a.database == b.database
            and a.username == b.username
        )

    # ------------------------------------------------------------------
    # Progress helpers
    # ------------------------------------------------------------------

    def _show_progress(self, current: int = 0, total: int = 0) -> None:
        if total <= 0:
            self.progress_bar.setRange(0, 0)  # indeterminate
        else:
            self.progress_bar.setRange(0, total)
            self.progress_bar.setValue(current)
        self.progress_bar.show()

    def _update_progress(self, current: int, total: int) -> None:
        if total <= 0:
            self.progress_bar.setRange(0, 0)
        else:
            self.progress_bar.setRange(0, total)
            self.progress_bar.setValue(current)

    def _hide_progress(self) -> None:
        self.progress_bar.hide()
        self.progress_bar.reset()

    def _persist_compare_task(
        self,
        source: ConnectionProfile,
        target: ConnectionProfile,
        diff: SchemaDiff,
    ) -> None:
        if self.app_store is None:
            return
        result_data = {
            "object_diffs": [
                {"schema": od.schema, "object": od.object_name, "status": od.status.value, "category": od.category.value, "reason": od.reason}
                for od in diff.object_diffs
            ],
            "column_diffs": [
                {"schema": cd.schema, "object": cd.object_name, "column": cd.column_name, "status": cd.status.value, "category": cd.category.value, "reason": cd.reason}
                for cd in diff.column_diffs
            ],
        }
        self.app_store.create_compare_task(
            source_profile_id=source.id,
            target_profile_id=target.id,
            status="completed",
            result_json=json.dumps(result_data, ensure_ascii=False),
        )
