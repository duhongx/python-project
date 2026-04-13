"""Main application window."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QStatusBar,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from db_schema_sync_client.domain.diff import SchemaDiff
from db_schema_sync_client.domain.models import ConnectionProfile, ConnectionRole, SchemaSnapshot
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


class _SchemaSelector(QWidget):
    """Checkable schema multi-select combo for the header panel."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        lay = QHBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        self._combo = QComboBox()
        self._combo.setMinimumWidth(180)
        self._combo.addItem("—")
        self._combo.setEnabled(False)
        lay.addWidget(self._combo)
        self._schemas: list[str] = []
        self._checked: set[str] = set()

    def set_schemas(self, schemas: list[str]) -> None:
        self._schemas = sorted(schemas)
        self._checked = set(self._schemas)
        self._rebuild()

    def selected_schemas(self) -> set[str]:
        return set(self._checked)

    def clear_schemas(self) -> None:
        model = self._combo.model()
        try:
            model.itemChanged.disconnect(self._on_changed)
        except Exception:
            pass
        self._schemas = []
        self._checked = set()
        self._combo.blockSignals(True)
        self._combo.clear()
        self._combo.addItem("—")
        self._combo.setEnabled(False)
        self._combo.blockSignals(False)

    def _rebuild(self) -> None:
        model = self._combo.model()
        try:
            model.itemChanged.disconnect(self._on_changed)
        except Exception:
            pass
        self._combo.blockSignals(True)
        self._combo.clear()
        n = len(self._schemas)
        self._combo.addItem(f"全部 Schema ({n})")
        for schema in self._schemas:
            self._combo.addItem(schema)
        model = self._combo.model()
        for i in range(1, n + 1):
            item = model.item(i)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(Qt.CheckState.Checked)
        self._combo.setEnabled(True)
        self._combo.blockSignals(False)
        model.itemChanged.connect(self._on_changed)

    def _on_changed(self, item) -> None:
        text = item.text()
        if text in self._schemas:
            if item.checkState() == Qt.CheckState.Checked:
                self._checked.add(text)
            else:
                self._checked.discard(text)
            nc, n = len(self._checked), len(self._schemas)
            label = f"全部 Schema ({n})" if nc == n else f"已选 {nc}/{n} Schema"
            self._combo.blockSignals(True)
            self._combo.setItemText(0, label)
            self._combo.blockSignals(False)


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
        toolbar.addAction("刷新结构").triggered.connect(self._load_metadata)
        toolbar.addAction("历史记录").triggered.connect(self._open_history_dialog)

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # ── PgAdmin4-style selector panel ─────────────────────────────────
        sel_frame = QFrame()
        sel_frame.setStyleSheet(
            "QFrame { background-color: #f8f9fa; border-bottom: 1px solid #ced4da; }"
        )
        grid = QGridLayout(sel_frame)
        grid.setContentsMargins(14, 10, 14, 10)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(8)

        # Source row
        src_lbl = QLabel("Select Source")
        f = QFont()
        f.setBold(True)
        src_lbl.setFont(f)
        src_lbl.setMinimumWidth(100)
        grid.addWidget(src_lbl, 0, 0)

        self.source_combo = QComboBox()
        self.source_combo.setMinimumWidth(210)
        self.source_combo.currentIndexChanged.connect(self._on_source_combo_changed)
        grid.addWidget(self.source_combo, 0, 1)

        self.source_db_label = QLabel("")
        self.source_db_label.setStyleSheet("color: #6c757d;")
        grid.addWidget(self.source_db_label, 0, 2)

        self.source_schema_selector = _SchemaSelector()
        grid.addWidget(self.source_schema_selector, 0, 3)

        # Target row
        tgt_lbl = QLabel("Select Target")
        tgt_lbl.setFont(f)
        tgt_lbl.setMinimumWidth(100)
        grid.addWidget(tgt_lbl, 1, 0)

        self.target_combo = QComboBox()
        self.target_combo.setMinimumWidth(210)
        self.target_combo.currentIndexChanged.connect(self._on_target_combo_changed)
        grid.addWidget(self.target_combo, 1, 1)

        self.target_type_label = QLabel("")
        self.target_type_label.setStyleSheet("color: #6c757d;")
        grid.addWidget(self.target_type_label, 1, 2)

        self.target_schema_selector = _SchemaSelector()
        grid.addWidget(self.target_schema_selector, 1, 3)

        # Compare button (spans 2 rows)
        self._compare_btn = QPushButton("\u21c4  \u5f00\u59cb\u6bd4\u5bf9")
        self._compare_btn.setMinimumSize(130, 60)
        self._compare_btn.setStyleSheet(
            "QPushButton { font-size: 13px; font-weight: bold; "
            "background-color: #0d6efd; color: white; "
            "border-radius: 6px; padding: 8px 16px; } "
            "QPushButton:hover { background-color: #0b5ed7; } "
            "QPushButton:pressed { background-color: #0a58ca; }"
        )
        self._compare_btn.clicked.connect(self._run_comparison)
        grid.addWidget(self._compare_btn, 0, 4, 2, 1)

        gen_btn = QPushButton("生成 SQL")
        gen_btn.clicked.connect(self._open_sql_preview)
        grid.addWidget(gen_btn, 0, 5)

        export_btn = QPushButton("导出报告")
        export_btn.clicked.connect(self._export_report)
        grid.addWidget(export_btn, 1, 5)

        # ── 用户/Schema 前缀过滤行 ────────────────────────────────────────
        prefix_lbl = QLabel("用户/Schema 前缀:")
        prefix_lbl.setMinimumWidth(100)
        grid.addWidget(prefix_lbl, 2, 0)

        self._prefix_input = QLineEdit()
        self._prefix_input.setPlaceholderText("df_  （多个前缀用英文逗号分隔，如 df_,jk_；留空表示不限制）")
        self._prefix_input.setMinimumWidth(350)
        self._prefix_input.returnPressed.connect(self._apply_prefix_filter)
        grid.addWidget(self._prefix_input, 2, 1, 1, 3)

        apply_prefix_btn = QPushButton("应用并刷新")
        apply_prefix_btn.setToolTip("重新按当前前缀加载两侧元数据")
        apply_prefix_btn.clicked.connect(self._apply_prefix_filter)
        grid.addWidget(apply_prefix_btn, 2, 4)

        grid.setColumnStretch(3, 1)
        main_layout.addWidget(sel_frame)

        # ── Info / result summary label ───────────────────────────────────
        self.info_label = QLabel(
            "Database Compare：选择源端和目标端连接，选择 Schema 范围，然后点击  ⇄ 开始比对"
        )
        self.info_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.info_label.setStyleSheet(
            "QLabel { padding: 6px; color: #495057; background-color: #e9f3ff;"
            " border-bottom: 1px solid #bee5eb; font-size: 12px; }"
        )
        main_layout.addWidget(self.info_label)

        # ── Comparison results (full width) ───────────────────────────────
        self.comparison_panel = ComparisonPanel()
        main_layout.addWidget(self.comparison_panel, stretch=1)

        # ── Action bar ────────────────────────────────────────────────────
        action_widget = QWidget()
        action_widget.setStyleSheet(
            "QWidget { background-color: #f8f9fa; border-top: 1px solid #ced4da; }"
        )
        action_bar = QHBoxLayout(action_widget)
        action_bar.setContentsMargins(10, 6, 10, 6)
        action_bar.setSpacing(8)

        self.selected_label = QLabel("已选择: 0")
        self.comparison_panel.selection_changed.connect(self._update_selected_label)
        action_bar.addWidget(self.selected_label)
        action_bar.addSpacing(8)

        dry_run_btn = QPushButton("Dry Run")
        dry_run_btn.clicked.connect(self._handle_direct_dry_run)
        action_bar.addWidget(dry_run_btn)

        clear_btn = QPushButton("清空选择")
        clear_btn.clicked.connect(self._clear_selection)
        action_bar.addWidget(clear_btn)

        self._execute_btn = QPushButton("执行同步")
        self._execute_btn.clicked.connect(self._handle_direct_execute)
        action_bar.addWidget(self._execute_btn)

        action_bar.addStretch()
        main_layout.addWidget(action_widget)

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
            # 若前缀输入框为空，用该 profile 的 schema_prefix 作为默认值
            if p and not self._prefix_input.text().strip():
                self._prefix_input.setText(p.schema_prefix)
            self._source_snapshot = None
            self.source_schema_selector.clear_schemas()
            self._load_source_metadata()
        else:
            self.current_source_profile = None
            self.source_db_label.setText("")
            self.source_schema_selector.clear_schemas()
            self._source_snapshot = None

    def _on_target_combo_changed(self) -> None:
        profile_id = self.target_combo.currentData()
        if self.app_store is not None and profile_id is not None:
            self.current_target_profile = self.app_store.get_profile(profile_id)
            p = self.current_target_profile
            self.target_type_label.setText(f"{p.db_type.value} · {p.host}:{p.port}" if p else "")
            self._target_snapshot = None
            self.target_schema_selector.clear_schemas()
            self._load_target_metadata()
        else:
            self.current_target_profile = None
            self.target_type_label.setText("")
            self.target_schema_selector.clear_schemas()
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
        self.source_schema_selector.clear_schemas()
        self.target_schema_selector.clear_schemas()
        self._load_source_metadata()
        self._load_target_metadata()

    def _build_metadata_filters(self) -> "MetadataFilters":
        """根据前缀输入框内容构建 MetadataFilters。
        
        支持逗号分隔多个前缀，例如 ``df_,jk_``。
        留空则不限制前缀（加载全部用户/Schema）。
        """
        from db_schema_sync_client.infrastructure.db_metadata import MetadataFilters

        text = self._prefix_input.text().strip()
        return MetadataFilters.from_prefix_text(text)

    def _apply_prefix_filter(self) -> None:
        """点击"应用并刷新"或在输入框按 Enter 时，以新前缀重新加载两侧元数据。"""
        self._load_metadata()

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

        filters = self._build_metadata_filters()
        worker = MetadataWorker(self.metadata_reader, profile, password, filters=filters)

        def _on_done(snapshot: SchemaSnapshot) -> None:
            self._source_snapshot = snapshot
            schemas = sorted({t.schema for t in snapshot.tables})
            self.source_schema_selector.set_schemas(schemas)
            self._worker_done(worker)
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

        filters = self._build_metadata_filters()
        worker = MetadataWorker(self.metadata_reader, profile, password, filters=filters)

        def _on_done(snapshot: SchemaSnapshot) -> None:
            self._target_snapshot = snapshot
            schemas = sorted({t.schema for t in snapshot.tables})
            self.target_schema_selector.set_schemas(schemas)
            self._worker_done(worker)
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

        # Always re-fetch fresh metadata — clear any cached snapshots
        self._source_snapshot = None
        self._target_snapshot = None

        self._compare_btn.setEnabled(False)
        self._show_progress()

        # ── helpers ────────────────────────────────────────────────────
        def _abort(worker, msg: str) -> None:
            self._worker_done(worker)
            self._compare_btn.setEnabled(True)
            QMessageBox.warning(self, "操作失败", msg)

        def _do_compare() -> None:
            sel = self.source_schema_selector.selected_schemas()
            if not sel:
                sel = {t.schema for t in self._source_snapshot.tables}
            f_src = SchemaSnapshot(
                database_name=self._source_snapshot.database_name,
                tables=tuple(t for t in self._source_snapshot.tables if t.schema in sel),
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
                self._compare_btn.setEnabled(True)
                self._persist_compare_task(source, target, diff)
                self.statusBar().showMessage(
                    f"比对完成（{n_schemas} 个 Schema，共 {total} 处差异）", 5000
                )

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
            tw = MetadataWorker(self.metadata_reader, target, target_pw, filters=self._build_metadata_filters())

            def _on_tgt(snap: SchemaSnapshot) -> None:
                self._target_snapshot = snap
                self.target_schema_selector.set_schemas(sorted({t.schema for t in snap.tables}))
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
            sw = MetadataWorker(self.metadata_reader, source, source_pw, filters=self._build_metadata_filters())

            def _on_src(snap: SchemaSnapshot) -> None:
                self._source_snapshot = snap
                self.source_schema_selector.set_schemas(sorted({t.schema for t in snap.tables}))
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
        """Generate a SQL plan from the currently selected auto-syncable diffs."""
        selected_columns = self.comparison_panel.selected_auto_syncable_diffs()
        selected_schemas = self.comparison_panel.selected_schema_syncable_diffs()

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

        # Count non-selected categories from current diff
        manual_count = 0
        hint_count = 0
        if self.current_diff:
            from db_schema_sync_client.domain.diff import DiffCategory

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

        # Schema 级别同步（生成 CREATE SCHEMA + CREATE TABLE）
        if selected_schemas and not column_items:
            return self.sql_generator.generate_schema_creates(
                schema_object_diffs=selected_schemas,
                target_type=target_type,
                manual_count=manual_count,
                hint_count=hint_count,
            )
        elif selected_schemas and column_items:
            # 混合：先建 Schema/Table，再补充字段
            schema_plan = self.sql_generator.generate_schema_creates(
                schema_object_diffs=selected_schemas, target_type=target_type
            )
            col_plan = self.sql_generator.generate_missing_columns(
                items=column_items, target_type=target_type
            )
            risk_levels = {"low": 0, "medium": 1, "high": 2}
            combined_risk = max(
                schema_plan.risk_level, col_plan.risk_level,
                key=lambda r: risk_levels.get(r, 0)
            )
            return GeneratedSqlPlan(
                target_type=target_type,
                statements=schema_plan.statements + col_plan.statements,
                warnings=schema_plan.warnings + col_plan.warnings,
                risk_level=combined_risk,
                auto_syncable_count=len(schema_plan.statements) + len(col_plan.statements),
                manual_required_count=manual_count,
                hint_only_count=hint_count,
            )

        return self.sql_generator.generate_missing_columns(
            items=column_items,
            target_type=target_type,
            manual_count=manual_count,
            hint_count=hint_count,
        )

    def _open_sql_preview(self) -> None:
        if self.current_target_profile is None:
            QMessageBox.warning(self, "SQL 预览", "请先选择目标端连接")
            return

        plan = self.generate_sql_plan_for_selected()
        if not plan.statements:
            QMessageBox.information(self, "SQL 预览", "没有可同步的项目")
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
            QMessageBox.information(self, "Dry Run", "没有可同步的项目")
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
            QMessageBox.information(self, "执行同步", "没有可同步的项目")
            return
        self._execute_sync(plan)

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
