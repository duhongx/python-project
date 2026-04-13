"""Diff comparison panel — grouped by schema."""

from __future__ import annotations

from collections import defaultdict

from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QBrush, QColor, QFont
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from db_schema_sync_client.domain.diff import ColumnDiff, DiffCategory, DiffStatus, ObjectDiff, SchemaDiff
from db_schema_sync_client.domain.models import ObjectType

# ── Colour palette ──────────────────────────────────────────────────────────
_COLOR_AUTO = QColor("#d4edda")
_COLOR_MANUAL = QColor("#fff3cd")
_COLOR_HINT = QColor("#f8f9fa")
_COLOR_ONLY_SRC = QColor("#ffeeba")
_COLOR_ONLY_TGT = QColor("#d1ecf1")
_COLOR_SCHEMA = QColor("#cce5ff")

# Column indices
_COL_NAME = 0
_COL_KIND = 1
_COL_SRC_TYPE = 2
_COL_TGT_TYPE = 3
_COL_STATUS = 4
_COL_CATEGORY = 5
_COL_REASON = 6

_STATUS_LABELS = {
    DiffStatus.ONLY_SOURCE: "仅源端",
    DiffStatus.ONLY_TARGET: "仅目标端",
    DiffStatus.TYPE_MISMATCH: "类型不一致",
    DiffStatus.NULLABILITY_MISMATCH: "可空性不一致",
    DiffStatus.DEFAULT_MISMATCH: "默认值不一致",
    DiffStatus.MATCH: "一致",
}
_REASON_LABELS = {
    "missing_schema": "目标端缺少此 Schema",
    "missing_table": "目标端缺少此表",
    "missing_view": "目标端缺少此视图",
    "extra_schema": "源端不存在此 Schema",
    "extra_object": "源端不存在此对象",
    "missing_target_column": "目标端缺少此字段",
    "extra_target_column": "目标端多余此字段",
}
_CAT_LABELS = {
    "auto_syncable": "可自动同步",
    "schema_syncable": "可同步 Schema",
    "table_syncable": "可同步对象",
    "view_rebuild_syncable": "可重建视图",
    "manual_required": "需人工处理",
    "only_hint": "仅提示",
}

_DIFF_ROLE = Qt.ItemDataRole.UserRole


def _bg(item: QTreeWidgetItem, color: QColor, cols: int = 7) -> None:
    brush = QBrush(color)
    for c in range(cols):
        item.setBackground(c, brush)


class ComparisonPanel(QWidget):
    selection_changed = pyqtSignal()

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.current_diff = SchemaDiff()
        self._build_ui()

    # ── UI construction ──────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(4)

        # Stats row
        stats_row = QHBoxLayout()
        self.auto_label = QLabel("可自动同步: 0")
        self.table_label = QLabel("可同步对象: 0")
        self.manual_label = QLabel("需人工处理: 0")
        self.hint_label = QLabel("仅供参考: 0")
        for lbl in (self.auto_label, self.table_label, self.manual_label, self.hint_label):
            stats_row.addWidget(lbl)
            stats_row.addSpacing(20)
        stats_row.addStretch()
        layout.addLayout(stats_row)

        # Filter row
        filters = QHBoxLayout()
        filters.setSpacing(8)

        self.object_type_filter = QComboBox()
        self.object_type_filter.addItem("全部对象", "all")
        self.object_type_filter.addItem("Table", "table")
        self.object_type_filter.addItem("View", "view")

        self.status_filter = QComboBox()
        self.status_filter.addItem("全部分类", "all")
        self.status_filter.addItem("可自动同步", DiffCategory.AUTO_SYNCABLE.value)
        self.status_filter.addItem("可同步 Schema", DiffCategory.SCHEMA_SYNCABLE.value)
        self.status_filter.addItem("可同步对象", DiffCategory.TABLE_SYNCABLE.value)
        self.status_filter.addItem("可重建视图", DiffCategory.VIEW_REBUILD_SYNCABLE.value)
        self.status_filter.addItem("需人工处理", DiffCategory.MANUAL_REQUIRED.value)
        self.status_filter.addItem("仅供参考", DiffCategory.ONLY_HINT.value)

        # 默认不勾选 — 展示全部差异
        self.only_syncable_checkbox = QCheckBox("只显示可同步项")
        self.only_syncable_checkbox.setChecked(False)

        # 忽略目标端多余对象（如客户现场的备份表）
        self.hide_only_target_checkbox = QCheckBox("忽略目标端多余对象")
        self.hide_only_target_checkbox.setChecked(False)
        self.hide_only_target_checkbox.setToolTip(
            "勾选后隐藏目标端多出的表/视图/字段（状态为\u201c仅目标端\u201d的差异），\n"
            "适用于目标端存在大量备份表的场景。"
        )

        # 目标端不一致项快捷筛选（可叠加）
        self.only_table_mismatch_checkbox = QCheckBox("仅显示目标端表不一致")
        self.only_view_mismatch_checkbox = QCheckBox("仅显示目标端视图不一致")

        self.object_type_filter.currentIndexChanged.connect(self.refresh_table)
        self.status_filter.currentIndexChanged.connect(self.refresh_table)
        self.only_syncable_checkbox.toggled.connect(self.refresh_table)
        self.hide_only_target_checkbox.toggled.connect(self.refresh_table)
        self.only_table_mismatch_checkbox.toggled.connect(self.refresh_table)
        self.only_view_mismatch_checkbox.toggled.connect(self.refresh_table)

        filters.addWidget(QLabel("对象类型:"))
        filters.addWidget(self.object_type_filter)
        filters.addWidget(QLabel("分类:"))
        filters.addWidget(self.status_filter)
        filters.addWidget(self.only_syncable_checkbox)
        filters.addWidget(self.hide_only_target_checkbox)
        filters.addWidget(self.only_table_mismatch_checkbox)
        filters.addWidget(self.only_view_mismatch_checkbox)
        filters.addStretch()
        layout.addLayout(filters)

        # 空状态提示（过滤条件下无结果时显示）
        self.empty_label = QLabel("当前过滤条件下无差异项")
        self.empty_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.empty_label.setStyleSheet(
            "QLabel { color: #6c757d; padding: 24px; font-size: 13px; }"
        )
        self.empty_label.setMinimumHeight(24)
        self.empty_label.setText("")
        layout.addWidget(self.empty_label)

        # Tree widget
        self.tree = QTreeWidget()
        self.tree.setColumnCount(7)
        self.tree.setHeaderLabels(["名称", "类型", "源端字段类型", "目标端字段类型", "状态", "分类", "说明"])
        self.tree.setColumnWidth(_COL_NAME, 260)
        self.tree.setColumnWidth(_COL_KIND, 60)
        self.tree.setColumnWidth(_COL_SRC_TYPE, 140)
        self.tree.setColumnWidth(_COL_TGT_TYPE, 140)
        self.tree.setColumnWidth(_COL_STATUS, 110)
        self.tree.setColumnWidth(_COL_CATEGORY, 100)
        self.tree.setAlternatingRowColors(False)
        self.tree.setUniformRowHeights(True)
        self.tree.itemChanged.connect(self._on_item_changed)
        layout.addWidget(self.tree)

    # ── Public API ───────────────────────────────────────────────────────────

    def set_diff(self, diff: SchemaDiff) -> None:
        self.current_diff = diff
        auto_count = (
            sum(
                1
                for d in diff.column_diffs
                if d.category in (DiffCategory.AUTO_SYNCABLE, DiffCategory.VIEW_REBUILD_SYNCABLE)
            )
            + len({od.schema for od in diff.object_diffs if od.category == DiffCategory.SCHEMA_SYNCABLE})
        )
        table_count = sum(1 for od in diff.object_diffs if od.category == DiffCategory.TABLE_SYNCABLE)
        manual_count = (
            sum(1 for d in diff.column_diffs if d.category == DiffCategory.MANUAL_REQUIRED)
            + sum(1 for d in diff.object_diffs if d.category == DiffCategory.MANUAL_REQUIRED)
        )
        hint_count = (
            sum(1 for d in diff.column_diffs if d.category == DiffCategory.ONLY_HINT)
            + sum(1 for d in diff.object_diffs if d.category == DiffCategory.ONLY_HINT)
        )
        self.auto_label.setText(f"可自动同步: {auto_count}")
        self.table_label.setText(f"可同步对象: {table_count}")
        self.manual_label.setText(f"需人工处理: {manual_count}")
        self.hint_label.setText(f"仅供参考: {hint_count}")
        self.refresh_table()

    def refresh_table(self) -> None:
        """Rebuild the tree according to current filters."""
        self.tree.blockSignals(True)
        self.tree.clear()

        category_filter: str = self.status_filter.currentData() or "all"
        obj_type_filter: str = self.object_type_filter.currentData() or "all"
        only_syncable: bool = self.only_syncable_checkbox.isChecked()
        hide_only_target: bool = self.hide_only_target_checkbox.isChecked()
        only_table_mismatch: bool = self.only_table_mismatch_checkbox.isChecked()
        only_view_mismatch: bool = self.only_view_mismatch_checkbox.isChecked()
        target_missing_filters_active: bool = any(
            [only_table_mismatch, only_view_mismatch]
        )

        diff = self.current_diff

        # ── Index diffs ──────────────────────────────────────────────────
        missing_schemas: set[str] = set()
        extra_schemas: set[str] = set()
        obj_diff_map: dict[tuple[str, str], ObjectDiff] = {}

        for od in diff.object_diffs:
            if od.reason == "missing_schema":
                missing_schemas.add(od.schema)
            elif od.reason == "extra_schema":
                extra_schemas.add(od.schema)
            else:
                obj_diff_map[(od.schema, od.object_name)] = od

        col_diff_map: dict[tuple[str, str], list[ColumnDiff]] = defaultdict(list)
        for cd in diff.column_diffs:
            col_diff_map[(cd.schema, cd.object_name)].append(cd)

        all_schemas: set[str] = set()
        for od in diff.object_diffs:
            all_schemas.add(od.schema)
        for cd in diff.column_diffs:
            all_schemas.add(cd.schema)

        objects_by_schema: dict[str, set[str]] = defaultdict(set)
        for (s, n) in obj_diff_map:
            objects_by_schema[s].add(n)
        for (s, n) in col_diff_map:
            objects_by_schema[s].add(n)

        # ── Build tree ───────────────────────────────────────────────────
        for schema in sorted(all_schemas):
            _font = QFont()
            _font.setBold(True)
            schema_node = QTreeWidgetItem([f"[Schema]  {schema}", "Schema", "", "", "", "", ""])
            schema_node.setFont(_COL_NAME, _font)
            schema_node.setExpanded(True)

            if schema in missing_schemas:
                # 目标端整个 Schema 缺失 → SCHEMA_SYNCABLE
                if category_filter not in ("all", DiffCategory.SCHEMA_SYNCABLE.value):
                    continue

                schema_obj_diffs = [
                    od for od in diff.object_diffs
                    if od.schema == schema and od.reason == "missing_schema"
                ]
                if target_missing_filters_active:
                    filtered_schema_obj_diffs: list[ObjectDiff] = []
                    for od in schema_obj_diffs:
                        src_obj = od.source_object
                        if src_obj is None:
                            continue
                        if src_obj.object_type == ObjectType.TABLE and only_table_mismatch:
                            filtered_schema_obj_diffs.append(od)
                        if src_obj.object_type == ObjectType.VIEW and only_view_mismatch:
                            filtered_schema_obj_diffs.append(od)
                    schema_obj_diffs = filtered_schema_obj_diffs
                    if not schema_obj_diffs:
                        continue

                table_count = sum(
                    1 for od in schema_obj_diffs
                    if od.source_object and od.source_object.object_type == ObjectType.TABLE
                )
                view_count = sum(
                    1 for od in schema_obj_diffs
                    if od.source_object and od.source_object.object_type == ObjectType.VIEW
                )
                schema_node.setText(_COL_STATUS, "仅源端")
                schema_node.setText(_COL_CATEGORY, "可同步 Schema")
                schema_node.setText(
                    _COL_REASON,
                    f"目标端缺少此 Schema（含 {table_count} 个表"
                    + (f"  {view_count} 个视图" if view_count else "") + "）"
                )
                _bg(schema_node, _COLOR_SCHEMA)
                schema_node.setData(_COL_NAME, _DIFF_ROLE, schema_obj_diffs)
                schema_node.setFlags(
                    schema_node.flags()
                    | Qt.ItemFlag.ItemIsUserCheckable
                    | Qt.ItemFlag.ItemIsEnabled
                )
                schema_node.setCheckState(_COL_NAME, Qt.CheckState.Checked)
                for od in sorted(schema_obj_diffs, key=lambda o: o.object_name):
                    if od.source_object is None:
                        continue
                    if obj_type_filter != "all" and od.source_object.object_type.value != obj_type_filter:
                        continue
                    kind = "Table" if od.source_object.object_type == ObjectType.TABLE else "View"
                    child = QTreeWidgetItem([
                        f"  {od.object_name}", kind, "", "", "仅源端", "可同步 Schema",
                        f"将在目标端创建（{len(od.source_object.columns)} 列）",
                    ])
                    _bg(child, QColor("#c3e6cb"))
                    schema_node.addChild(child)
                self.tree.addTopLevelItem(schema_node)
                schema_node.setExpanded(True)
                continue

            if schema in extra_schemas:
                # 目标端多出整个 Schema → ONLY_HINT
                if hide_only_target:
                    continue
                if only_syncable:
                    continue
                if category_filter not in ("all", DiffCategory.ONLY_HINT.value):
                    continue
                schema_node.setText(_COL_STATUS, "仅目标端")
                schema_node.setText(_COL_CATEGORY, "仅提示")
                schema_node.setText(_COL_REASON, "源端不存在此 Schema")
                _bg(schema_node, _COLOR_ONLY_TGT)
                self.tree.addTopLevelItem(schema_node)
                continue

            # Schema 在两端均存在 — 遍历对象
            has_visible_children = False
            for obj_name in sorted(objects_by_schema[schema]):
                od = obj_diff_map.get((schema, obj_name))
                col_diffs = col_diff_map.get((schema, obj_name), [])

                if od and od.source_object:
                    obj_type = od.source_object.object_type
                elif od and od.target_object:
                    obj_type = od.target_object.object_type
                elif col_diffs:
                    obj_type = col_diffs[0].object_type or ObjectType.TABLE
                else:
                    obj_type = ObjectType.TABLE

                kind_label = "Table" if obj_type == ObjectType.TABLE else "View"

                if obj_type_filter != "all" and obj_type.value != obj_type_filter:
                    continue

                obj_node = QTreeWidgetItem([f"[{kind_label}]  {obj_name}", kind_label, "", "", "", "", ""])
                obj_node.setExpanded(True)

                if od is not None:
                    # 对象级差异（只在源端 / 只在目标端）
                    if hide_only_target and od.status == DiffStatus.ONLY_TARGET:
                        continue
                    if target_missing_filters_active:
                        if od.reason == "missing_table" and not only_table_mismatch:
                            continue
                        if od.reason == "missing_view" and not only_view_mismatch:
                            continue
                        if od.reason not in ("missing_table", "missing_view"):
                            continue
                    if only_syncable and od.category != DiffCategory.TABLE_SYNCABLE:
                        continue
                    cat_val = od.category.value
                    if category_filter != "all" and cat_val != category_filter:
                        continue

                    status_label = _STATUS_LABELS.get(od.status, od.status.value)
                    reason_label = _REASON_LABELS.get(od.reason or "", od.reason or "")
                    cat_label = _CAT_LABELS.get(cat_val, cat_val)
                    obj_node.setText(_COL_STATUS, status_label)
                    obj_node.setText(_COL_CATEGORY, cat_label)
                    obj_node.setText(_COL_REASON, reason_label)
                    if od.category == DiffCategory.TABLE_SYNCABLE:
                        _bg(obj_node, _COLOR_AUTO)
                        obj_node.setData(_COL_NAME, _DIFF_ROLE, od)
                        obj_node.setFlags(
                            obj_node.flags()
                            | Qt.ItemFlag.ItemIsUserCheckable
                            | Qt.ItemFlag.ItemIsEnabled
                        )
                        obj_node.setCheckState(_COL_NAME, Qt.CheckState.Checked)
                    elif od.status == DiffStatus.ONLY_SOURCE:
                        _bg(obj_node, _COLOR_ONLY_SRC)
                    elif od.status == DiffStatus.ONLY_TARGET:
                        _bg(obj_node, _COLOR_ONLY_TGT)
                    else:
                        _bg(obj_node, _COLOR_MANUAL)

                    schema_node.addChild(obj_node)
                    has_visible_children = True
                    continue

                # 两端均有此对象 — 展示字段级差异
                from db_schema_sync_client.ui.database_tree import format_column_type as _fmt

                def _fmt_null(col):
                    if col is None:
                        return ""
                    null_mark = "NULL" if col.is_nullable else "NOT NULL"
                    return f"{_fmt(col)}  {null_mark}"

                has_visible_cols = False
                for cd in sorted(col_diffs, key=lambda x: x.column_name):
                    if hide_only_target and cd.status == DiffStatus.ONLY_TARGET:
                        continue
                    if target_missing_filters_active:
                        if cd.reason != "missing_target_column":
                            continue
                        cd_obj_type = cd.object_type or obj_type
                        if cd_obj_type == ObjectType.TABLE and not only_table_mismatch:
                            continue
                        if cd_obj_type == ObjectType.VIEW and not only_view_mismatch:
                            continue
                    if only_syncable and cd.category != DiffCategory.AUTO_SYNCABLE:
                        if cd.category != DiffCategory.VIEW_REBUILD_SYNCABLE:
                            continue
                    if category_filter != "all" and cd.category.value != category_filter:
                        continue

                    src_type = _fmt_null(cd.source_column)
                    tgt_type = _fmt_null(cd.target_column)
                    status_label = _STATUS_LABELS.get(cd.status, cd.status.value)
                    reason_label = _REASON_LABELS.get(cd.reason or "", cd.reason or "")
                    cat_label = _CAT_LABELS.get(cd.category.value, cd.category.value)

                    col_node = QTreeWidgetItem(
                        [f"  {cd.column_name}", "字段", src_type, tgt_type, status_label, cat_label, reason_label]
                    )
                    col_node.setData(_COL_NAME, _DIFF_ROLE, cd)

                    if cd.category in (DiffCategory.AUTO_SYNCABLE, DiffCategory.VIEW_REBUILD_SYNCABLE):
                        col_node.setFlags(
                            col_node.flags()
                            | Qt.ItemFlag.ItemIsUserCheckable
                            | Qt.ItemFlag.ItemIsEnabled
                        )
                        col_node.setCheckState(_COL_NAME, Qt.CheckState.Checked)
                        _bg(col_node, _COLOR_AUTO)
                    elif cd.category == DiffCategory.MANUAL_REQUIRED:
                        _bg(col_node, _COLOR_MANUAL)
                    else:
                        _bg(col_node, _COLOR_HINT)

                    obj_node.addChild(col_node)
                    has_visible_cols = True

                if has_visible_cols:
                    schema_node.addChild(obj_node)
                    has_visible_children = True

            if has_visible_children:
                self.tree.addTopLevelItem(schema_node)

        self.tree.blockSignals(False)

        # 更新空状态：保持树始终可见，避免筛选后布局跳变
        is_empty = self.tree.topLevelItemCount() == 0
        self.empty_label.setText("当前过滤条件下无差异项" if is_empty else "")
        self.tree.setVisible(True)

        self.selection_changed.emit()

    def selected_auto_syncable_diffs(self) -> list[ColumnDiff]:
        result: list[ColumnDiff] = []
        self._collect_checked(self.tree.invisibleRootItem(), result, category=DiffCategory.AUTO_SYNCABLE)
        return result

    def selected_view_rebuild_diffs(self) -> list[ColumnDiff]:
        result: list[ColumnDiff] = []
        self._collect_checked(
            self.tree.invisibleRootItem(),
            result,
            category=DiffCategory.VIEW_REBUILD_SYNCABLE,
        )
        return result

    def selected_schema_syncable_diffs(self) -> list[ObjectDiff]:
        """Return ObjectDiff items for all checked missing-schema rows."""
        result: list[ObjectDiff] = []
        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):
            item = root.child(i)
            data = item.data(_COL_NAME, _DIFF_ROLE)
            if (
                isinstance(data, list)
                and item.checkState(_COL_NAME) == Qt.CheckState.Checked
            ):
                result.extend(data)
        return result

    def selected_table_syncable_diffs(self) -> list[ObjectDiff]:
        """Return ObjectDiff items for all checked TABLE_SYNCABLE object rows."""
        result: list[ObjectDiff] = []
        root = self.tree.invisibleRootItem()
        for i in range(root.childCount()):          # schema level
            schema_node = root.child(i)
            for j in range(schema_node.childCount()):  # object level
                obj_node = schema_node.child(j)
                data = obj_node.data(_COL_NAME, _DIFF_ROLE)
                if (
                    isinstance(data, ObjectDiff)
                    and data.category == DiffCategory.TABLE_SYNCABLE
                    and obj_node.checkState(_COL_NAME) == Qt.CheckState.Checked
                ):
                    result.append(data)
        return result

    # ── Internals ────────────────────────────────────────────────────────────

    def _collect_checked(
        self,
        parent: QTreeWidgetItem,
        result: list[ColumnDiff],
        *,
        category: DiffCategory,
    ) -> None:
        for i in range(parent.childCount()):
            item = parent.child(i)
            diff = item.data(_COL_NAME, _DIFF_ROLE)
            if (
                isinstance(diff, ColumnDiff)
                and diff.category == category
                and item.checkState(_COL_NAME) == Qt.CheckState.Checked
            ):
                result.append(diff)
            self._collect_checked(item, result, category=category)

    def _on_item_changed(self, _item: QTreeWidgetItem) -> None:
        self.selection_changed.emit()
