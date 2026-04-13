"""Diff comparison panel — PgAdmin4-style, grouped by object type."""

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
_COLOR_CATEGORY = QColor("#e9ecef")

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
}

_CAT_LABELS = {
    "auto_syncable": "可自动同步",
    "schema_syncable": "可同步 Schema",
    "manual_required": "需人工处理",
    "only_hint": "仅提示",
}

_DIFF_ROLE = Qt.ItemDataRole.UserRole


def _bg(item: QTreeWidgetItem, color: QColor, cols: int = 7) -> None:
    brush = QBrush(color)
    for c in range(cols):
        item.setBackground(c, brush)


def _make_category_node(label: str, different: int, src_only: int, tgt_only: int) -> QTreeWidgetItem:
    """Top-level category node like PgAdmin4: Tables  Different: 0  Source Only: N  Target Only: N"""
    text = f"{label}    Different: {different}    Source Only: {src_only}    Target Only: {tgt_only}"
    item = QTreeWidgetItem([text, "", "", "", "", "", ""])
    font = QFont()
    font.setBold(True)
    item.setFont(_COL_NAME, font)
    _bg(item, _COLOR_CATEGORY)
    return item



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

        # Stats row
        stats_row = QHBoxLayout()
        self.auto_label = QLabel("可自动同步: 0")
        self.manual_label = QLabel("需人工处理: 0")
        self.hint_label = QLabel("仅供参考: 0")
        for lbl in (self.auto_label, self.manual_label, self.hint_label):
            stats_row.addWidget(lbl)
            stats_row.addSpacing(20)
        stats_row.addStretch()
        layout.addLayout(stats_row)

        # Filter row
        filters = QHBoxLayout()
        self.object_type_filter = QComboBox()
        self.object_type_filter.addItem("全部对象", "all")
        self.object_type_filter.addItem("Table", "table")
        self.object_type_filter.addItem("View", "view")
        self.status_filter = QComboBox()
        self.status_filter.addItem("全部分类", "all")
        self.status_filter.addItem("可自动同步", DiffCategory.AUTO_SYNCABLE.value)
        self.status_filter.addItem("可同步 Schema", DiffCategory.SCHEMA_SYNCABLE.value)
        self.status_filter.addItem("需人工处理", DiffCategory.MANUAL_REQUIRED.value)
        self.status_filter.addItem("仅供参考", DiffCategory.ONLY_HINT.value)
        self.only_syncable_checkbox = QCheckBox("只显示可同步项")
        self.object_type_filter.currentIndexChanged.connect(self.refresh_table)
        self.status_filter.currentIndexChanged.connect(self.refresh_table)
        self.only_syncable_checkbox.toggled.connect(self.refresh_table)
        filters.addWidget(QLabel("对象类型:"))
        filters.addWidget(self.object_type_filter)
        filters.addWidget(QLabel("分类:"))
        filters.addWidget(self.status_filter)
        filters.addWidget(self.only_syncable_checkbox)
        filters.addStretch()
        layout.addLayout(filters)

        # Tree widget
        self.tree = QTreeWidget()
        self.tree.setColumnCount(7)
        self.tree.setHeaderLabels(["对象 / 字段", "类型", "源端字段类型", "目标端字段类型", "状态", "分类", "说明"])
        self.tree.setColumnWidth(_COL_NAME, 300)
        self.tree.setColumnWidth(_COL_KIND, 60)
        self.tree.setColumnWidth(_COL_SRC_TYPE, 130)
        self.tree.setColumnWidth(_COL_TGT_TYPE, 130)
        self.tree.setColumnWidth(_COL_STATUS, 110)
        self.tree.setColumnWidth(_COL_CATEGORY, 90)
        self.tree.setAlternatingRowColors(False)
        self.tree.setUniformRowHeights(True)
        self.tree.itemChanged.connect(self._on_item_changed)
        layout.addWidget(self.tree)

    # ── Public API ───────────────────────────────────────────────────────────

    def set_diff(self, diff: SchemaDiff) -> None:
        self.current_diff = diff
        auto_count = sum(1 for d in diff.column_diffs if d.category == DiffCategory.AUTO_SYNCABLE)
        manual_count = (
            sum(1 for d in diff.column_diffs if d.category == DiffCategory.MANUAL_REQUIRED)
            + sum(1 for d in diff.object_diffs if d.category == DiffCategory.MANUAL_REQUIRED)
        )
        hint_count = (
            sum(1 for d in diff.column_diffs if d.category == DiffCategory.ONLY_HINT)
            + sum(1 for d in diff.object_diffs if d.category == DiffCategory.ONLY_HINT)
        )
        self.auto_label.setText(f"可自动同步: {auto_count}")
        self.manual_label.setText(f"需人工处理: {manual_count}")
        self.hint_label.setText(f"仅供参考: {hint_count}")
        self.refresh_table()

    def refresh_table(self) -> None:
        """Rebuild tree: PgAdmin4-style top-level grouping by object type."""
        self.tree.blockSignals(True)
        self.tree.clear()

        category_filter = self.status_filter.currentData()
        obj_type_filter = self.object_type_filter.currentData()
        only_syncable = self.only_syncable_checkbox.isChecked()
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

        all_objects: set[tuple[str, str]] = set(obj_diff_map.keys()) | set(col_diff_map.keys())

        def _obj_type(schema: str, name: str) -> ObjectType:
            od = obj_diff_map.get((schema, name))
            if od:
                if od.source_object:
                    return od.source_object.object_type
                if od.target_object:
                    return od.target_object.object_type
            cds = col_diff_map.get((schema, name), [])
            if cds and cds[0].object_type:
                return cds[0].object_type
            return ObjectType.TABLE

        table_objs = sorted((s, n) for s, n in all_objects if _obj_type(s, n) == ObjectType.TABLE)
        view_objs = sorted((s, n) for s, n in all_objects if _obj_type(s, n) == ObjectType.VIEW)

        # ── Stats ─────────────────────────────────────────────────────────
        def _stats(objs: list[tuple[str, str]]) -> tuple[int, int, int]:
            src = sum(1 for s, n in objs if (s, n) in obj_diff_map and obj_diff_map[(s, n)].status == DiffStatus.ONLY_SOURCE)
            tgt = sum(1 for s, n in objs if (s, n) in obj_diff_map and obj_diff_map[(s, n)].status == DiffStatus.ONLY_TARGET)
            diff_ = sum(1 for s, n in objs if (s, n) in col_diff_map and (s, n) not in obj_diff_map)
            return diff_, src, tgt

        # ── Build one object node (table or view) ─────────────────────────
        def _build_obj_node(schema: str, obj_name: str) -> QTreeWidgetItem | None:
            od = obj_diff_map.get((schema, obj_name))
            col_diffs = col_diff_map.get((schema, obj_name), [])
            kind = "Table" if _obj_type(schema, obj_name) == ObjectType.TABLE else "View"

            obj_node = QTreeWidgetItem([f"{schema} . {obj_name}", kind, "", "", "", "", ""])
            obj_node.setExpanded(True)

            if od is not None:
                status_label = _STATUS_LABELS.get(od.status, od.status.value)
                reason_label = _REASON_LABELS.get(od.reason or "", od.reason or "")
                cat_label = _CAT_LABELS.get(od.category.value, od.category.value)
                obj_node.setText(_COL_STATUS, status_label)
                obj_node.setText(_COL_CATEGORY, cat_label)
                obj_node.setText(_COL_REASON, reason_label)
                if od.status == DiffStatus.ONLY_SOURCE:
                    _bg(obj_node, _COLOR_ONLY_SRC)
                elif od.status == DiffStatus.ONLY_TARGET:
                    _bg(obj_node, _COLOR_ONLY_TGT)
                else:
                    _bg(obj_node, _COLOR_MANUAL)
                if only_syncable:
                    return None
                if category_filter != "all" and od.category.value != category_filter:
                    return None
                return obj_node

            # has column-level diffs
            from db_schema_sync_client.ui.database_tree import format_column_type as _fmt

            has_cols = False
            for cd in sorted(col_diffs, key=lambda x: x.column_name):
                if only_syncable and cd.category != DiffCategory.AUTO_SYNCABLE:
                    continue
                if category_filter != "all" and cd.category.value != category_filter:
                    continue

                src_type = _fmt(cd.source_column) if cd.source_column else ""
                tgt_type = _fmt(cd.target_column) if cd.target_column else ""
                status_label = _STATUS_LABELS.get(cd.status, cd.status.value)
                reason_label = _REASON_LABELS.get(cd.reason or "", cd.reason or "")
                cat_label = _CAT_LABELS.get(cd.category.value, cd.category.value)

                col_node = QTreeWidgetItem(
                    [f"    {cd.column_name}", "字段", src_type, tgt_type, status_label, cat_label, reason_label]
                )
                col_node.setData(_COL_NAME, _DIFF_ROLE, cd)

                if cd.category == DiffCategory.AUTO_SYNCABLE:
                    col_node.setFlags(
                        col_node.flags() | Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled
                    )
                    col_node.setCheckState(_COL_NAME, Qt.CheckState.Checked)
                    _bg(col_node, _COLOR_AUTO)
                elif cd.category == DiffCategory.MANUAL_REQUIRED:
                    _bg(col_node, _COLOR_MANUAL)
                else:
                    _bg(col_node, _COLOR_HINT)

                obj_node.addChild(col_node)
                has_cols = True

            return obj_node if has_cols else None

        def _build_category(label: str, objs: list[tuple[str, str]]) -> None:
            if not objs:
                return
            diff_, src, tgt = _stats(objs)
            cat_node = _make_category_node(label, diff_, src, tgt)
            has_children = False
            for schema, obj_name in objs:
                node = _build_obj_node(schema, obj_name)
                if node is not None:
                    cat_node.addChild(node)
                    has_children = True
            if has_children:
                self.tree.addTopLevelItem(cat_node)
                cat_node.setExpanded(True)

        # ── Render categories ─────────────────────────────────────────────
        if obj_type_filter in ("all", "table"):
            _build_category("Tables", table_objs)
        if obj_type_filter in ("all", "view"):
            _build_category("Views", view_objs)

        # ── Missing / extra schemas ───────────────────────────────────────
        if not only_syncable and (missing_schemas or extra_schemas):
            if category_filter in ("all", DiffCategory.ONLY_HINT.value):
                schema_cat = _make_category_node(
                    "Schemas",
                    0,
                    len(missing_schemas),
                    len(extra_schemas),
                )
                for s in sorted(missing_schemas):
                    n = QTreeWidgetItem([f"  {s}", "Schema", "", "", "仅源端", "仅提示", "目标端缺少此 Schema"])
                    _bg(n, _COLOR_ONLY_SRC)
                    schema_cat.addChild(n)
                for s in sorted(extra_schemas):
                    n = QTreeWidgetItem([f"  {s}", "Schema", "", "", "仅目标端", "仅提示", "源端不存在此 Schema"])
                    _bg(n, _COLOR_ONLY_TGT)
                    schema_cat.addChild(n)
                if schema_cat.childCount() > 0:
                    self.tree.addTopLevelItem(schema_cat)
                    schema_cat.setExpanded(True)

        self.tree.blockSignals(False)
        self.selection_changed.emit()

    def selected_auto_syncable_diffs(self) -> list[ColumnDiff]:
        result: list[ColumnDiff] = []
        self._collect_checked(self.tree.invisibleRootItem(), result)
        return result

    # ── Internals ────────────────────────────────────────────────────────────

    def _collect_checked(self, parent: QTreeWidgetItem, result: list[ColumnDiff]) -> None:
        for i in range(parent.childCount()):
            item = parent.child(i)
            diff = item.data(_COL_NAME, _DIFF_ROLE)
            if (
                isinstance(diff, ColumnDiff)
                and diff.category == DiffCategory.AUTO_SYNCABLE
                and item.checkState(_COL_NAME) == Qt.CheckState.Checked
            ):
                result.append(diff)
            self._collect_checked(item, result)

    def _on_item_changed(self, _item: QTreeWidgetItem) -> None:
        self.selection_changed.emit()


        # Filter row
        filters = QHBoxLayout()
        self.object_type_filter = QComboBox()
        self.object_type_filter.addItem("全部对象", "all")
        self.object_type_filter.addItem("Table", "table")
        self.object_type_filter.addItem("View", "view")
        self.status_filter = QComboBox()
        self.status_filter.addItem("全部分类", "all")
        self.status_filter.addItem("可自动同步", DiffCategory.AUTO_SYNCABLE.value)
        self.status_filter.addItem("需人工处理", DiffCategory.MANUAL_REQUIRED.value)
        self.status_filter.addItem("仅供参考", DiffCategory.ONLY_HINT.value)
        self.only_syncable_checkbox = QCheckBox("只显示可同步字段")
        self.object_type_filter.currentIndexChanged.connect(self.refresh_table)
        self.status_filter.currentIndexChanged.connect(self.refresh_table)
        self.only_syncable_checkbox.toggled.connect(self.refresh_table)
        filters.addWidget(self.object_type_filter)
        filters.addWidget(self.status_filter)
        filters.addWidget(self.only_syncable_checkbox)
        filters.addStretch()
        layout.addLayout(filters)

        # Tree widget
        self.tree = QTreeWidget()
        self.tree.setColumnCount(7)
        self.tree.setHeaderLabels(["名称", "类型", "源端字段类型", "目标端字段类型", "状态", "分类", "说明"])
        self.tree.setColumnWidth(_COL_NAME, 220)
        self.tree.setColumnWidth(_COL_KIND, 60)
        self.tree.setColumnWidth(_COL_SRC_TYPE, 140)
        self.tree.setColumnWidth(_COL_TGT_TYPE, 140)
        self.tree.setColumnWidth(_COL_STATUS, 120)
        self.tree.setColumnWidth(_COL_CATEGORY, 100)
        self.tree.setAlternatingRowColors(False)
        self.tree.setUniformRowHeights(True)
        self.tree.itemChanged.connect(self._on_item_changed)
        layout.addWidget(self.tree)

    # ── Public API ───────────────────────────────────────────────────────────

    def set_diff(self, diff: SchemaDiff) -> None:
        self.current_diff = diff
        auto_count = (
            sum(1 for d in diff.column_diffs if d.category == DiffCategory.AUTO_SYNCABLE)
            + len({od.schema for od in diff.object_diffs if od.category == DiffCategory.SCHEMA_SYNCABLE})
        )
        manual_count = (
            sum(1 for d in diff.column_diffs if d.category == DiffCategory.MANUAL_REQUIRED)
            + sum(1 for d in diff.object_diffs if d.category == DiffCategory.MANUAL_REQUIRED)
        )
        hint_count = (
            sum(1 for d in diff.column_diffs if d.category == DiffCategory.ONLY_HINT)
            + sum(1 for d in diff.object_diffs if d.category == DiffCategory.ONLY_HINT)
        )
        self.auto_label.setText(f"可自动同步: {auto_count}")
        self.manual_label.setText(f"需人工处理: {manual_count}")
        self.hint_label.setText(f"仅供参考: {hint_count}")
        self.refresh_table()

    def refresh_table(self) -> None:
        """Rebuild the tree according to current filters."""
        self.tree.blockSignals(True)
        self.tree.clear()

        category_filter = self.status_filter.currentData()
        obj_type_filter = self.object_type_filter.currentData()
        only_syncable = self.only_syncable_checkbox.isChecked()

        diff = self.current_diff

        # ── Index diffs ──────────────────────────────────────────────────
        # schemas with a "missing_schema" ObjectDiff
        missing_schemas: set[str] = set()
        # schemas with "extra_schema" ObjectDiff
        extra_schemas: set[str] = set()
        # (schema, obj_name) → ObjectDiff
        obj_diff_map: dict[tuple[str, str], ObjectDiff] = {}

        for od in diff.object_diffs:
            if od.reason == "missing_schema":
                missing_schemas.add(od.schema)
            elif od.reason == "extra_schema":
                extra_schemas.add(od.schema)
            else:
                obj_diff_map[(od.schema, od.object_name)] = od

        # (schema, obj_name) → list[ColumnDiff]
        col_diff_map: dict[tuple[str, str], list[ColumnDiff]] = defaultdict(list)
        for cd in diff.column_diffs:
            col_diff_map[(cd.schema, cd.object_name)].append(cd)

        # all schemas present in diffs
        all_schemas: set[str] = set()
        for od in diff.object_diffs:
            all_schemas.add(od.schema)
        for cd in diff.column_diffs:
            all_schemas.add(cd.schema)

        # all objects per schema
        objects_by_schema: dict[str, set[str]] = defaultdict(set)
        for (s, n) in obj_diff_map:
            objects_by_schema[s].add(n)
        for (s, n) in col_diff_map:
            objects_by_schema[s].add(n)

        # ── Build tree ───────────────────────────────────────────────────
        for schema in sorted(all_schemas):
            schema_node = QTreeWidgetItem([f"[Schema]  {schema}", "Schema", "", "", "", "", ""])
            schema_node.setExpanded(True)

            if schema in missing_schemas:
                # SCHEMA_SYNCABLE: 可选择，展开显示该 Schema 下所有表
                schema_obj_diffs = [
                    od for od in diff.object_diffs
                    if od.schema == schema and od.reason == "missing_schema"
                ]
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
                    f"目标端缺少此 Schema（含 {table_count} 个表{'  ' + str(view_count) + ' 个视图' if view_count else ''}）"
                )
                _bg(schema_node, _COLOR_AUTO)
                # 存储 ObjectDiff 列表，用于 SQL 生成
                schema_node.setData(_COL_NAME, _DIFF_ROLE, schema_obj_diffs)
                # 可勾选
                schema_node.setFlags(
                    schema_node.flags()
                    | Qt.ItemFlag.ItemIsUserCheckable
                    | Qt.ItemFlag.ItemIsEnabled
                )
                schema_node.setCheckState(_COL_NAME, Qt.CheckState.Checked)
                # 添加子节点：展示将要创建的表/视图
                for od in sorted(schema_obj_diffs, key=lambda o: o.object_name):
                    if od.source_object is None:
                        continue
                    if obj_type_filter != "all" and od.source_object.object_type.value != obj_type_filter:
                        continue
                    kind = "Table" if od.source_object.object_type == ObjectType.TABLE else "View"
                    col_count = len(od.source_object.columns)
                    child = QTreeWidgetItem([
                        f"  {od.object_name}",
                        kind,
                        "",
                        "",
                        "仅源端",
                        "可同步 Schema",
                        f"将在目标端创建（{col_count} 列）",
                    ])
                    _bg(child, QColor("#c3e6cb"))
                    schema_node.addChild(child)
                # 分类过滤匹配 SCHEMA_SYNCABLE 或 all
                if category_filter in ("all", DiffCategory.SCHEMA_SYNCABLE.value):
                    self.tree.addTopLevelItem(schema_node)
                    schema_node.setExpanded(True)
                continue

            if schema in extra_schemas:
                schema_node.setText(_COL_STATUS, "仅目标端")
                schema_node.setText(_COL_CATEGORY, "仅提示")
                schema_node.setText(_COL_REASON, "源端不存在此 Schema")
                _bg(schema_node, _COLOR_ONLY_TGT)
                if not only_syncable and (category_filter == "all" or category_filter == DiffCategory.ONLY_HINT.value):
                    self.tree.addTopLevelItem(schema_node)
                continue

            # Schema exists in both — iterate objects
            has_visible_children = False
            for obj_name in sorted(objects_by_schema[schema]):
                od = obj_diff_map.get((schema, obj_name))
                col_diffs = col_diff_map.get((schema, obj_name), [])

                # Determine object type
                if od and od.source_object:
                    obj_type = od.source_object.object_type
                elif od and od.target_object:
                    obj_type = od.target_object.object_type
                elif col_diffs:
                    obj_type = col_diffs[0].object_type or ObjectType.TABLE
                else:
                    obj_type = ObjectType.TABLE

                kind_label = "Table" if obj_type == ObjectType.TABLE else "View"

                # Apply object-type filter
                if obj_type_filter != "all" and obj_type.value != obj_type_filter:
                    continue

                obj_node = QTreeWidgetItem([f"[{kind_label}]  {obj_name}", kind_label, "", "", "", "", ""])
                obj_node.setExpanded(True)

                if od is not None:
                    # Object-level diff (missing table/view or extra)
                    status_label = _STATUS_LABELS.get(od.status, od.status.value)
                    reason_label = _REASON_LABELS.get(od.reason or "", od.reason or "")
                    cat_label = {"auto_syncable": "可自动同步", "manual_required": "需人工处理", "only_hint": "仅提示"}.get(od.category.value, od.category.value)
                    obj_node.setText(_COL_STATUS, status_label)
                    obj_node.setText(_COL_CATEGORY, cat_label)
                    obj_node.setText(_COL_REASON, reason_label)

                    if od.status == DiffStatus.ONLY_SOURCE:
                        _bg(obj_node, _COLOR_ONLY_SRC)
                    elif od.status == DiffStatus.ONLY_TARGET:
                        _bg(obj_node, _COLOR_ONLY_TGT)
                    else:
                        _bg(obj_node, _COLOR_MANUAL)

                    cat_val = od.category.value
                    if only_syncable:
                        continue
                    if category_filter != "all" and cat_val != category_filter:
                        continue
                    schema_node.addChild(obj_node)
                    has_visible_children = True
                    continue

                # Table exists in both — show column diffs
                has_visible_cols = False
                for cd in sorted(col_diffs, key=lambda x: x.column_name):
                    if only_syncable and cd.category != DiffCategory.AUTO_SYNCABLE:
                        continue
                    if category_filter != "all" and cd.category.value != category_filter:
                        continue

                    from db_schema_sync_client.ui.database_tree import format_column_type as _fmt

                    src_type = _fmt(cd.source_column) if cd.source_column else ""
                    tgt_type = _fmt(cd.target_column) if cd.target_column else ""
                    status_label = _STATUS_LABELS.get(cd.status, cd.status.value)
                    reason_label = _REASON_LABELS.get(cd.reason or "", cd.reason or "")
                    cat_label = {"auto_syncable": "可自动同步", "manual_required": "需人工处理", "only_hint": "仅提示"}.get(cd.category.value, cd.category.value)

                    col_node = QTreeWidgetItem(
                        [f"  {cd.column_name}", "字段", src_type, tgt_type, status_label, cat_label, reason_label]
                    )
                    col_node.setData(_COL_NAME, _DIFF_ROLE, cd)

                    if cd.category == DiffCategory.AUTO_SYNCABLE:
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

                if has_visible_cols or obj_node.childCount() > 0:
                    schema_node.addChild(obj_node)
                    has_visible_children = True

            if has_visible_children or schema_node.childCount() > 0:
                self.tree.addTopLevelItem(schema_node)

        self.tree.blockSignals(False)
        self.selection_changed.emit()

    def selected_auto_syncable_diffs(self) -> list[ColumnDiff]:
        result: list[ColumnDiff] = []
        self._collect_checked(self.tree.invisibleRootItem(), result)
        return result

    def selected_schema_syncable_diffs(self) -> list[ObjectDiff]:
        """Return all ObjectDiff items for checked missing-schema rows."""
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

    # ── Internals ────────────────────────────────────────────────────────────

    def _collect_checked(self, parent: QTreeWidgetItem, result: list[ColumnDiff]) -> None:
        for i in range(parent.childCount()):
            item = parent.child(i)
            diff = item.data(_COL_NAME, _DIFF_ROLE)
            if (
                isinstance(diff, ColumnDiff)
                and diff.category == DiffCategory.AUTO_SYNCABLE
                and item.checkState(_COL_NAME) == Qt.CheckState.Checked
            ):
                result.append(diff)
            self._collect_checked(item, result)

    def _on_item_changed(self, _item: QTreeWidgetItem) -> None:
        self.selection_changed.emit()
