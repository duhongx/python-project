"""Database structure tree widget."""

from __future__ import annotations

from collections import defaultdict

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QLineEdit,
    QPushButton,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from db_schema_sync_client.domain.models import ColumnDefinition, ObjectType, SchemaSnapshot


_NO_PRECISION_TYPES = {
    "integer", "int", "int4", "int2", "int8",
    "bigint", "smallint", "boolean", "bool",
    "text", "date", "oid", "uuid",
    "json", "jsonb", "bytea", "real",
    "double precision", "float", "float4", "float8",
    "serial", "bigserial", "smallserial",
}


def format_column_type(column: ColumnDefinition) -> str:
    data_type = column.data_type
    if data_type.lower() in _NO_PRECISION_TYPES:
        return data_type
    if column.character_maximum_length:
        return f"{data_type}({column.character_maximum_length})"
    if column.numeric_precision is not None and column.numeric_scale is not None:
        return f"{data_type}({column.numeric_precision}, {column.numeric_scale})"
    if column.numeric_precision is not None and data_type.lower() in {"numeric", "decimal"}:
        return f"{data_type}({column.numeric_precision})"
    return data_type


class _SchemaMultiSelect(QWidget):
    """A simple multi-select schema filter widget via checkable combo box."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self.combo = QComboBox()
        self.combo.setPlaceholderText("Schema 选择…")
        self.combo.setEditable(False)
        layout.addWidget(self.combo)
        self._all_schemas: list[str] = []
        self._checked: set[str] = set()

    # -- public API --

    def set_schemas(self, schemas: list[str]) -> None:
        self._all_schemas = sorted(schemas)
        self._checked = set(self._all_schemas)  # default: all selected
        self._rebuild()

    def selected_schemas(self) -> set[str]:
        return set(self._checked)

    # -- internal --

    def _rebuild(self) -> None:
        self.combo.blockSignals(True)
        self.combo.clear()
        self.combo.addItem(f"全部 ({len(self._all_schemas)})")
        from PyQt6.QtGui import QStandardItem

        model = self.combo.model()
        for schema in self._all_schemas:
            self.combo.addItem(schema)
            idx = self.combo.count() - 1
            item: QStandardItem = model.item(idx)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(
                Qt.CheckState.Checked if schema in self._checked else Qt.CheckState.Unchecked
            )
        self.combo.blockSignals(False)
        model.itemChanged.connect(self._on_item_changed)

    def _on_item_changed(self, item) -> None:

        text = item.text()
        if text in self._all_schemas:
            if item.checkState() == Qt.CheckState.Checked:
                self._checked.add(text)
            else:
                self._checked.discard(text)


class DatabaseTreeWidget(QWidget):
    def __init__(self, title: str, parent=None) -> None:
        super().__init__(parent)
        self.title = title
        self._snapshot: SchemaSnapshot | None = None
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Filter row
        filter_row = QHBoxLayout()

        self.schema_multi_select = _SchemaMultiSelect()
        filter_row.addWidget(self.schema_multi_select)

        self.schema_filter = QLineEdit()
        self.schema_filter.setPlaceholderText("Schema 关键字…")
        self.schema_filter.textChanged.connect(self._apply_filter)
        filter_row.addWidget(self.schema_filter)

        self.name_filter = QLineEdit()
        self.name_filter.setPlaceholderText("对象名过滤…")
        self.name_filter.textChanged.connect(self._apply_filter)
        filter_row.addWidget(self.name_filter)

        self.type_filter = QComboBox()
        self.type_filter.addItem("全部", "all")
        self.type_filter.addItem("Table", "table")
        self.type_filter.addItem("View", "view")
        self.type_filter.currentIndexChanged.connect(self._apply_filter)
        filter_row.addWidget(self.type_filter)

        apply_btn = QPushButton("应用")
        apply_btn.clicked.connect(self._apply_filter)
        filter_row.addWidget(apply_btn)

        layout.addLayout(filter_row)

        # Tree
        self.tree = QTreeWidget()
        self.tree.setColumnCount(5)
        self.tree.setHeaderLabels(["对象", "类型", "可空", "默认值", "位置"])
        self.tree.setAlternatingRowColors(True)
        self.tree.setUniformRowHeights(True)
        self.tree.setColumnWidth(0, 240)
        self.tree.setColumnWidth(1, 180)
        self.tree.itemExpanded.connect(self._on_item_expanded)
        self.tree.itemChanged.connect(self._on_tree_item_changed)
        layout.addWidget(self.tree)

    _PLACEHOLDER_TEXT = "__lazy_placeholder__"

    def load_snapshot(self, snapshot: SchemaSnapshot | None) -> None:
        self._snapshot = snapshot
        # Populate schema multi-select
        if snapshot is not None:
            all_schemas = sorted({t.schema for t in snapshot.tables})
            self.schema_multi_select.set_schemas(all_schemas)
        else:
            self.schema_multi_select.set_schemas([])
        self._apply_filter()

    def clear(self) -> None:
        self._snapshot = None
        self.tree.clear()

    _CHECKABLE = Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled

    def _apply_filter(self) -> None:
        self.tree.blockSignals(True)
        self.tree.clear()
        if self._snapshot is None:
            self.tree.blockSignals(False)
            return

        schema_kw = self.schema_filter.text().strip().lower()
        name_kw = self.name_filter.text().strip().lower()
        type_val = self.type_filter.currentData()
        selected_schemas = self.schema_multi_select.selected_schemas()

        root = QTreeWidgetItem([self._snapshot.database_name, "Database", "", "", ""])
        root.setFlags(root.flags() | self._CHECKABLE)
        root.setCheckState(0, Qt.CheckState.Checked)
        self.tree.addTopLevelItem(root)

        schemas: dict[str, list] = defaultdict(list)
        for table in self._snapshot.tables:
            if selected_schemas and table.schema not in selected_schemas:
                continue
            if schema_kw and schema_kw not in table.schema.lower():
                continue
            if name_kw and name_kw not in table.name.lower():
                continue
            if type_val == "table" and table.object_type != ObjectType.TABLE:
                continue
            if type_val == "view" and table.object_type != ObjectType.VIEW:
                continue
            schemas[table.schema].append(table)

        for schema_name in sorted(schemas):
            schema_item = QTreeWidgetItem([schema_name, "Schema", "", "", ""])
            schema_item.setFlags(schema_item.flags() | self._CHECKABLE)
            schema_item.setCheckState(0, Qt.CheckState.Checked)
            root.addChild(schema_item)
            for table in sorted(schemas[schema_name], key=lambda item: item.name):
                object_type = "Table" if table.object_type == ObjectType.TABLE else "View"
                table_item = QTreeWidgetItem([table.name, object_type, "", "", ""])
                table_item.setFlags(table_item.flags() | self._CHECKABLE)
                table_item.setCheckState(0, Qt.CheckState.Checked)
                table_item.setData(0, Qt.ItemDataRole.UserRole, table)
                # Add a placeholder child so the expand arrow appears
                placeholder = QTreeWidgetItem([self._PLACEHOLDER_TEXT])
                table_item.addChild(placeholder)
                schema_item.addChild(table_item)

        root.setExpanded(True)
        self.tree.blockSignals(False)

    # ------------------------------------------------------------------
    # Checkbox cascade
    # ------------------------------------------------------------------

    def _on_tree_item_changed(self, item: QTreeWidgetItem, column: int) -> None:
        """Cascade check state downward when a node is toggled."""
        if column != 0:
            return
        self.tree.blockSignals(True)
        self._cascade_to_children(item, item.checkState(0))
        self.tree.blockSignals(False)

    def _cascade_to_children(self, item: QTreeWidgetItem, state: Qt.CheckState) -> None:
        for i in range(item.childCount()):
            child = item.child(i)
            if child.text(0) == self._PLACEHOLDER_TEXT:
                continue
            if child.flags() & Qt.ItemFlag.ItemIsUserCheckable:
                child.setCheckState(0, state)
                self._cascade_to_children(child, state)

    def get_checked_tables(self) -> list:
        """Return TableDefinition objects for all currently checked Table/View nodes."""
        result = []
        root_item = self.tree.invisibleRootItem()
        for i in range(root_item.childCount()):          # Database level
            db_node = root_item.child(i)
            if db_node.checkState(0) == Qt.CheckState.Unchecked:
                continue
            for j in range(db_node.childCount()):        # Schema level
                schema_node = db_node.child(j)
                if schema_node.checkState(0) == Qt.CheckState.Unchecked:
                    continue
                for k in range(schema_node.childCount()):  # Table/View level
                    table_node = schema_node.child(k)
                    if table_node.checkState(0) == Qt.CheckState.Checked:
                        table = table_node.data(0, Qt.ItemDataRole.UserRole)
                        if table is not None:
                            result.append(table)
        return result

    # ------------------------------------------------------------------
    # Lazy column loading
    # ------------------------------------------------------------------

    def _on_item_expanded(self, item: QTreeWidgetItem) -> None:
        """Lazy-load column children when a table node is expanded."""
        if item.childCount() != 1:
            return
        first_child = item.child(0)
        if first_child is None or first_child.text(0) != self._PLACEHOLDER_TEXT:
            return

        table = item.data(0, Qt.ItemDataRole.UserRole)
        if table is None:
            return

        item.removeChild(first_child)
        for column in table.columns:
            item.addChild(
                QTreeWidgetItem(
                    [
                        column.name,
                        format_column_type(column),
                        "YES" if column.is_nullable else "NO",
                        column.column_default or "",
                        str(column.ordinal_position),
                    ]
                )
            )
