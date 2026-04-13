#!/usr/bin/env python3
"""
数据库结构同步工具 - PyQt6 桌面版
实现真正的树形结构：+/- 展开、单选框、联动对比
"""

import sys
import yaml
import psycopg2
from pathlib import Path
from collections import defaultdict
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTreeWidget, QTreeWidgetItem, QCheckBox, QPushButton, QLabel,
    QSplitter, QTextEdit, QMessageBox, QProgressBar, QDialog,
    QTableWidget, QTableWidgetItem, QHeaderView, QRadioButton
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QIcon, QColor


# ============= 数据库操作 =============
class DatabaseManager:
    """数据库管理器"""

    def __init__(self, config):
        self.config = config

    def get_connection(self, db_type):
        """获取数据库连接"""
        try:
            if db_type == 'source':
                cfg = self.config['postgresql']
            else:
                cfg = self.config['kingbase']

            conn = psycopg2.connect(
                host=cfg['host'],
                port=cfg['port'],
                database=cfg['database'],
                user=cfg['user'],
                password=cfg['password'],
                client_encoding='utf8'
            )
            return conn, None
        except Exception as e:
            return None, str(e)

    def get_schemas(self, conn):
        """获取 schema 列表"""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT n.nspname
                FROM pg_namespace n
                WHERE pg_get_userbyid(n.nspowner) LIKE 'df_%'
                ORDER BY n.nspname
            """)
            return [row[0] for row in cur.fetchall()]

    def get_tables(self, conn, schema):
        """获取表列表"""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type IN ('BASE TABLE', 'VIEW')
                ORDER BY table_name
            """, (schema,))
            return [(row[0], row[1]) for row in cur.fetchall()]

    def get_columns(self, conn, schema, table):
        """获取字段列表"""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, character_maximum_length,
                       numeric_precision, numeric_scale, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table))
            columns = []
            for row in cur.fetchall():
                col = {
                    'name': row[0],
                    'type': row[1],
                    'length': row[2],
                    'precision': row[3],
                    'scale': row[4],
                    'nullable': row[5],
                    'default': row[6]
                }
                col['type_str'] = self.format_type(col)
                columns.append(col)
            return columns

    def format_type(self, col):
        """格式化类型"""
        t = col['type']
        if col['length']:
            return f"{t}({col['length']})"
        elif col['precision'] and col['scale']:
            return f"{t}({col['precision']},{col['scale']})"
        elif col['precision']:
            return f"{t}({col['precision']})"
        return t


# ============= 树形组件 =============
class DatabaseTreeWidget(QTreeWidget):
    """数据库树形组件"""

    nodeSelected = pyqtSignal(str, str, str)  # side, node_type, value
    itemChecked = pyqtSignal(str, bool)  # item_id, checked

    def __init__(self, side, db_manager, parent=None):
        super().__init__(parent)
        self.side = side
        self.db_manager = db_manager
        self.conn = None

        # 设置树形控件
        self.setHeaderLabels(["对象", "类型"])
        self.setAlternatingRowColors(True)
        self.setColumnWidth(0, 300)

        # 连接信号
        self.itemClicked.connect(self.on_item_clicked)
        self.itemChanged.connect(self.on_item_changed)

    def load_data(self):
        """加载数据"""
        # 清空树
        self.clear()

        # 连接数据库
        self.conn, error = self.db_manager.get_connection(self.side)
        if not self.conn:
            self.add_error_item(f"连接失败: {error}")
            return False

        # 添加根节点（Database）
        config = self.db_manager.config
        db_name = config['postgresql' if self.side == 'source' else 'kingbase']['database']

        db_item = QTreeWidgetItem(self)
        db_item.setText(0, f"🗄️ {db_name}")
        db_item.setText(1, "Database")
        db_item.setData(0, Qt.ItemDataRole.UserRole, {
            'type': 'database',
            'name': db_name,
            'id': f"{self.side}_db_{db_name}"
        })
        self.addTopLevelItem(db_item)

        # 加载 Schemas
        schemas = self.db_manager.get_schemas(self.conn)

        for schema in schemas:
            schema_item = QTreeWidgetItem(db_item)
            schema_item.setText(0, f"📂 {schema}")
            schema_item.setText(1, "Schema")
            schema_item.setData(0, Qt.ItemDataRole.UserRole, {
                'type': 'schema',
                'name': schema,
                'id': f"{self.side}_schema_{db_name}_{schema}"
            })

            # 添加单选框列
            self.add_radio_widget(schema_item)

            # 加载 Tables
            tables = self.db_manager.get_tables(self.conn, schema)

            for table_name, table_type in tables:
                icon = "📋" if table_type == "BASE TABLE" else "👁️"
                table_item = QTreeWidgetItem(schema_item)
                table_item.setText(0, f"{icon} {table_name}")
                table_item.setText(1, "Table" if table_type == "BASE TABLE" else "View")
                table_item.setData(0, Qt.ItemDataRole.UserRole, {
                    'type': 'table' if table_type == "BASE TABLE" else 'view',
                    'name': table_name,
                    'schema': schema,
                    'id': f"{self.side}_table_{db_name}_{schema}_{table_name}"
                })

                # 添加单选框
                self.add_radio_widget(table_item)

                # 预加载字段（延迟加载可以优化性能）
                columns = self.db_manager.get_columns(self.conn, schema, table_name)

                for col in columns:
                    field_item = QTreeWidgetItem(table_item)
                    field_item.setText(0, f"🔹 {col['name']}")
                    field_item.setText(1, col['type_str'])
                    field_item.setData(0, Qt.ItemDataRole.UserRole, {
                        'type': 'field',
                        'name': col['name'],
                        'schema': schema,
                        'table': table_name,
                        'data': col,
                        'id': f"{self.side}_field_{schema}_{table_name}_{col['name']}"
                    })

                    # 源库字段添加同步复选框
                    if self.side == 'source':
                        self.add_sync_widget(field_item, f"{schema}:{table_name}:{col['name']}")

            schema_item.addChild(table_item)

            db_item.addChild(schema_item)

        # 默认展开第一层
        db_item.setExpanded(True)

        return True

    def add_error_item(self, message):
        """添加错误项"""
        item = QTreeWidgetItem(self)
        item.setText(0, f"❌ {message}")
        item.setForeground(0, QColor("red"))

    def add_radio_widget(self, item):
        """添加单选框到树节点"""
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setAlignment(Qt.AlignmentFlag.AlignLeft)

        radio = QRadioButton()
        radio.setProperty("tree_item_id", item.data(0, Qt.ItemDataRole.UserRole)['id'])
        radio.toggled.connect(lambda checked: self.on_radio_toggled(radio, item, checked))

        layout.addWidget(radio)
        layout.addStretch()

        self.setItemWidget(item, 0, widget)

    def add_sync_widget(self, item, sync_id):
        """添加同步复选框到字段节点"""
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 10, 0)

        checkbox = QCheckBox("同步")
        checkbox.setProperty("sync_id", sync_id)
        checkbox.stateChanged.connect(lambda state: self.on_sync_toggled(checkbox, sync_id, state))

        layout.addWidget(checkbox)
        layout.addStretch()

        self.setItemWidget(item, 0, widget)

    def on_item_clicked(self, item, column):
        """节点点击事件"""
        data = item.data(0, Qt.ItemDataRole.UserRole)
        if not data:
            return

        self.nodeSelected.emit(
            self.side,
            data['type'],
            data.get('schema', '') + '.' + data.get('name', '')
        )

    def on_radio_toggled(self, radio, item, checked):
        """单选框切换"""
        if not checked:
            return

        data = item.data(0, Qt.ItemDataRole.UserRole)
        item_id = data['id']
        node_type = data['type']

        # 清除同类型的其他选择
        self.clear_radio_selection(item_id, node_type)

        self.nodeSelected.emit(
            self.side,
            node_type,
            data.get('schema', '') + '.' + data.get('name', '')
        )

    def clear_radio_selection(self, exclude_id, node_type):
        """清除同类型的其他选择"""
        root = self.invisibleRootItem()
        self._clear_radio_recursive(root, exclude_id, node_type)

    def _clear_radio_recursive(self, item, exclude_id, node_type):
        """递归清除单选选择"""
        for i in range(item.childCount()):
            child = item.child(i)
            data = child.data(0, Qt.ItemDataRole.UserRole)
            if data and data.get('type') == node_type:
                widget = self.itemWidget(child, 0)
                if widget:
                    radio = widget.findChild(QRadioButton)
                    if radio and radio.property("tree_item_id") != exclude_id:
                        radio.blockSignals(True)
                        radio.setChecked(False)
                        radio.blockSignals(False)

            self._clear_radio_recursive(child, exclude_id, node_type)

    def on_sync_toggled(self, checkbox, sync_id, state):
        """同步复选框切换"""
        checked = (state == 2)  # Qt.Checked = 2
        self.itemChecked.emit(sync_id, checked)

    def on_item_changed(self, item, column):
        """节点变化事件"""
        pass


# ============= 对比面板 =============
class ComparisonPanel(QWidget):
    """对比面板"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.source_tree = None
        self.target_tree = None
        self.setup_ui()

    def set_trees(self, source_tree, target_tree):
        self.source_tree = source_tree
        self.target_tree = target_tree

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # 标题
        title = QLabel("📊 对比结果")
        title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px;")
        layout.addWidget(title)

        # 统计卡片
        stats_layout = QHBoxLayout()

        self.stat_only_source = self.create_stat_card("🔵 仅源库", "0", "#17a2b8")
        self.stat_only_target = self.create_stat_card("🔴 仅目标库", "0", "#dc3545")
        self.stat_common = self.create_stat_card("🟢 共有", "0", "#28a745")

        stats_layout.addWidget(self.stat_only_source)
        stats_layout.addWidget(self.stat_only_target)
        stats_layout.addWidget(self.stat_common)

        layout.addLayout(stats_layout)

        # 对比详情表格
        self.comparison_table = QTableWidget()
        self.comparison_table.setColumnCount(4)
        self.comparison_table.setHorizontalHeaderLabels(["对象", "源库类型", "目标库类型", "状态"])
        self.comparison_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.comparison_table)

    def create_stat_card(self, title, value, color):
        """创建统计卡片"""
        card = QWidget()
        layout = QVBoxLayout(card)
        layout.setContentsMargins(10, 10, 10, 10)

        label_title = QLabel(title)
        label_value = QLabel(value)
        label_value.setStyleSheet(f"font-size: 24px; font-weight: bold; color: {color};")

        layout.addWidget(label_title)
        layout.addWidget(label_value)

        return card

    def update_comparison(self, source_schema, target_schema):
        """更新对比信息"""
        # 这里实现实际的对比逻辑
        pass


# ============= 主窗口 =============
class MainWindow(QMainWindow):
    """主窗口"""

    def __init__(self):
        super().__init__()
        self.config = self.load_config()
        self.db_manager = DatabaseManager(self.config)
        self.items_to_sync = set()
        self.setup_ui()

    def load_config(self):
        """加载配置"""
        config_path = Path(__file__).parent / "config.yaml"
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def setup_ui(self):
        """设置界面"""
        self.setWindowTitle("数据库结构同步工具")
        self.setGeometry(100, 100, 1400, 800)

        # 中心部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        main_layout = QVBoxLayout(central_widget)

        # 标题
        title = QLabel("🔄 数据库结构同步工具")
        title.setStyleSheet("""
            font-size: 20px;
            font-weight: bold;
            padding: 10px;
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 #1f77b4, stop:1 #17a2b8);
            color: white;
            border-radius: 8px;
        """)
        main_layout.addWidget(title)

        # 选项
        options_layout = QHBoxLayout()

        self.auto_expand_cb = QCheckBox("🔄 联动展开")
        self.auto_expand_cb.setChecked(True)
        options_layout.addWidget(self.auto_expand_cb)

        options_layout.addStretch()

        refresh_btn = QPushButton("🔄 刷新")
        refresh_btn.clicked.connect(self.refresh_trees)
        options_layout.addWidget(refresh_btn)

        main_layout.addLayout(options_layout)

        # 分割器（左右树形）
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # 源数据库树
        source_widget = QWidget()
        source_layout = QVBoxLayout(source_widget)
        source_title = QLabel("📥 源数据库 (PostgreSQL)")
        source_title.setStyleSheet("font-weight: bold; padding: 5px;")
        source_layout.addWidget(source_title)

        self.source_tree = DatabaseTreeWidget('source', self.db_manager)
        self.source_tree.nodeSelected.connect(self.on_source_selected)
        source_layout.addWidget(self.source_tree)

        splitter.addWidget(source_widget)

        # 对比面板
        self.comparison_panel = ComparisonPanel()
        self.comparison_panel.set_trees(self.source_tree, None)
        splitter.addWidget(self.comparison_panel)

        # 目标数据库树
        target_widget = QWidget()
        target_layout = QVBoxLayout(target_widget)
        target_title = QLabel("📤 目标数据库 (KingBase)")
        target_title.setStyleSheet("font-weight: bold; padding: 5px;")
        target_layout.addWidget(target_title)

        self.target_tree = DatabaseTreeWidget('target', self.db_manager)
        self.target_tree.nodeSelected.connect(self.on_target_selected)
        self.target_tree.itemChecked.connect(self.on_item_checked)
        target_layout.addWidget(self.target_tree)

        splitter.addWidget(target_widget)

        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 2)
        splitter.setStretchFactor(2, 1)

        main_layout.addWidget(splitter)

        # 底部操作栏
        bottom_layout = QHBoxLayout()

        self.sync_count_label = QLabel("已选择: 0 个字段")
        bottom_layout.addWidget(self.sync_count_label)

        bottom_layout.addStretch()

        generate_sql_btn = QPushButton("📄 生成 SQL")
        generate_sql_btn.clicked.connect(self.generate_sql)
        bottom_layout.addWidget(generate_sql_btn)

        clear_btn = QPushButton("🗑️ 清空选择")
        clear_btn.clicked.connect(self.clear_selection)
        bottom_layout.addWidget(clear_btn)

        sync_btn = QPushButton("🚀 执行同步")
        sync_btn.clicked.connect(self.execute_sync)
        bottom_layout.addWidget(sync_btn)

        main_layout.addLayout(bottom_layout)

        # 加载数据
        self.refresh_trees()

    def refresh_trees(self):
        """刷新树形数据"""
        self.source_tree.load_data()
        self.target_tree.load_data()

    def on_source_selected(self, side, node_type, value):
        """源库选择事件"""
        if self.auto_expand_cb.isChecked():
            # 联动展开目标库
            self.auto_expand_target(value)

    def on_target_selected(self, side, node_type, value):
        """目标库选择事件"""
        # 更新对比面板
        self.update_comparison()

    def on_item_checked(self, sync_id, checked):
        """同步复选框切换"""
        if checked:
            self.items_to_sync.add(sync_id)
        else:
            self.items_to_sync.discard(sync_id)

        self.sync_count_label.setText(f"已选择: {len(self.items_to_sync)} 个字段")

    def auto_expand_target(self, source_value):
        """联动展开目标库"""
        # 实现联动逻辑
        pass

    def update_comparison(self):
        """更新对比信息"""
        # 实现对比逻辑
        pass

    def generate_sql(self):
        """生成 SQL"""
        if not self.items_to_sync:
            QMessageBox.warning(self, "提示", "请先选择要同步的字段")
            return

        # 打开 SQL 预览对话框
        dialog = SQLPreviewDialog(list(self.items_to_sync), self.db_manager, self)
        dialog.exec()

    def clear_selection(self):
        """清空选择"""
        self.items_to_sync.clear()
        self.sync_count_label.setText("已选择: 0 个字段")

    def execute_sync(self):
        """执行同步"""
        if not self.items_to_sync:
            QMessageBox.warning(self, "提示", "请先选择要同步的字段")
            return

        # 执行同步逻辑
        QMessageBox.information(self, "提示", f"将同步 {len(self.items_to_sync)} 个字段")


# ============= SQL 预览对话框 =============
class SQLPreviewDialog(QDialog):
    """SQL 预览对话框"""

    def __init__(self, items, db_manager, parent=None):
        super().__init__(parent)
        self.items = items
        self.db_manager = db_manager
        self.setup_ui()

    def setup_ui(self):
        self.setWindowTitle("SQL 预览")
        self.setGeometry(200, 200, 800, 600)

        layout = QVBoxLayout(self)

        # SQL 文本框
        self.sql_text = QTextEdit()
        self.sql_text.setReadOnly(True)
        self.sql_text.setStyleSheet("font-family: monospace; font-size: 12px;")
        layout.addWidget(self.sql_text)

        # 生成 SQL
        sql = self.generate_sql()
        self.sql_text.setPlainText(sql)

        # 按钮
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        cancel_btn = QPushButton("取消")
        cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(cancel_btn)

        execute_btn = QPushButton("确认执行")
        execute_btn.clicked.connect(self.accept)
        button_layout.addWidget(execute_btn)

        layout.addLayout(button_layout)

    def generate_sql(self):
        """生成 SQL 语句"""
        sql_lines = ["-- 同步字段"]
        sql_lines.append(f"-- 共 {len(self.items)} 个字段")
        sql_lines.append("")

        for item in self.items:
            schema, table, field = item.split(':')
            sql_lines.append(f"-- {schema}.{table}.{field}")
            # 这里应该生成实际的 ALTER TABLE 语句
            sql_lines.append(f"ALTER TABLE {schema}.{table} ADD COLUMN {field} type;")
            sql_lines.append("")

        return "\n".join(sql_lines)


# ============= 入口 =============
def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
