#!/usr/bin/env python3
"""
数据库结构同步工具 - PyQt6 桌面版 V3
真正的树形结构：左右两栏、+/- 展开、单选框
"""

import sys
import yaml
import psycopg2
from pathlib import Path
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTreeWidget, QTreeWidgetItem, QRadioButton, QCheckBox,
    QPushButton, QLabel, QSplitter, QFrame, QDialog, QTextEdit
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor


class TreeWidgetWithWidgets(QTreeWidget):
    """支持自定义 widget 的树形控件"""

    nodeSelected = pyqtSignal(str, str, str)  # side, node_type, name
    itemChecked = pyqtSignal(str, bool)  # item_id, checked

    def __init__(self, side, parent=None):
        super().__init__(parent)
        self.side = side
        self.conn = None
        self.config = None
        self.radio_buttons = []
        self.checkboxes = {}

    def load_config(self, config):
        """加载配置"""
        self.config = config

    def load_data(self):
        """加载数据"""
        self.clear()
        self.radio_buttons = []
        self.checkboxes = {}

        # 连接数据库
        try:
            if self.side == 'source':
                cfg = self.config['postgresql']
            else:
                cfg = self.config['kingbase']

            self.conn = psycopg2.connect(
                host=cfg['host'],
                port=cfg['port'],
                database=cfg['database'],
                user=cfg['user'],
                password=cfg['password'],
                client_encoding='utf8'
            )
        except Exception as e:
            self.add_error_item(f"连接失败: {e}")
            return

        # 创建 Database 节点
        db_name = cfg['database']
        db_item = self.create_node('database', db_name, None)
        self.addTopLevelItem(db_item)

        # 加载 Schemas
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT n.nspname
                FROM pg_namespace n
                WHERE pg_getuserbyid(n.nspowner) LIKE 'df_%'
                ORDER BY n.nspname
            """)
            schemas = [row[0] for row in cur.fetchall()]

        for schema in schemas:
            schema_item = self.create_node('schema', schema, db_item)
            db_item.addChild(schema_item)

            # 加载 Tables
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_type IN ('BASE TABLE', 'VIEW')
                    ORDER BY table_name
                """, (schema,))
                tables = [(row[0], row[1]) for row in cur.fetchall()]

            for table_name, table_type in tables:
                table_item = self.create_node('table' if table_type == 'BASE TABLE' else 'view',
                                             table_name, schema_item)
                schema_item.addChild(table_item)

                # 加载 Fields
                with self.conn.cursor() as cur:
                    cur.execute("""
                        SELECT column_name, data_type, character_maximum_length,
                               is_nullable, column_default
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                    """, (schema, table_name))
                    columns = cur.fetchall()

                for col in columns:
                    self.create_field_node(col, table_item)

    def create_node(self, node_type, name, parent):
        """创建节点（带 +/- 和单选框）"""
        item = QTreeWidgetItem(parent)
        item.setText(0, f"📄 {name}") if node_type == 'database' else
                    f"📂 {name}" if node_type == 'schema' else
                    f"📋 {name}" if node_type == 'table' else
                    f"👁️ {name}")

        item.setData(0, Qt.ItemDataRole.UserRole, {
            'type': node_type,
            'name': name
        })

        # 创建 +/- 按钮
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 2, 0, 2)
        layout.setSpacing(5)

        toggle_btn = QPushButton('+')
        toggle_btn.setFixedSize(20, 20)
        toggle_btn.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        toggle_btn.setStyleSheet("""
            QPushButton {
                border: 1px solid #ccc;
                border-radius: 3px;
                background: #f0f0f0;
                font-weight: bold;
                font-size: 14px;
            }
        """)

        # 展开/收起
        has_children = item.childCount() > 0
        if has_children:
            toggle_btn.setText('-')
            toggle_btn.clicked.connect(lambda checked, s=self, i=item: self.toggle_item(s, i, toggle_btn))

        layout.addWidget(toggle_btn)

        # 单选框（Database/Schema/Table）
        if node_type in ['database', 'schema', 'table', 'view']:
            radio = QRadioButton()
            radio.setProperty('node_type', node_type)
            radio.setProperty('node_name', name)
            radio.toggled.connect(lambda checked: self.on_radio_toggled(radio, item, node_type, name))
            self.radio_buttons.append(radio)
            layout.addWidget(radio)

        # 同步复选框（仅源库字段）
        if self.side == 'source' and node_type == 'field':
            checkbox = QCheckBox("同步")
            checkbox.setFocusPolicy(Qt.FocusPolicy.NoFocus)
            checkbox.stateChanged.connect(lambda state: self.on_sync_toggled(checkbox, item))
            layout.addWidget(checkbox)

        layout.addStretch()

        self.setItemWidget(item, 0, widget)

        return item

    def create_field_node(self, col, parent):
        """创建字段节点"""
        item = QTreeWidgetItem(parent)
        col_name, col_type, length, nullable, default_val = col

        if length:
            col_type = f"{col_type}({length})"

        item.setText(0, f"🔹 {col_name}")
        item.setText(1, col_type)
        item.setText(2, nullable)

        item.setData(0, Qt.ItemDataRole.UserRole, {
            'type': 'field',
            'name': col_name,
            'data': col
        })

        # 源库字段添加同步复选框
        if self.side == 'source':
            widget = QWidget()
            layout = QHBoxLayout(widget)
            layout.setContentsMargins(0, 2, 0, 2)
            layout.setSpacing(5)

            label = QLabel(f"🔹 {col_name}")
            layout.addWidget(label)

            checkbox = QCheckBox("同步")
            checkbox.setFocusPolicy(Qt.FocusPolicy.NoFocus)
            sync_id = f"{parent.data(0, Qt.ItemDataRole.UserRole)['name']}:parent.data(0, Qt.ItemDataRole.UserRole)['name']}:{col_name}"
            checkbox.setProperty('sync_id', sync_id)
            checkbox.stateChanged.connect(lambda state: self.on_sync_toggled(checkbox, item))
            layout.addWidget(checkbox)

            layout.addStretch()

            self.setItemWidget(item, 0, widget)

    def toggle_item(self, tree_widget, item, button):
        """切换展开/收起"""
        is_expanded = item.isExpanded()
        if is_expanded:
            item.setExpanded(False)
            button.setText('+')
        else:
            item.setExpanded(True)
            button.setText('-')

        # 如果开启了联动，通知另一边
        if hasattr(tree_widget, 'auto_expand_to_other'):
            tree_widget.auto_expand_to_other(item)

    def on_radio_toggled(self, radio, item, node_type, name):
        """单选框切换"""
        if not radio.isChecked():
            return

        # 清除同类型的其他选择
        for rb in self.radio_buttons:
            if rb != radio and rb.property('node_type') == node_type:
                rb.blockSignals(True)
                rb.setChecked(False)
                rb.blockSignals(False)

        self.nodeSelected.emit(self.side, node_type, name)

    def on_sync_toggled(self, checkbox, item):
        """同步复选框切换"""
        checked = checkbox.isChecked()
        sync_id = checkbox.property('sync_id')
        self.itemChecked.emit(sync_id, checked)

    def add_error_item(self, message):
        """添加错误项"""
        item = QTreeWidgetItem(self)
        item.setText(0, f"❌ {message}")
        item.setForeground(0, QColor("red"))


class MainWindow(QMainWindow):
    """主窗口"""

    def __init__(self):
        super().__init__()
        self.config = self.load_config()
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
        self.setGeometry(100, 100, 1600, 900)

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # 标题
        title = QLabel("🔄 数据库结构同步工具")
        title.setStyleSheet("""
            font-size: 24px;
            font-weight: bold;
            padding: 15px;
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #1f77b4, stop:1 #17a2b8);
            color: white;
            border-radius: 8px;
        """)
        layout.addWidget(title)

        # 工具栏
        toolbar = QHBoxLayout()

        self.auto_expand_cb = QCheckBox("🔄 联动展开")
        self.auto_expand_cb.setChecked(True)
        toolbar.addWidget(self.auto_expand_cb)

        toolbar.addStretch()

        refresh_btn = QPushButton("🔄 刷新")
        refresh_btn.clicked.connect(self.refresh_data)
        toolbar.addWidget(refresh_btn)

        layout.addLayout(toolbar)

        # 主内容区 - 左右两栏
        content = QSplitter(Qt.Orientation.Horizontal)

        # 左侧面板 - 源数据库
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_title = QLabel("📥 源数据库 (PostgreSQL)")
        left_title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px; background: #e3f2fd; border-radius: 4px;")
        left_layout.addWidget(left_title)

        self.source_tree = TreeWidgetWithWidgets('source')
        self.source_tree.load_config(self.config)
        self.source_tree.nodeSelected.connect(self.on_source_selected)
        self.source_tree.itemChecked.connect(self.on_item_checked)
        left_layout.addWidget(self.source_tree)

        content.addWidget(left_panel)

        # 右侧面板 - 目标数据库
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_title = QLabel("📤 目标数据库 (KingBase)")
        right_title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px; background: #f8f9fa; border-radius: 4px;")
        right_layout.addWidget(right_title)

        self.target_tree = TreeWidgetWithWidgets('target')
        self.target_tree.load_config(self.config)
        self.target_tree.nodeSelected.connect(self.on_target_selected)
        right_layout.addWidget(self.target_tree)

        content.addWidget(right_panel)

        content.setStretchFactor(0, 1)
        content.setStretchFactor(1, 1)
        layout.addWidget(content)

        # 底部状态栏
        status_bar = QHBoxLayout()

        self.sync_label = QLabel("已选择: 0 个字段")
        status_bar.addWidget(self.sync_label)

        status_bar.addStretch()

        clear_btn = QPushButton("🗑️ 清空")
        clear_btn.clicked.connect(self.clear_selection)
        status_bar.addWidget(clear_btn)

        generate_btn = QPushButton("📄 生成 SQL")
        generate_btn.clicked.connect(self.generate_sql)
        status_bar.addWidget(generate_btn)

        sync_btn = QPushButton("🚀 执行同步")
        sync_btn.clicked.connect(self.execute_sync)
        status_bar.addWidget(sync_btn)

        layout.addLayout(status_bar)

        # 加载数据
        self.refresh_data()

    def refresh_data(self):
        """刷新数据"""
        self.source_tree.load_data()
        self.target_tree.load_data()

    def on_source_selected(self, side, node_type, name):
        """源库选择事件"""
        print(f"源库选择了: {node_type} - {name}")

    def on_target_selected(self, side, node_type, name):
        """目标库选择事件"""
        print(f"目标库选择了: {node_type} - {name}")

    def on_item_checked(self, sync_id, checked):
        """同步复选框切换"""
        if checked:
            self.items_to_sync.add(sync_id)
        else:
            self.items_to_sync.discard(sync_id)

        self.sync_label.setText(f"已选择: {len(self.items_to_sync)} 个字段")

    def clear_selection(self):
        """清空选择"""
        self.items_to_sync.clear()
        self.sync_label.setText("已选择: 0 个字段")

    def generate_sql(self):
        """生成 SQL"""
        if not self.items_to_sync:
            QMessageBox.warning(self, "提示", "请先选择要同步的字段")
            return

        dialog = QDialog(self)
        dialog.setWindowTitle("SQL 预览")
        dialog.resize(800, 500)

        layout = QVBoxLayout(dialog)

        sql_text = QTextEdit()
        sql_text.setReadOnly(True)
        sql_text.setStyleSheet("font-family: monospace; font-size: 12px;")

        sql = "-- 将要执行的 SQL\n"
        sql += f"-- 共 {len(self.items_to_sync)} 个字段\n\n"

        for sync_id in sorted(self.items_to_sync):
            schema, table, field = sync_id.split(':')
            sql += f"ALTER TABLE {schema}.{table} ADD COLUMN {field} type;\n"

        sql_text.setPlainText(sql)

        layout.addWidget(QLabel("📄 生成的 SQL:"))
        layout.addWidget(sql_text)

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        cancel_btn = QPushButton("取消")
        cancel_btn.clicked.connect(dialog.reject)
        btn_layout.addWidget(cancel_btn)

        confirm_btn = QPushButton("确认执行")
        confirm_btn.clicked.connect(dialog.accept)
        btn_layout.addWidget(confirm_btn)

        layout.addLayout(btn_layout)
        dialog.exec()

    def execute_sync(self):
        """执行同步"""
        if not self.items_to_sync:
            QMessageBox.warning(self, "提示", "请先选择要同步的字段")
            return

        QMessageBox.information(self, "提示", f"将同步 {len(self.items_to_sync)} 个字段")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
