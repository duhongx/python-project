#!/usr/bin/env python3
"""
数据库结构同步工具 - PyQt6 桌面版 V2
真正符合需求的实现：左右两栏、+/-展开、单选框
"""

import sys
import yaml
import psycopg2
from pathlib import Path
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTreeWidget, QTreeWidgetItem, QRadioButton, QCheckBox,
    QPushButton, QLabel, QSplitter, QFrame, QLineEdit, QTextEdit
)
from PyQt6.QtCore import Qt, QSize
from PyQt6.QtGui import QIcon, QCursor


class DatabaseTree(QTreeWidget):
    """数据库树 - 支持 +/- 展开、单选框"""

    def __init__(self, side, parent=None):
        super().__init__(parent)
        self.side = side
        self.conn = None
        self.config = None

        # 设置树形控件
        self.setHeaderHidden(True)
        self.setIndentation(15)
        self.setRootIsDecorated(False)
        self.setExpandsOnDoubleClick(False)
        self.setFrameShape(QFrame.Shape.StyledPanel)

        # 单选框组
        self.radio_groups = {'database': [], 'schema': [], 'table': []}

    def load_config(self, config):
        """加载配置"""
        self.config = config

    def load_data(self):
        """加载数据"""
        self.clear()
        self.radio_groups = {'database': [], 'schema': [], 'table': []}

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
            error_item = QTreeWidgetItem(self)
            error_item.setText(0, f"❌ 连接失败: {e}")
            error_item.setForeground(0, QColor("red"))
            return

        # 创建 Database 节点
        db_name = cfg['database']
        db_item = DatabaseNode(self, 'database', db_name)
        self.addTopLevelItem(db_item)

        # 加载 Schemas
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT n.nspname
                FROM pg_namespace n
                WHERE pg_get_userbyid(n.nspowner) LIKE 'df_%'
                ORDER BY n.nspname
            """)
            schemas = [row[0] for row in cur.fetchall()]

        for schema in schemas:
            schema_item = DatabaseNode(self, 'schema', schema, db_item, self)
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
                table_item = DatabaseNode(self, 'table' if table_type == 'BASE TABLE' else 'view',
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
                    field_item = FieldNode(self, col, table_item)
                    table_item.addChild(field_item)

    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()


class DatabaseNode(QTreeWidgetItem):
    """数据库/Schema/表节点 - 带 +/- 和单选框"""

    def __init__(self, tree, node_type, name, parent=None):
        self.tree_widget = tree
        self.node_type = node_type
        self.name = name
        self.children_loaded = False

        # 设置文本
        icon_map = {'database': '🗄️ ', 'schema': '📂 ', 'table': '📋 ', 'view': '👁️ '}
        icon = icon_map.get(node_type, '📄 ')
        self.setText(0, f"{icon} {name}")

        # 创建单选框
        self.radio = QRadioButton()
        self.radio.setProperty('node_type', node_type)
        self.radio.setProperty('node_name', name)
        self.radio.toggled.connect(lambda: self.on_radio_toggled())

        # 添加到单选框组
        tree_widget.radio_groups[node_type].append(self.radio)

        # 设置为第一列的 widget
        self.tree_widget.setItemWidget(self, 0, self.create_widget())

    def create_widget(self):
        """创建包含 +/- 和单选框的 widget"""
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 2, 0, 2)
        layout.setSpacing(5)

        # 展开/收起按钮
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
            QPushButton:hover {
                background: #e0e0e0;
            }
        """)
        toggle_btn.clicked.connect(self.toggle_children)
        self.toggle_btn = toggle_btn

        # 单选框
        layout.addWidget(toggle_btn)
        layout.addWidget(self.radio)
        layout.addStretch()

        return widget

    def toggle_children(self):
        """切换子节点展开/收起"""
        if self.childCount() == 0:
            return

        is_expanded = self.isExpanded()
        if is_expanded:
            self.setExpanded(False)
            self.toggle_btn.setText('+')
        else:
            self.setExpanded(True)
            self.toggle_btn.setText('-')

    def on_radio_toggled(self):
        """单选框切换"""
        if not self.radio.isChecked():
            return

        # 清除同类型的其他选择
        for radio in self.tree_widget.radio_groups[self.node_type]:
            if radio != self.radio:
                radio.blockSignals(True)
                radio.setChecked(False)
                radio.blockSignals(False)

        # 通知主窗口
        if hasattr(self.tree_widget, 'on_node_selected'):
            self.tree_widget.on_node_selected(self.tree_widget.side,
                                                 self.node_type,
                                                 self.name)

    def get_full_path(self):
        """获取完整路径"""
        path = [self.name]
        parent = self.parent()
        while parent and isinstance(parent, DatabaseNode):
            path.insert(0, parent.name)
            parent = parent.parent()
        return '.'.join(path)


class FieldNode(QTreeWidgetItem):
    """字段节点 - 带同步复选框"""

    def __init__(self, tree, col_data, parent):
        super().__init__(parent)
        self.col_data = col_data
        self.tree_widget = tree
        self.schema = None
        self.table = None

        # 获取 schema 和 table
        parent_node = parent
        while parent_node and isinstance(parent_node, DatabaseNode):
            if parent_node.node_type == 'schema':
                self.schema = parent_node.name
            elif parent_node.node_type in ['table', 'view']:
                self.table = parent_node.name
            parent_node = parent_node.parent()

        # 设置文本
        col_name, col_type, length, nullable, default_val = col_data
        if length:
            col_type = f"{col_type}({length})"
        self.setText(0, f"🔹 {col_name}")
        self.setText(1, col_type)
        self.setText(2, nullable)

        # 源库字段添加同步复选框
        if tree.side == 'source':
            self.sync_checkbox = QCheckBox("同步")
            self.sync_checkbox.setFocusPolicy(Qt.FocusPolicy.NoFocus)
            self.sync_checkbox.stateChanged.connect(lambda: self.on_sync_toggled())
            self.tree_widget.setItemWidget(self, 0, self.create_sync_widget())
        else:
            self.tree_widget.setItemWidget(self, 0, QWidget())

    def create_sync_widget(self):
        """创建带复选框的 widget"""
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 2, 0, 2)
        layout.setSpacing(5)

        # 文本标签
        label = QLabel(self.text(0))
        layout.addWidget(label)

        # 同步复选框
        layout.addWidget(self.sync_checkbox)
        layout.addStretch()

        return widget

    def on_sync_toggled(self):
        """同步复选框切换"""
        if hasattr(self.tree_widget, 'on_sync_toggled'):
            sync_id = f"{self.schema}:{self.table}:{self.col_data[0]}"
            self.tree_widget.on_sync_toggled(sync_id, self.sync_checkbox.isChecked())


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
        left_title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px; background: #f8f9fa; border-radius: 4px;")
        left_layout.addWidget(left_title)

        self.source_tree = DatabaseTree('source')
        self.source_tree.on_node_selected = self.on_source_selected
        self.source_tree.on_sync_toggled = self.on_sync_toggled
        left_layout.addWidget(self.source_tree)

        content.addWidget(left_panel)

        # 右侧面板 - 目标数据库
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_title = QLabel("📤 目标数据库 (KingBase)")
        right_title.setStyleSheet("font-size: 16px; font-weight: bold; padding: 10px; background: #f8f9fa; border-radius: 4px;")
        right_layout.addWidget(right_title)

        self.target_tree = DatabaseTree('target')
        self.target_tree.on_node_selected = self.on_target_selected
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
        self.source_tree.load_config(self.config)
        self.target_tree.load_config(self.config)
        self.refresh_data()

    def refresh_data(self):
        """刷新数据"""
        self.source_tree.load_data()
        self.target_tree.load_data()

    def on_source_selected(self, side, node_type, name):
        """源库选择事件"""
        if self.auto_expand_cb.isChecked():
            # 联动展开目标库
            self.auto_expand_target(name)

    def on_target_selected(self, side, node_type, name):
        """目标库选择事件"""
        print(f"{side} 选择了 {node_type}: {name}")

    def on_sync_toggled(self, sync_id, checked):
        """同步复选框切换"""
        if checked:
            self.items_to_sync.add(sync_id)
        else:
            self.items_to_sync.discard(sync_id)

        self.sync_label.setText(f"已选择: {len(self.items_to_sync)} 个字段")

    def auto_expand_target(self, name):
        """联动展开目标库"""
        # 这里实现联动逻辑
        pass

    def clear_selection(self):
        """清空选择"""
        self.items_to_sync.clear()
        self.sync_label.setText("已选择: 0 个字段")

    def generate_sql(self):
        """生成 SQL"""
        if not self.items_to_sync:
            QMessageBox.warning(self, "提示", "请先选择要同步的字段")
            return

        sql_preview = QTextEdit()
        sql_preview.setReadOnly(True)
        sql_preview.setMaximumHeight(300)
        sql_preview.setStyleSheet("font-family: monospace; font-size: 12px;")

        sql = "-- 将要执行的 SQL\n"
        sql += f"-- 共 {len(self.items_to_sync)} 个字段\n\n"

        for sync_id in sorted(self.items_to_sync):
            schema, table, field = sync_id.split(':')
            sql += f"ALTER TABLE {schema}.{table} ADD COLUMN {field} type;\n"

        sql_preview.setText(sql)

        dialog = QDialog(self)
        dialog.setWindowTitle("SQL 预览")
        dialog.resize(800, 400)

        dialog_layout = QVBoxLayout(dialog)
        dialog_layout.addWidget(QLabel("📄 生成的 SQL:"))
        dialog_layout.addWidget(sql_preview)

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        cancel_btn = QPushButton("取消")
        cancel_btn.clicked.connect(dialog.reject)
        btn_layout.addWidget(cancel_btn)

        confirm_btn = QPushButton("确认执行")
        confirm_btn.clicked.connect(dialog.accept)
        btn_layout.addWidget(confirm_btn)

        dialog_layout.addLayout(btn_layout)
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
