#!/usr/bin/env python3
"""桌面应用 - 简化测试版"""

import sys
import yaml
import psycopg2
from pathlib import Path
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout,
    QTreeWidget, QTreeWidgetItem, QLabel, QPushButton, QHBoxLayout
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor


class SimpleTreeApp(QMainWindow):
    """简化的树形应用"""

    def __init__(self):
        super().__init__()
        self.config = self.load_config()
        self.setup_ui()

    def load_config(self):
        config_path = Path(__file__).parent / "config.yaml"
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def setup_ui(self):
        self.setWindowTitle("数据库结构同步工具")
        self.setGeometry(100, 100, 1400, 800)

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # 标题
        title = QLabel("🔄 数据库结构同步工具")
        title.setStyleSheet("font-size: 20px; font-weight: bold; padding: 10px;")
        layout.addWidget(title)

        # 树形控件
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["对象", "类型", "状态"])
        self.tree.setColumnWidth(0, 400)
        self.tree.setColumnWidth(1, 150)
        layout.addWidget(self.tree)

        # 添加测试数据
        self.load_test_data()

        # 底部按钮
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        refresh_btn = QPushButton("🔄 刷新")
        refresh_btn.clicked.connect(self.load_test_data)
        btn_layout.addWidget(refresh_btn)

        quit_btn = QPushButton("✕ 退出")
        quit_btn.clicked.connect(self.close)
        btn_layout.addWidget(quit_btn)

        layout.addLayout(btn_layout)

    def load_test_data(self):
        """加载测试数据"""
        self.tree.clear()

        # 模拟树形数据
        databases = ["df_his"]
        schemas = ["df_authorization", "df_bingangl", "df_bingli"]

        for db in databases:
            db_item = QTreeWidgetItem(self.tree)
            db_item.setText(0, f"🗄️ {db}")
            db_item.setText(1, "Database")

            for schema in schemas:
                schema_item = QTreeWidgetItem(db_item)
                schema_item.setText(0, f"📂 {schema}")
                schema_item.setText(1, "Schema")

                # 添加一些表
                tables = [
                    ("ba_bingrenjbxx", "table", 2),
                    ("ba_jk_bingrenjbxx", "table", 3),
                    ("v_hqms", "view", 4),
                ]

                for table_name, t_type, num_fields in tables:
                    icon = "📋" if t_type == "table" else "👁️"
                    table_item = QTreeWidgetItem(schema_item)
                    table_item.setText(0, f"{icon} {table_name}")
                    table_item.setText(1, "Table" if t_type == "table" else "View")
                    table_item.setText(2, f"{num_fields} 字段")

                    # 添加一些字段
                    fields = [
                        ("id", "bigint", "NO"),
                        ("name", "varchar(100)", "YES"),
                        ("tenantid", "varchar(20)", "YES"),
                    ]

                    for field_name, f_type, nullable in fields:
                        field_item = QTreeWidgetItem(table_item)
                        field_item.setText(0, f"🔹 {field_name}")
                        field_item.setText(1, f_type)
                        field_item.setText(2, nullable)

                        # 设置颜色
                        if field_name == "tenantid":
                            field_item.setForeground(0, QColor("#17a2b8"))

                    schema_item.addChild(table_item)

                db_item.addChild(schema_item)

            self.tree.addTopLevelItem(db_item)

        # 默认展开
        self.tree.expandToDepth(2)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SimpleTreeApp()
    window.show()
    sys.exit(app.exec())
