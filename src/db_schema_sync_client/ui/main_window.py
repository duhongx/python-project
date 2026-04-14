"""Main application window shell."""

from __future__ import annotations

from typing import Optional

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QPushButton,
    QStackedWidget,
    QStatusBar,
    QVBoxLayout,
    QWidget,
)

from db_schema_sync_client.domain.diff import SchemaDiff
from db_schema_sync_client.domain.models import ConnectionProfile
from db_schema_sync_client.infrastructure.app_store import AppStore
from db_schema_sync_client.services.sql_generator import GeneratedSqlPlan
from db_schema_sync_client.ui.structure_sync_page import StructureSyncPage


class _PlaceholderPage(QWidget):
    def __init__(self, title: str, parent=None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 24, 24, 24)
        label = QLabel(title)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        label.setStyleSheet("color: #6c757d; font-size: 14px;")
        layout.addStretch()
        layout.addWidget(label)
        layout.addStretch()

    def refresh(self) -> None:
        return


class MainWindow(QMainWindow):
    def __init__(self, app_store: Optional[AppStore] = None, parent=None) -> None:
        super().__init__(parent)
        self.app_store = app_store

        self.setWindowTitle("数据库结构同步客户端")
        self.resize(1280, 820)

        self._build_ui()
        self._register_pages()
        self.navigation_list.setCurrentRow(0)

    def _build_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)

        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)

        top_bar = QWidget()
        top_bar.setStyleSheet("background-color: #f8fafc; border-bottom: 1px solid #d0d7de;")
        top_layout = QHBoxLayout(top_bar)
        top_layout.setContentsMargins(16, 12, 16, 12)
        top_layout.setSpacing(12)

        title = QLabel("数据库结构同步客户端")
        title.setStyleSheet("font-size: 16px; font-weight: 600; color: #1f2937;")
        top_layout.addWidget(title)
        top_layout.addStretch()

        env_label = QLabel("环境: DEV")
        env_label.setStyleSheet("color: #475569;")
        top_layout.addWidget(env_label)

        config_button = QPushButton("连接配置")
        config_button.clicked.connect(self._open_config_dialog)
        top_layout.addWidget(config_button)

        history_button = QPushButton("历史记录")
        history_button.clicked.connect(self._open_history_dialog)
        top_layout.addWidget(history_button)

        refresh_button = QPushButton("刷新")
        refresh_button.clicked.connect(self._refresh_current_page)
        top_layout.addWidget(refresh_button)

        exit_button = QPushButton("退出")
        exit_button.clicked.connect(self.close)
        top_layout.addWidget(exit_button)

        root_layout.addWidget(top_bar)

        body = QWidget()
        body_layout = QHBoxLayout(body)
        body_layout.setContentsMargins(0, 0, 0, 0)
        body_layout.setSpacing(0)

        navigation = QWidget()
        navigation.setFixedWidth(220)
        navigation.setStyleSheet("background-color: #f4f7fb; border-right: 1px solid #d0d7de;")
        navigation_layout = QVBoxLayout(navigation)
        navigation_layout.setContentsMargins(12, 16, 12, 16)
        navigation_layout.setSpacing(12)

        nav_title = QLabel("导航")
        nav_title.setStyleSheet("font-size: 13px; font-weight: 600; color: #334155;")
        navigation_layout.addWidget(nav_title)

        self.navigation_list = QListWidget()
        self.navigation_list.setObjectName("navigation_list")
        self.navigation_list.setSpacing(4)
        self.navigation_list.currentRowChanged.connect(self._switch_page)
        navigation_layout.addWidget(self.navigation_list, 1)

        body_layout.addWidget(navigation)

        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(16, 12, 16, 12)
        content_layout.setSpacing(8)

        self.breadcrumb_label = QLabel("结构同步 / 结构比对")
        self.breadcrumb_label.setStyleSheet("color: #64748b; font-size: 12px;")
        content_layout.addWidget(self.breadcrumb_label)

        self.page_title_label = QLabel("结构同步")
        self.page_title_label.setStyleSheet("font-size: 18px; font-weight: 600; color: #0f172a;")
        content_layout.addWidget(self.page_title_label)

        self.page_stack = QStackedWidget()
        self.page_stack.setObjectName("page_stack")
        content_layout.addWidget(self.page_stack, 1)

        body_layout.addWidget(content, 1)
        root_layout.addWidget(body, 1)

        self.setStatusBar(QStatusBar())

    def _register_pages(self) -> None:
        self.structure_sync_page = StructureSyncPage(self.app_store, parent=self)
        self.cluster_page = _PlaceholderPage("集群管理页面开发中", parent=self)
        self.history_page = _PlaceholderPage("历史与审计页面开发中", parent=self)
        self.settings_page = _PlaceholderPage("系统设置页面开发中", parent=self)

        self._pages = [
            ("结构同步", "结构同步 / 结构比对", self.structure_sync_page),
            ("集群管理", "集群管理 / 集群列表", self.cluster_page),
            ("历史与审计", "历史与审计", self.history_page),
            ("系统设置", "系统设置", self.settings_page),
        ]

        for label, _, page in self._pages:
            self.navigation_list.addItem(QListWidgetItem(label))
            self.page_stack.addWidget(page)

    def _switch_page(self, index: int) -> None:
        if index < 0 or index >= len(self._pages):
            return

        label, breadcrumb, page = self._pages[index]
        self.page_stack.setCurrentWidget(page)
        self.page_title_label.setText(label)
        self.breadcrumb_label.setText(breadcrumb)

    def _refresh_current_page(self) -> None:
        page = self.page_stack.currentWidget()
        if page is None:
            return

        refresh = getattr(page, "refresh", None)
        if callable(refresh):
            refresh()

    def _open_config_dialog(self) -> None:
        self.structure_sync_page._open_config_dialog()

    def _open_history_dialog(self) -> None:
        self.structure_sync_page._open_history_dialog()

    def _refresh_profile_combos(self) -> None:
        self.structure_sync_page._refresh_profile_combos()

    def _run_comparison(self) -> None:
        self.structure_sync_page._run_comparison()

    @property
    def comparison_panel(self):
        return self.structure_sync_page.comparison_panel

    @property
    def current_diff(self) -> Optional[SchemaDiff]:
        return self.structure_sync_page.current_diff

    @current_diff.setter
    def current_diff(self, value: Optional[SchemaDiff]) -> None:
        self.structure_sync_page.current_diff = value

    @property
    def current_target_profile(self) -> Optional[ConnectionProfile]:
        return self.structure_sync_page.current_target_profile

    @current_target_profile.setter
    def current_target_profile(self, value: Optional[ConnectionProfile]) -> None:
        self.structure_sync_page.current_target_profile = value

    @property
    def current_source_profile(self) -> Optional[ConnectionProfile]:
        return self.structure_sync_page.current_source_profile

    @current_source_profile.setter
    def current_source_profile(self, value: Optional[ConnectionProfile]) -> None:
        self.structure_sync_page.current_source_profile = value

    def generate_sql_plan_for_selected(self) -> GeneratedSqlPlan:
        return self.structure_sync_page.generate_sql_plan_for_selected()
