"""Connection profile configuration dialog."""

from __future__ import annotations

from typing import Optional, Tuple

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QComboBox,
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QSplitter,
    QVBoxLayout,
    QWidget,
)

from db_schema_sync_client.domain.models import ConnectionProfile, ConnectionRole, DatabaseType


def validate_profile_inputs(
    *,
    role: ConnectionRole,
    name: str,
    db_type_value: str,
    host: str,
    port_text: str,
    database: str,
    username: str,
    password: str,
    existing_password: bool = False,
) -> Tuple[Optional[ConnectionProfile], Optional[str]]:
    """Validate form inputs and return a ConnectionProfile or an error message."""
    if not name.strip():
        return None, "Name is required"
    if not host.strip():
        return None, "Host is required"

    try:
        port = int(port_text)
    except (ValueError, TypeError):
        return None, "Port must be an integer"

    if not 1 <= port <= 65535:
        return None, "Port must be between 1 and 65535"

    if not database.strip():
        return None, "Database is required"
    if not username.strip():
        return None, "Username is required"

    if not password and not existing_password:
        return None, "Password is required"

    try:
        db_type = DatabaseType(db_type_value)
    except ValueError:
        return None, f"Unsupported database type: {db_type_value}"

    if role == ConnectionRole.SOURCE and db_type != DatabaseType.POSTGRESQL:
        return None, "Source must be PostgreSQL"
    if role == ConnectionRole.TARGET and db_type not in {DatabaseType.POSTGRESQL, DatabaseType.KINGBASE}:
        return None, "Target must be PostgreSQL or KingBase"

    profile = ConnectionProfile(
        name=name.strip(),
        role=role,
        db_type=db_type,
        host=host.strip(),
        port=port,
        database=database.strip(),
        username=username.strip(),
    )
    return profile, None


class ConnectionConfigDialog(QDialog):
    """Dialog for creating and editing database connection profiles."""

    def __init__(self, app_store, parent=None) -> None:
        super().__init__(parent)
        self.app_store = app_store
        self._editing_profile: Optional[ConnectionProfile] = None
        self._current_role: Optional[ConnectionRole] = None
        self.setWindowTitle("连接配置")
        self.setModal(True)
        self.resize(480, 420)
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()

        self.name_input = QLineEdit()
        form.addRow("名称", self.name_input)

        self.db_type_combo = QComboBox()
        form.addRow("数据库类型", self.db_type_combo)

        self.host_input = QLineEdit()
        self.host_input.setPlaceholderText("127.0.0.1")
        form.addRow("Host", self.host_input)

        self.port_input = QSpinBox()
        self.port_input.setMinimum(1)
        self.port_input.setMaximum(65535)
        self.port_input.setValue(5432)
        form.addRow("Port", self.port_input)

        self.database_input = QLineEdit()
        form.addRow("Database", self.database_input)

        self.username_input = QLineEdit()
        form.addRow("Username", self.username_input)

        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        form.addRow("Password", self.password_input)

        self.schema_prefix_input = QLineEdit()
        self.schema_prefix_input.setPlaceholderText("df_")
        form.addRow("Schema 前缀", self.schema_prefix_input)

        self.owner_prefix_input = QLineEdit()
        self.owner_prefix_input.setPlaceholderText("df_")
        form.addRow("Owner 前缀", self.owner_prefix_input)

        layout.addLayout(form)

        self.error_label = QLabel("")
        self.error_label.setStyleSheet("color: #b42318;")
        layout.addWidget(self.error_label)

        buttons = QHBoxLayout()
        self.test_button = QPushButton("测试连接")
        self.test_button.clicked.connect(self._handle_test_connection)
        buttons.addWidget(self.test_button)

        buttons.addStretch()

        self.save_button = QPushButton("保存")
        self.save_button.clicked.connect(self._handle_save)
        buttons.addWidget(self.save_button)

        self.cancel_button = QPushButton("取消")
        self.cancel_button.clicked.connect(self.reject)
        buttons.addWidget(self.cancel_button)

        layout.addLayout(buttons)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start_create_profile(self, role: ConnectionRole) -> None:
        """Reset form for creating a new profile."""
        self._editing_profile = None
        self._current_role = role
        self._clear_form()
        self._configure_db_type_combo(role)

    def start_edit_profile(self, profile: ConnectionProfile) -> None:
        """Populate form with an existing profile for editing."""
        self._editing_profile = profile
        self._current_role = profile.role
        self._configure_db_type_combo(profile.role)
        self.name_input.setText(profile.name)
        self._set_db_type_combo(profile.db_type)
        self.host_input.setText(profile.host)
        self.port_input.setValue(profile.port)
        self.database_input.setText(profile.database)
        self.username_input.setText(profile.username)
        self.schema_prefix_input.setText(profile.schema_prefix)
        self.owner_prefix_input.setText(profile.owner_prefix)
        self.password_input.clear()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _configure_db_type_combo(self, role: ConnectionRole) -> None:
        self.db_type_combo.clear()
        if role == ConnectionRole.SOURCE:
            self.db_type_combo.addItem("PostgreSQL", DatabaseType.POSTGRESQL.value)
        else:
            self.db_type_combo.addItem("PostgreSQL", DatabaseType.POSTGRESQL.value)
            self.db_type_combo.addItem("KingBase", DatabaseType.KINGBASE.value)

    def _set_db_type_combo(self, db_type: DatabaseType) -> None:
        for i in range(self.db_type_combo.count()):
            if self.db_type_combo.itemData(i) == db_type.value:
                self.db_type_combo.setCurrentIndex(i)
                return

    def _clear_form(self) -> None:
        self.name_input.clear()
        self.host_input.clear()
        self.port_input.setValue(5432)
        self.database_input.clear()
        self.username_input.clear()
        self.password_input.clear()
        self.schema_prefix_input.clear()
        self.owner_prefix_input.clear()
        self.error_label.clear()

    def _read_form(self) -> Tuple[Optional[ConnectionProfile], Optional[str]]:
        role = self._current_role
        if role is None:
            return None, "Connection role not set"

        has_existing_password = self._editing_profile is not None
        profile, error = validate_profile_inputs(
            role=role,
            name=self.name_input.text(),
            db_type_value=self.db_type_combo.currentData(),
            host=self.host_input.text(),
            port_text=str(self.port_input.value()),
            database=self.database_input.text(),
            username=self.username_input.text(),
            password=self.password_input.text(),
            existing_password=has_existing_password,
        )
        if error:
            return None, error

        if self._editing_profile is not None:
            profile = ConnectionProfile(
                id=self._editing_profile.id,
                name=profile.name,
                role=profile.role,
                db_type=profile.db_type,
                host=profile.host,
                port=profile.port,
                database=profile.database,
                username=profile.username,
                credential_key=self._editing_profile.credential_key,
                schema_prefix=self.schema_prefix_input.text() or "df_",
                owner_prefix=self.owner_prefix_input.text() or "df_",
            )
        else:
            profile = ConnectionProfile(
                name=profile.name,
                role=profile.role,
                db_type=profile.db_type,
                host=profile.host,
                port=profile.port,
                database=profile.database,
                username=profile.username,
                schema_prefix=self.schema_prefix_input.text() or "df_",
                owner_prefix=self.owner_prefix_input.text() or "df_",
            )
        return profile, None

    def _handle_test_connection(self) -> None:
        profile, error = self._read_form()
        if error:
            self.error_label.setText(error)
            return

        if self.app_store is None:
            self.error_label.setText("未初始化应用存储")
            return

        from db_schema_sync_client.infrastructure.db_connection import DatabaseConnectionFactory

        password = self.password_input.text()
        if not password and self._editing_profile is not None:
            try:
                password = self.app_store.get_profile_password(self._editing_profile)
            except Exception:
                self.error_label.setText("无法获取已保存的密码")
                return

        factory = DatabaseConnectionFactory()
        result = factory.test_connection(profile, password)
        if result.success:
            QMessageBox.information(self, "测试连接", "连接成功")
            self.error_label.clear()
        else:
            self.error_label.setText(f"连接失败: {result.message}")

    def _handle_save(self) -> None:
        profile, error = self._read_form()
        if error:
            self.error_label.setText(error)
            return

        if self.app_store is None:
            self.error_label.setText("未初始化应用存储")
            return

        password = self.password_input.text()
        if not password and self._editing_profile is not None:
            try:
                password = self.app_store.get_profile_password(self._editing_profile)
            except Exception:
                self.error_label.setText("无法获取已保存的密码，请重新输入密码")
                return

        try:
            saved = self.app_store.save_profile(profile, password)
            self._editing_profile = saved
            self.accept()
        except Exception as exc:
            self.error_label.setText(str(exc))


class ProfileManagerDialog(QDialog):
    """Full profile management dialog with list + CRUD + duplicate + default."""

    def __init__(self, app_store, parent=None) -> None:
        super().__init__(parent)
        self.app_store = app_store
        self.setWindowTitle("连接管理")
        self.setModal(True)
        self.resize(800, 520)
        self._build_ui()
        self._refresh_lists()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # --- Source panel ---
        source_panel = QWidget()
        source_layout = QVBoxLayout(source_panel)
        source_layout.addWidget(QLabel("源端连接"))
        self.source_list = QListWidget()
        source_layout.addWidget(self.source_list)
        source_buttons = QHBoxLayout()
        add_source = QPushButton("新增")
        add_source.clicked.connect(lambda: self._add_profile(ConnectionRole.SOURCE))
        source_buttons.addWidget(add_source)
        edit_source = QPushButton("编辑")
        edit_source.clicked.connect(lambda: self._edit_selected(self.source_list))
        source_buttons.addWidget(edit_source)
        del_source = QPushButton("删除")
        del_source.clicked.connect(lambda: self._delete_selected(self.source_list))
        source_buttons.addWidget(del_source)
        dup_source = QPushButton("复制")
        dup_source.clicked.connect(lambda: self._duplicate_selected(self.source_list))
        source_buttons.addWidget(dup_source)
        def_source = QPushButton("设为默认")
        def_source.clicked.connect(lambda: self._set_default(self.source_list, ConnectionRole.SOURCE))
        source_buttons.addWidget(def_source)
        source_layout.addLayout(source_buttons)
        splitter.addWidget(source_panel)

        # --- Target panel ---
        target_panel = QWidget()
        target_layout = QVBoxLayout(target_panel)
        target_layout.addWidget(QLabel("目标端连接"))
        self.target_list = QListWidget()
        target_layout.addWidget(self.target_list)
        target_buttons = QHBoxLayout()
        add_target = QPushButton("新增")
        add_target.clicked.connect(lambda: self._add_profile(ConnectionRole.TARGET))
        target_buttons.addWidget(add_target)
        edit_target = QPushButton("编辑")
        edit_target.clicked.connect(lambda: self._edit_selected(self.target_list))
        target_buttons.addWidget(edit_target)
        del_target = QPushButton("删除")
        del_target.clicked.connect(lambda: self._delete_selected(self.target_list))
        target_buttons.addWidget(del_target)
        dup_target = QPushButton("复制")
        dup_target.clicked.connect(lambda: self._duplicate_selected(self.target_list))
        target_buttons.addWidget(dup_target)
        def_target = QPushButton("设为默认")
        def_target.clicked.connect(lambda: self._set_default(self.target_list, ConnectionRole.TARGET))
        target_buttons.addWidget(def_target)
        target_layout.addLayout(target_buttons)
        splitter.addWidget(target_panel)

        layout.addWidget(splitter)

        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn)

    def _refresh_lists(self) -> None:
        if self.app_store is None:
            return
        self.source_list.clear()
        for p in self.app_store.list_profiles(ConnectionRole.SOURCE):
            label = f"{'★ ' if p.is_default else ''}{p.name} ({p.host}:{p.port}/{p.database})"
            item = QListWidgetItem(label)
            item.setData(Qt.ItemDataRole.UserRole, p.id)
            self.source_list.addItem(item)

        self.target_list.clear()
        for p in self.app_store.list_profiles(ConnectionRole.TARGET):
            label = f"{'★ ' if p.is_default else ''}{p.name} [{p.db_type.value}] ({p.host}:{p.port}/{p.database})"
            item = QListWidgetItem(label)
            item.setData(Qt.ItemDataRole.UserRole, p.id)
            self.target_list.addItem(item)

    def _get_selected_profile_id(self, list_widget: QListWidget) -> Optional[int]:
        current = list_widget.currentItem()
        if current is None:
            return None
        return current.data(Qt.ItemDataRole.UserRole)

    def _add_profile(self, role: ConnectionRole) -> None:
        dialog = ConnectionConfigDialog(self.app_store, parent=self)
        dialog.start_create_profile(role)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self._refresh_lists()

    def _edit_selected(self, list_widget: QListWidget) -> None:
        profile_id = self._get_selected_profile_id(list_widget)
        if profile_id is None:
            return
        profile = self.app_store.get_profile(profile_id)
        if profile is None:
            return
        dialog = ConnectionConfigDialog(self.app_store, parent=self)
        dialog.start_edit_profile(profile)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self._refresh_lists()

    def _delete_selected(self, list_widget: QListWidget) -> None:
        profile_id = self._get_selected_profile_id(list_widget)
        if profile_id is None:
            return
        reply = QMessageBox.question(
            self, "删除确认", "确定要删除此连接配置？",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            self.app_store.delete_profile(profile_id)
            self._refresh_lists()

    def _duplicate_selected(self, list_widget: QListWidget) -> None:
        profile_id = self._get_selected_profile_id(list_widget)
        if profile_id is None:
            return
        profile = self.app_store.get_profile(profile_id)
        if profile is None:
            return
        try:
            password = self.app_store.get_profile_password(profile)
        except Exception:
            QMessageBox.warning(self, "复制失败", "无法获取原连接密码")
            return
        new_profile = ConnectionProfile(
            name=f"{profile.name} (副本)",
            role=profile.role,
            db_type=profile.db_type,
            host=profile.host,
            port=profile.port,
            database=profile.database,
            username=profile.username,
            schema_prefix=profile.schema_prefix,
            owner_prefix=profile.owner_prefix,
        )
        self.app_store.save_profile(new_profile, password)
        self._refresh_lists()

    def _set_default(self, list_widget: QListWidget, role: ConnectionRole) -> None:
        profile_id = self._get_selected_profile_id(list_widget)
        if profile_id is None:
            return
        self.app_store.set_default_profile(role, profile_id)
        self._refresh_lists()
