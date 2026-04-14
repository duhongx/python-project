"""Cluster configuration dialog."""

from __future__ import annotations

from typing import Optional

from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
)

from db_schema_sync_client.domain.models import ClusterEnvironment, ClusterProfile


class ClusterDialog(QDialog):
    def __init__(self, app_store, parent=None) -> None:
        super().__init__(parent)
        self.app_store = app_store
        self._editing_cluster: Optional[ClusterProfile] = None
        self.setWindowTitle("集群配置")
        self.setModal(True)
        self.resize(520, 420)
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()

        self.name_input = QLineEdit()
        form.addRow("集群名称", self.name_input)

        self.environment_combo = QComboBox()
        for environment in ClusterEnvironment:
            self.environment_combo.addItem(environment.value, environment)
        form.addRow("环境", self.environment_combo)

        self.description_input = QLineEdit()
        form.addRow("说明", self.description_input)

        self.patroni_input = QLineEdit()
        self.patroni_input.setPlaceholderText("逗号分隔")
        form.addRow("Patroni 地址", self.patroni_input)

        self.pg_host_input = QLineEdit()
        form.addRow("PG Host", self.pg_host_input)

        self.pg_port_input = QSpinBox()
        self.pg_port_input.setRange(1, 65535)
        self.pg_port_input.setValue(5432)
        form.addRow("PG Port", self.pg_port_input)

        self.pg_database_input = QLineEdit()
        self.pg_database_input.setText("postgres")
        form.addRow("PG Database", self.pg_database_input)

        self.pg_username_input = QLineEdit()
        form.addRow("PG Username", self.pg_username_input)

        self.pg_password_input = QLineEdit()
        self.pg_password_input.setEchoMode(QLineEdit.EchoMode.Password)
        form.addRow("PG Password", self.pg_password_input)

        self.etcd_input = QLineEdit()
        self.etcd_input.setPlaceholderText("逗号分隔")
        form.addRow("etcd 地址", self.etcd_input)

        self.enabled_checkbox = QCheckBox("启用")
        self.enabled_checkbox.setChecked(True)
        form.addRow("", self.enabled_checkbox)

        layout.addLayout(form)

        self.error_label = QLabel("")
        self.error_label.setStyleSheet("color: #b42318;")
        layout.addWidget(self.error_label)

        buttons = QHBoxLayout()
        test_button = QPushButton("测试连接")
        test_button.clicked.connect(self._handle_test)
        buttons.addWidget(test_button)
        buttons.addStretch()

        save_button = QPushButton("保存")
        save_button.clicked.connect(self._handle_save)
        buttons.addWidget(save_button)

        cancel_button = QPushButton("取消")
        cancel_button.clicked.connect(self.reject)
        buttons.addWidget(cancel_button)

        layout.addLayout(buttons)

    def start_create(self) -> None:
        self._editing_cluster = None
        self._clear_form()

    def start_edit(self, cluster: ClusterProfile) -> None:
        self._editing_cluster = cluster
        self.name_input.setText(cluster.name)
        self.description_input.setText(cluster.description)
        self.patroni_input.setText(", ".join(cluster.patroni_endpoints))
        self.pg_host_input.setText(cluster.pg_host)
        self.pg_port_input.setValue(cluster.pg_port)
        self.pg_database_input.setText(cluster.pg_database)
        self.pg_username_input.setText(cluster.pg_username)
        self.etcd_input.setText(", ".join(cluster.etcd_endpoints))
        self.enabled_checkbox.setChecked(cluster.is_enabled)
        self.environment_combo.setCurrentIndex(self.environment_combo.findData(cluster.environment))
        self.pg_password_input.clear()

    def _clear_form(self) -> None:
        self.name_input.clear()
        self.description_input.clear()
        self.patroni_input.clear()
        self.pg_host_input.clear()
        self.pg_port_input.setValue(5432)
        self.pg_database_input.setText("postgres")
        self.pg_username_input.clear()
        self.pg_password_input.clear()
        self.etcd_input.clear()
        self.enabled_checkbox.setChecked(True)
        self.error_label.clear()

    def _build_profile(self) -> ClusterProfile:
        patroni_endpoints = tuple(
            part.strip() for part in self.patroni_input.text().split(",") if part.strip()
        )
        etcd_endpoints = tuple(
            part.strip() for part in self.etcd_input.text().split(",") if part.strip()
        )

        if not self.name_input.text().strip():
            raise ValueError("集群名称不能为空")
        if not patroni_endpoints:
            raise ValueError("Patroni 地址不能为空")
        if not self.pg_host_input.text().strip():
            raise ValueError("PG Host 不能为空")
        if not self.pg_username_input.text().strip():
            raise ValueError("PG Username 不能为空")
        if not etcd_endpoints:
            raise ValueError("etcd 地址不能为空")

        return ClusterProfile(
            id=self._editing_cluster.id if self._editing_cluster else None,
            credential_key=self._editing_cluster.credential_key if self._editing_cluster else None,
            name=self.name_input.text().strip(),
            environment=self.environment_combo.currentData(),
            description=self.description_input.text().strip(),
            patroni_endpoints=patroni_endpoints,
            pg_host=self.pg_host_input.text().strip(),
            pg_port=self.pg_port_input.value(),
            pg_database=self.pg_database_input.text().strip() or "postgres",
            pg_username=self.pg_username_input.text().strip(),
            etcd_endpoints=etcd_endpoints,
            is_enabled=self.enabled_checkbox.isChecked(),
        )

    def _handle_test(self) -> None:
        try:
            self._build_profile()
        except ValueError as exc:
            self.error_label.setText(str(exc))
            return

        QMessageBox.information(self, "测试连接", "轻量版暂未接入真实检查，配置格式校验通过。")
        self.error_label.clear()

    def _handle_save(self) -> None:
        if self.app_store is None:
            self.error_label.setText("未初始化应用存储")
            return

        try:
            profile = self._build_profile()
        except ValueError as exc:
            self.error_label.setText(str(exc))
            return

        password = self.pg_password_input.text()
        if not password and self._editing_cluster is None:
            self.error_label.setText("PG Password 不能为空")
            return

        if not password and self._editing_cluster is not None:
            password = self.app_store.credential_store.get(self._editing_cluster.credential_key)

        self.app_store.save_cluster_profile(profile, password)
        self.accept()
