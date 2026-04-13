"""Login dialog for desktop client."""

from __future__ import annotations

from PyQt6.QtWidgets import QDialog, QFormLayout, QLabel, QLineEdit, QPushButton, QVBoxLayout


class LoginDialog(QDialog):
    def __init__(self, app_store, parent=None) -> None:
        super().__init__(parent)
        self.app_store = app_store
        self.setWindowTitle("登录")
        self.setModal(True)
        self.resize(360, 180)
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()
        self.username_input = QLineEdit()
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        form.addRow("用户名", self.username_input)
        form.addRow("密码", self.password_input)
        layout.addLayout(form)

        self.error_label = QLabel("")
        self.error_label.setStyleSheet("color: #b42318;")
        layout.addWidget(self.error_label)

        self.login_button = QPushButton("登录")
        self.login_button.clicked.connect(self.attempt_login)
        self.password_input.returnPressed.connect(self.attempt_login)
        layout.addWidget(self.login_button)

    def attempt_login(self) -> None:
        username = self.username_input.text().strip()
        password = self.password_input.text()

        if not username or not password:
            self.error_label.setText("用户名和密码不能为空")
            return

        if self.app_store.verify_user(username, password):
            self.error_label.clear()
            self.accept()
            return

        self.error_label.setText("用户名或密码错误")
