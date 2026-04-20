"""Dialog for adding or editing a Kubernetes cluster configuration.

Lets the user:
  1. Give the cluster a display name.
  2. Upload (select) a kubeconfig file.
  3. Choose a context from the available contexts inside the file.
  4. Maintain a list of namespaces (add / remove rows).
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from PyQt6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from db_schema_sync_client.k8s.domain.models import KubeClusterConfig
from db_schema_sync_client.k8s.infrastructure.k8s_client import K8sClient


_DEFAULT_NAMESPACES = ["dev", "test", "pre", "prod"]


class ClusterConfigDialog(QDialog):
    """Add / edit a KubeClusterConfig."""

    def __init__(
        self,
        config: Optional[KubeClusterConfig] = None,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._config = config
        self._selected_kubeconfig_path: Optional[Path] = None

        self.setWindowTitle("K8s 集群配置" if config is None else "编辑 K8s 集群")
        self.setMinimumWidth(520)
        self._build_ui()

        if config is not None:
            self._populate(config)

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setSpacing(12)

        form = QFormLayout()
        form.setLabelAlignment(form.labelAlignment())

        self._name_input = QLineEdit()
        self._name_input.setPlaceholderText("例如: 生产集群")
        form.addRow("集群名称", self._name_input)

        # Kubeconfig file picker
        kube_row = QHBoxLayout()
        self._kubeconfig_label = QLabel("未选择文件")
        self._kubeconfig_label.setStyleSheet("color: #6c757d;")
        kube_row.addWidget(self._kubeconfig_label, 1)
        browse_btn = QPushButton("浏览…")
        browse_btn.clicked.connect(self._browse_kubeconfig)
        kube_row.addWidget(browse_btn)
        kube_widget = QWidget()
        kube_widget.setLayout(kube_row)
        form.addRow("Kubeconfig 文件", kube_widget)

        # Context selector (populated after file is chosen)
        self._context_combo = QComboBox()
        self._context_combo.setPlaceholderText("请先选择 kubeconfig 文件")
        form.addRow("集群 Context", self._context_combo)

        layout.addLayout(form)

        # Namespace list
        ns_label = QLabel("Namespaces")
        ns_label.setStyleSheet("font-weight: 600;")
        layout.addWidget(ns_label)

        self._ns_list = QListWidget()
        self._ns_list.setMaximumHeight(140)
        layout.addWidget(self._ns_list)

        ns_btn_row = QHBoxLayout()
        self._ns_input = QLineEdit()
        self._ns_input.setPlaceholderText("输入 namespace 名称")
        ns_btn_row.addWidget(self._ns_input, 1)
        add_ns_btn = QPushButton("添加")
        add_ns_btn.clicked.connect(self._add_namespace)
        ns_btn_row.addWidget(add_ns_btn)
        remove_ns_btn = QPushButton("删除选中")
        remove_ns_btn.clicked.connect(self._remove_namespace)
        ns_btn_row.addWidget(remove_ns_btn)
        layout.addLayout(ns_btn_row)

        # Dialog buttons
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        # Pre-fill default namespaces for new configs
        if self._config is None:
            for ns in _DEFAULT_NAMESPACES:
                self._ns_list.addItem(QListWidgetItem(ns))

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def _browse_kubeconfig(self) -> None:
        path_str, _ = QFileDialog.getOpenFileName(
            self,
            "选择 kubeconfig 文件",
            str(Path.home()),
            "kubeconfig 文件 (config)",
        )
        if not path_str:
            return

        path = Path(path_str)
        self._selected_kubeconfig_path = path
        self._kubeconfig_label.setText(path.name)
        self._kubeconfig_label.setStyleSheet("color: #1f2937;")
        self._load_contexts(path)

    def _load_contexts(self, path: Path) -> None:
        self._context_combo.clear()
        try:
            contexts = K8sClient.list_contexts(path)
            for ctx in contexts:
                self._context_combo.addItem(ctx)
        except Exception as exc:
            QMessageBox.warning(self, "解析失败", f"无法读取 kubeconfig 文件：\n{exc}")

    def _add_namespace(self) -> None:
        name = self._ns_input.text().strip()
        if not name:
            return
        # Check for duplicates
        for i in range(self._ns_list.count()):
            if self._ns_list.item(i).text() == name:
                return
        self._ns_list.addItem(QListWidgetItem(name))
        self._ns_input.clear()

    def _remove_namespace(self) -> None:
        for item in self._ns_list.selectedItems():
            self._ns_list.takeItem(self._ns_list.row(item))

    def _accept(self) -> None:
        if not self._name_input.text().strip():
            QMessageBox.warning(self, "验证失败", "请输入集群名称。")
            return
        if self._config is None and self._selected_kubeconfig_path is None:
            QMessageBox.warning(self, "验证失败", "请选择 kubeconfig 文件。")
            return
        if self._context_combo.currentText() == "" and self._context_combo.count() == 0:
            QMessageBox.warning(self, "验证失败", "请先选择 kubeconfig 文件以加载 Context 列表。")
            return
        self.accept()

    # ------------------------------------------------------------------
    # Result accessors
    # ------------------------------------------------------------------

    def get_name(self) -> str:
        return self._name_input.text().strip()

    def get_context(self) -> str:
        return self._context_combo.currentText()

    def get_namespaces(self) -> List[str]:
        return [
            self._ns_list.item(i).text()
            for i in range(self._ns_list.count())
        ]

    def get_kubeconfig_path(self) -> Optional[Path]:
        """Return the newly selected kubeconfig path (None if unchanged)."""
        return self._selected_kubeconfig_path

    # ------------------------------------------------------------------
    # Pre-population for edit mode
    # ------------------------------------------------------------------

    def _populate(self, config: KubeClusterConfig) -> None:
        self._name_input.setText(config.name)
        # Show the stored filename
        stored = Path(config.kubeconfig_path)
        self._kubeconfig_label.setText(stored.name)
        self._kubeconfig_label.setStyleSheet("color: #1f2937;")
        # Load contexts from the stored file
        if stored.exists():
            self._load_contexts(stored)
            idx = self._context_combo.findText(config.context_name)
            if idx >= 0:
                self._context_combo.setCurrentIndex(idx)
        else:
            self._context_combo.addItem(config.context_name)

        self._ns_list.clear()
        for ns in config.namespaces:
            self._ns_list.addItem(QListWidgetItem(ns))
