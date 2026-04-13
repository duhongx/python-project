"""SQL preview dialog with dry-run and report export."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from PyQt6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
)

from db_schema_sync_client.domain.models import ConnectionProfile
from db_schema_sync_client.services.report_service import ReportService
from db_schema_sync_client.services.sql_generator import GeneratedSqlPlan


class SqlPreviewDialog(QDialog):
    """Preview generated SQL, inspect warnings, and choose to execute or dry-run."""

    def __init__(
        self,
        target_profile: ConnectionProfile,
        plan: GeneratedSqlPlan,
        *,
        report_service: Optional[ReportService] = None,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.target_profile = target_profile
        self.plan = plan
        self.report_service = report_service or ReportService()
        self.selected_action: Optional[str] = None
        self.setWindowTitle("SQL 预览")
        self.setModal(True)
        self.resize(700, 520)
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        self.summary_label = QLabel(
            f"目标连接: {self.target_profile.name}  |  "
            f"数据库类型: {self.target_profile.db_type.value}  |  "
            f"风险等级: {self.plan.risk_level}"
        )
        layout.addWidget(self.summary_label)

        self.counts_label = QLabel(
            f"Auto-syncable: {self.plan.auto_syncable_count}  |  "
            f"Manual required: {self.plan.manual_required_count}  |  "
            f"Hint only: {self.plan.hint_only_count}  |  "
            f"SQL 数量: {len(self.plan.statements)}"
        )
        layout.addWidget(self.counts_label)

        # SQL list
        sql_label = QLabel("将执行的 SQL:")
        layout.addWidget(sql_label)
        self.sql_text = QTextEdit()
        self.sql_text.setReadOnly(True)
        self.sql_text.setPlainText("\n".join(self.plan.statements) if self.plan.statements else "无可执行 SQL")
        layout.addWidget(self.sql_text)

        # Warnings
        if self.plan.warnings:
            warn_label = QLabel("风险提示:")
            layout.addWidget(warn_label)
            self.warning_text = QTextEdit()
            self.warning_text.setReadOnly(True)
            self.warning_text.setPlainText("\n".join(self.plan.warnings))
            self.warning_text.setMaximumHeight(100)
            layout.addWidget(self.warning_text)
        else:
            self.warning_text = QTextEdit()
            self.warning_text.setReadOnly(True)
            self.warning_text.hide()

        # Action buttons
        buttons = QHBoxLayout()

        copy_button = QPushButton("复制 SQL")
        copy_button.clicked.connect(self._handle_copy)
        buttons.addWidget(copy_button)

        save_sql_button = QPushButton("保存 SQL")
        save_sql_button.clicked.connect(self._handle_save_sql)
        buttons.addWidget(save_sql_button)

        dry_run_button = QPushButton("Dry Run")
        dry_run_button.clicked.connect(self.handle_dry_run)
        buttons.addWidget(dry_run_button)

        buttons.addStretch()

        execute_button = QPushButton("确认执行")
        execute_button.clicked.connect(self._handle_execute)
        if not self.plan.statements:
            execute_button.setEnabled(False)
        buttons.addWidget(execute_button)

        cancel_button = QPushButton("取消")
        cancel_button.clicked.connect(self.reject)
        buttons.addWidget(cancel_button)

        layout.addLayout(buttons)

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def _handle_copy(self) -> None:
        from PyQt6.QtWidgets import QApplication

        clipboard = QApplication.clipboard()
        if clipboard is not None:
            clipboard.setText("\n".join(self.plan.statements))

    def _handle_save_sql(self) -> None:
        from PyQt6.QtWidgets import QFileDialog

        path, _ = QFileDialog.getSaveFileName(
            self, "保存 SQL", "sync.sql", "SQL Files (*.sql);;All Files (*)"
        )
        if path:
            Path(path).write_text("\n".join(self.plan.statements), encoding="utf-8")

    def handle_dry_run(self) -> None:
        self.selected_action = "dry_run"
        self.accept()

    def _handle_execute(self) -> None:
        self.selected_action = "execute"
        self.accept()

    def save_sql_and_report(self, output_dir: Path) -> Path:
        """Save SQL and a basic report to *output_dir*; returns the report path."""
        text_lines = [
            f"# Dry Run Report",
            "",
            f"Target: {self.target_profile.name} ({self.target_profile.db_type.value})",
            f"SQL count: {len(self.plan.statements)}",
            f"Risk level: {self.plan.risk_level}",
            "",
            "## SQL",
        ]
        if self.plan.statements:
            text_lines.extend(["```sql", *self.plan.statements, "```"])
        else:
            text_lines.append("_No SQL._")

        if self.plan.warnings:
            text_lines.extend(["", "## Warnings", *self.plan.warnings])

        report_text = "\n".join(text_lines)
        return self.report_service.save_report(report_text, output_dir)
