import importlib.util
import os
import tempfile
import unittest
from pathlib import Path


if importlib.util.find_spec("PyQt6") is None:
    raise unittest.SkipTest("PyQt6 not installed in this interpreter")

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtWidgets import QApplication, QDialog, QPushButton

from db_schema_sync_client.domain.diff import ColumnDiff, DiffCategory, DiffStatus, SchemaDiff
from db_schema_sync_client.domain.models import (
    ClusterEnvironment,
    ClusterProfile,
    ColumnDefinition,
    ConnectionProfile,
    ConnectionRole,
    DatabaseType,
)
from db_schema_sync_client.services.report_service import ReportService
from db_schema_sync_client.services.sql_generator import GeneratedSqlPlan
from db_schema_sync_client.ui.cluster_list_page import ClusterListPage
from db_schema_sync_client.ui.cluster_overview_page import ClusterOverviewPage
from db_schema_sync_client.ui.config_dialog import ConnectionConfigDialog, validate_profile_inputs
from db_schema_sync_client.ui.login_dialog import LoginDialog
from db_schema_sync_client.ui.main_window import MainWindow
from db_schema_sync_client.ui.sql_preview_dialog import SqlPreviewDialog


def ensure_app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class FakeStore:
    def __init__(self):
        self.verified = []

    def verify_user(self, username, password):
        self.verified.append((username, password))
        return username == "admin" and password == "cloudhis@2123"


class FakeClusterStore:
    def __init__(self):
        self.audit_records = [
            {"created_at": "2026-04-14 10:03", "operator": "admin", "action": "reload", "status": "success", "detail": "pg02"}
        ]

    def list_cluster_profiles(self, environment=None, keyword=None, enabled_only=False):
        clusters = [
            ClusterProfile(
                id=1,
                name="HIS-PROD",
                environment=ClusterEnvironment.PROD,
                patroni_endpoints=("http://patroni-prod:8008",),
                pg_host="10.0.0.10",
                pg_port=5432,
                pg_database="postgres",
                pg_username="postgres",
                etcd_endpoints=("http://etcd-prod:2379",),
                is_enabled=True,
                last_health_status="healthy",
            ),
            ClusterProfile(
                id=2,
                name="HIS-UAT",
                environment=ClusterEnvironment.UAT,
                patroni_endpoints=("http://patroni-uat:8008",),
                pg_host="10.0.0.20",
                pg_port=5432,
                pg_database="postgres",
                pg_username="postgres",
                etcd_endpoints=("http://etcd-uat:2379",),
                is_enabled=False,
                last_health_status="warning",
            ),
        ]
        if environment is not None:
            clusters = [cluster for cluster in clusters if cluster.environment == environment]
        if enabled_only:
            clusters = [cluster for cluster in clusters if cluster.is_enabled]
        if keyword:
            clusters = [cluster for cluster in clusters if keyword.lower() in cluster.name.lower()]
        return clusters

    def get_cluster_profile(self, cluster_id):
        for cluster in self.list_cluster_profiles():
            if cluster.id == cluster_id:
                return cluster
        return None

    def list_cluster_audit_logs(self, cluster_id=None, limit=20):
        return self.audit_records[:limit]


class FakeClusterService:
    def load_overview(self, cluster, app_store):
        from db_schema_sync_client.services.cluster_service import ClusterNodeStatus, ClusterOverview

        return ClusterOverview(
            cluster_name=cluster.name,
            status="healthy",
            primary_node="pg01",
            replica_count=2,
            patroni_healthy_count=3,
            patroni_total_count=3,
            etcd_healthy_count=3,
            etcd_total_count=3,
            total_connections=120,
            active_connections=18,
            topology_lines=("pg01 (Primary)  --->  pg02 (Replica)", "pg01 (Primary)  --->  pg03 (Replica)"),
            nodes=(
                ClusterNodeStatus("pg01", "Primary", "正常", "12", "-", False, "10:21:03"),
                ClusterNodeStatus("pg02", "Replica", "正常", "12", "0 MB", False, "10:21:01"),
            ),
            recent_operations=tuple(app_store.list_cluster_audit_logs(cluster.id)),
        )


class UiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = ensure_app()

    def test_login_dialog_accepts_default_admin(self):
        dialog = LoginDialog(FakeStore())
        dialog.username_input.setText("admin")
        dialog.password_input.setText("cloudhis@2123")

        dialog.attempt_login()

        self.assertEqual(dialog.result(), QDialog.DialogCode.Accepted)

    def test_login_dialog_shows_error_for_wrong_password(self):
        dialog = LoginDialog(FakeStore())
        dialog.username_input.setText("admin")
        dialog.password_input.setText("bad-password")

        dialog.attempt_login()

        self.assertEqual(dialog.error_label.text(), "用户名或密码错误")

    def test_validate_profile_inputs_requires_password_for_new_profile(self):
        profile, error = validate_profile_inputs(
            role=ConnectionRole.SOURCE,
            name="source-main",
            db_type_value=DatabaseType.POSTGRESQL.value,
            host="127.0.0.1",
            port_text="5432",
            database="demo",
            username="demo_user",
            password="",
            existing_password=False,
        )

        self.assertIsNone(profile)
        self.assertEqual(error, "Password is required")

    def test_config_dialog_locks_source_type_and_allows_target_choices(self):
        dialog = ConnectionConfigDialog(app_store=None)

        dialog.start_create_profile(ConnectionRole.SOURCE)
        self.assertEqual(dialog.db_type_combo.count(), 1)
        self.assertEqual(dialog.db_type_combo.currentData(), DatabaseType.POSTGRESQL.value)

        dialog.start_create_profile(ConnectionRole.TARGET)
        self.assertEqual(
            [dialog.db_type_combo.itemData(index) for index in range(dialog.db_type_combo.count())],
            [DatabaseType.POSTGRESQL.value, DatabaseType.KINGBASE.value],
        )

    def test_sql_preview_dialog_shows_counts_and_supports_dry_run(self):
        target = ConnectionProfile(
            id=1,
            name="target-kb",
            role=ConnectionRole.TARGET,
            db_type=DatabaseType.KINGBASE,
            host="127.0.0.1",
            port=5432,
            database="demo",
            username="demo_user",
            credential_key="cred-1",
        )
        plan = GeneratedSqlPlan(
            target_type=DatabaseType.KINGBASE,
            statements=['ALTER TABLE "df_demo"."users" ADD COLUMN "name" character varying(100);'],
            warnings=["manual review warning"],
            auto_syncable_count=1,
            manual_required_count=2,
            hint_only_count=3,
            risk_level="high",
        )

        dialog = SqlPreviewDialog(target, plan, report_service=ReportService())

        self.assertIn("target-kb", dialog.summary_label.text())
        self.assertIn("Auto-syncable: 1", dialog.counts_label.text())
        self.assertIn("manual review warning", dialog.warning_text.toPlainText())

        dialog.handle_dry_run()
        self.assertEqual(dialog.selected_action, "dry_run")

    def test_main_window_generates_sql_for_selected_auto_syncable_diffs(self):
        column = ColumnDefinition(
            name="display_name",
            ordinal_position=2,
            data_type="character varying",
            character_maximum_length=100,
            numeric_precision=None,
            numeric_scale=None,
            is_nullable=True,
            column_default=None,
            is_sequence_related=False,
        )
        diff = SchemaDiff(
            column_diffs=(
                ColumnDiff(
                    schema="df_demo",
                    object_name="users",
                    column_name="display_name",
                    status=DiffStatus.ONLY_SOURCE,
                    category=DiffCategory.AUTO_SYNCABLE,
                    source_column=column,
                    target_column=None,
                ),
            )
        )
        target = ConnectionProfile(
            id=2,
            name="target-pg",
            role=ConnectionRole.TARGET,
            db_type=DatabaseType.POSTGRESQL,
            host="127.0.0.1",
            port=5432,
            database="demo",
            username="demo_user",
            credential_key="cred-2",
        )

        window = MainWindow(app_store=None)
        window.current_diff = diff
        window.current_target_profile = target
        window.comparison_panel.set_diff(diff)

        plan = window.generate_sql_plan_for_selected()

        self.assertEqual(
            plan.statements,
            ['ALTER TABLE "df_demo"."users" ADD COLUMN "display_name" character varying(100);'],
        )

    def test_main_window_builds_navigation_shell_with_structure_sync_default(self):
        window = MainWindow(app_store=None)

        self.assertTrue(hasattr(window, "navigation_list"))
        self.assertTrue(hasattr(window, "page_stack"))
        self.assertTrue(hasattr(window, "structure_sync_page"))
        self.assertEqual(window.navigation_list.count(), 4)
        self.assertIs(window.page_stack.currentWidget(), window.structure_sync_page)

    def test_main_window_keeps_config_and_history_entry_points(self):
        window = MainWindow(app_store=None)
        button_texts = {button.text() for button in window.findChildren(QPushButton)}

        self.assertIn("连接配置", button_texts)
        self.assertIn("历史记录", button_texts)

    def test_cluster_list_page_loads_rows_from_store(self):
        page = ClusterListPage(FakeClusterStore())

        page.refresh()

        self.assertEqual(page.cluster_table.rowCount(), 2)
        self.assertEqual(page.cluster_table.item(0, 1).text(), "HIS-PROD")

    def test_cluster_list_page_calls_detail_callback_for_selected_cluster(self):
        seen = []
        page = ClusterListPage(FakeClusterStore(), open_cluster_callback=seen.append)

        page.refresh()
        page.cluster_table.selectRow(0)
        page._open_selected_cluster()

        self.assertEqual(seen, [1])

    def test_cluster_overview_page_renders_summary_from_service(self):
        store = FakeClusterStore()
        cluster = store.get_cluster_profile(1)
        page = ClusterOverviewPage(store, FakeClusterService(), cluster)

        page.refresh()

        self.assertIn("Primary: pg01", page.summary_label.text())
        self.assertEqual(page.node_table.rowCount(), 2)
        self.assertEqual(page.audit_table.rowCount(), 1)

    def test_sql_preview_dialog_dry_run_saves_report_without_execution(self):
        target = ConnectionProfile(
            id=1,
            name="target-kb",
            role=ConnectionRole.TARGET,
            db_type=DatabaseType.KINGBASE,
            host="127.0.0.1",
            port=5432,
            database="demo",
            username="demo_user",
            credential_key="cred-1",
        )
        plan = GeneratedSqlPlan(
            target_type=DatabaseType.KINGBASE,
            statements=['ALTER TABLE "df_demo"."users" ADD COLUMN "name" character varying(100);'],
            auto_syncable_count=1,
        )
        dialog = SqlPreviewDialog(target, plan, report_service=ReportService())

        with tempfile.TemporaryDirectory() as temp_dir:
            path = dialog.save_sql_and_report(Path(temp_dir))
            self.assertTrue(path.exists())


if __name__ == "__main__":
    unittest.main()
