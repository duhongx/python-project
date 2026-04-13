from db_schema_sync_client.domain.diff import ColumnDiff, DiffCategory, DiffStatus, SchemaDiff
from db_schema_sync_client.domain.models import ColumnDefinition, ConnectionProfile, ConnectionRole, DatabaseType
from db_schema_sync_client.services.report_service import ReportService
from db_schema_sync_client.services.sql_generator import GeneratedSqlPlan
from db_schema_sync_client.services.sync_executor import ExecutedStatementResult, SyncExecutionResult


def make_profile(name: str, role: ConnectionRole, db_type: DatabaseType) -> ConnectionProfile:
    return ConnectionProfile(
        id=1,
        name=name,
        role=role,
        db_type=db_type,
        host="127.0.0.1",
        port=5432,
        database="demo",
        username="demo_user",
        credential_key=f"cred-{name}",
    )


def make_compare_diff() -> SchemaDiff:
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
    return SchemaDiff(
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
        ),
    )


def test_render_compare_report_includes_summary_and_generated_sql():
    service = ReportService()
    source = make_profile("source-main", ConnectionRole.SOURCE, DatabaseType.POSTGRESQL)
    target = make_profile("target-kb", ConnectionRole.TARGET, DatabaseType.KINGBASE)
    diff = make_compare_diff()
    plan = GeneratedSqlPlan(
        target_type=DatabaseType.KINGBASE,
        statements=['ALTER TABLE "df_demo"."users" ADD COLUMN "display_name" character varying(100);'],
        auto_syncable_count=1,
        manual_required_count=2,
        hint_only_count=3,
    )

    report = service.render_compare_report(source, target, diff, plan)

    assert "source-main" in report
    assert "target-kb" in report
    assert "kingbase" in report
    assert "Auto-syncable: 1" in report
    assert "Manual required: 2" in report
    assert 'ALTER TABLE "df_demo"."users" ADD COLUMN "display_name" character varying(100);' in report


def test_render_reports_never_include_passwords(tmp_path):
    service = ReportService()
    target = make_profile("target-kb", ConnectionRole.TARGET, DatabaseType.KINGBASE)
    result = SyncExecutionResult(
        run_id=1,
        success_count=1,
        failure_count=0,
        results=(
            ExecutedStatementResult(
                statement='ALTER TABLE "df_demo"."users" ADD COLUMN "display_name" character varying(100);',
                status="success",
                error_message=None,
            ),
        ),
    )

    report = service.render_sync_report(target, result)
    path = service.save_report(report, tmp_path)

    assert "secret" not in report
    assert path.exists() is True
    assert "secret" not in path.read_text(encoding="utf-8")
