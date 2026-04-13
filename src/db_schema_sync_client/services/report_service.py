"""Report rendering helpers for compare and sync operations."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from db_schema_sync_client.domain.diff import SchemaDiff
from db_schema_sync_client.domain.models import ConnectionProfile

from .sql_generator import GeneratedSqlPlan
from .sync_executor import SyncExecutionResult


class ReportService:
    def render_compare_report(
        self,
        source_profile: ConnectionProfile,
        target_profile: ConnectionProfile,
        diff: SchemaDiff,
        sql_plan: GeneratedSqlPlan,
    ) -> str:
        lines = [
            "# Compare Report",
            "",
            f"Source profile: {source_profile.name}",
            f"Target profile: {target_profile.name}",
            f"Target database type: {target_profile.db_type.value}",
            "",
            "## Summary",
            f"Auto-syncable: {sql_plan.auto_syncable_count}",
            f"Manual required: {sql_plan.manual_required_count}",
            f"Hint only: {sql_plan.hint_only_count}",
            f"Object diffs: {len(diff.object_diffs)}",
            f"Column diffs: {len(diff.column_diffs)}",
            "",
            "## SQL",
        ]
        if sql_plan.statements:
            lines.extend(["```sql", *sql_plan.statements, "```"])
        else:
            lines.append("_No executable SQL generated._")
        return "\n".join(lines)

    def render_sync_report(
        self,
        target_profile: ConnectionProfile,
        result: SyncExecutionResult,
    ) -> str:
        lines = [
            "# Sync Report",
            "",
            f"Target profile: {target_profile.name}",
            f"Target database type: {target_profile.db_type.value}",
            f"Success count: {result.success_count}",
            f"Failure count: {result.failure_count}",
            "",
            "## Statements",
        ]
        for item in result.results:
            lines.append(f"- [{item.status}] {item.statement}")
            if item.error_message:
                lines.append(f"  Error: {item.error_message}")
        return "\n".join(lines)

    def save_report(self, text: str, output_dir: Path) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        path.write_text(text, encoding="utf-8")
        return path
