"""SQL generation for safe phase-one sync actions."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from db_schema_sync_client.domain.diff import DiffCategory, DiffStatus, ObjectDiff, SchemaDiff
from db_schema_sync_client.domain.models import ColumnDefinition, DatabaseType, ObjectType

from .dialects import get_dialect


def _compute_risk_level(
    warnings: list[str],
    items: Iterable[tuple[str, str, ColumnDefinition]],
) -> str:
    """Return ``'low'``, ``'medium'``, or ``'high'`` risk.

    * high — any sequence/identity column involved
    * medium — NOT NULL warnings exist but no sequence columns
    * low — no warnings
    """
    if not warnings:
        return "low"
    has_sequence = any(col.is_sequence_related for _, _, col in items)
    if has_sequence:
        return "high"
    return "medium"


@dataclass(frozen=True)
class GeneratedSqlPlan:
    target_type: DatabaseType
    statements: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    risk_level: str = "low"
    auto_syncable_count: int = 0
    manual_required_count: int = 0
    hint_only_count: int = 0


class SqlGenerator:
    def generate(self, diff: SchemaDiff, target_type: DatabaseType) -> GeneratedSqlPlan:
        items = []
        manual_count = 0
        hint_count = 0

        for column_diff in diff.column_diffs:
            if (
                column_diff.category == DiffCategory.AUTO_SYNCABLE
                and column_diff.status == DiffStatus.ONLY_SOURCE
                and column_diff.source_column is not None
            ):
                items.append((column_diff.schema, column_diff.object_name, column_diff.source_column))
            elif column_diff.category == DiffCategory.MANUAL_REQUIRED:
                manual_count += 1
            else:
                hint_count += 1

        for object_diff in diff.object_diffs:
            if object_diff.category == DiffCategory.MANUAL_REQUIRED:
                manual_count += 1
            elif object_diff.category == DiffCategory.ONLY_HINT:
                hint_count += 1
            # SCHEMA_SYNCABLE 计入可同步范围，不计为 hint

        return self.generate_missing_columns(
            items=items,
            target_type=target_type,
            manual_count=manual_count,
            hint_count=hint_count,
        )

    def generate_missing_columns(
        self,
        items: Iterable[tuple[str, str, ColumnDefinition]],
        target_type: DatabaseType,
        manual_count: int = 0,
        hint_count: int = 0,
    ) -> GeneratedSqlPlan:
        dialect = get_dialect(target_type)
        statements: list[str] = []
        warnings: list[str] = []
        items_list = list(items)

        for schema_name, table_name, column in items_list:
            sql, item_warnings = dialect.build_add_column_sql(schema_name, table_name, column)
            statements.append(sql)
            warnings.extend(item_warnings)

        risk_level = _compute_risk_level(warnings, items_list)
        return GeneratedSqlPlan(
            target_type=target_type,
            statements=statements,
            warnings=warnings,
            risk_level=risk_level,
            auto_syncable_count=len(statements),
            manual_required_count=manual_count,
            hint_only_count=hint_count,
        )

    def generate_schema_creates(
        self,
        schema_object_diffs: List[ObjectDiff],
        target_type: DatabaseType,
        manual_count: int = 0,
        hint_count: int = 0,
    ) -> GeneratedSqlPlan:
        """Generate CREATE SCHEMA + CREATE TABLE statements for missing schemas."""
        dialect = get_dialect(target_type)
        statements: list[str] = []
        warnings: list[str] = []

        # Group source TableDefinitions by schema
        tables_by_schema: dict[str, list] = defaultdict(list)
        for od in schema_object_diffs:
            if od.source_object is not None:
                tables_by_schema[od.schema].append(od.source_object)

        for schema_name in sorted(tables_by_schema.keys()):
            # 1. CREATE ROLE
            role_sql, role_warnings = dialect.build_create_role_sql(schema_name)
            statements.append(role_sql)
            warnings.extend(role_warnings)

            # 2. CREATE SCHEMA AUTHORIZATION role
            schema_sql, schema_warnings = dialect.build_create_schema_sql(schema_name)
            statements.append(schema_sql)
            warnings.extend(schema_warnings)

            # 3. CREATE TABLE (tables only)
            view_defs = []
            for table_def in sorted(tables_by_schema[schema_name], key=lambda t: t.name):
                if table_def.object_type == ObjectType.TABLE:
                    tbl_sql, tbl_warnings = dialect.build_create_table_sql(schema_name, table_def)
                    statements.append(tbl_sql)
                    warnings.extend(tbl_warnings)
                elif table_def.object_type == ObjectType.VIEW:
                    view_defs.append(table_def)

            # 4. CREATE VIEW (use stored DDL when available)
            for view_def in view_defs:
                view_sql, view_warnings = dialect.build_create_view_sql(schema_name, view_def)
                statements.append(view_sql)
                warnings.extend(view_warnings)

        risk_level = "medium" if warnings else "low"
        return GeneratedSqlPlan(
            target_type=target_type,
            statements=statements,
            warnings=warnings,
            risk_level=risk_level,
            auto_syncable_count=len(statements),
            manual_required_count=manual_count,
            hint_only_count=hint_count,
        )
