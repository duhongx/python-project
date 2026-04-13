"""SQL generation for safe phase-one sync actions."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
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

    def generate_object_creates(
        self,
        object_diffs: List[ObjectDiff],
        target_type: DatabaseType,
        existing_schema_owner_fixes: Optional[list[str]] = None,
        role_hashes: Optional[dict[str, str]] = None,
        manual_count: int = 0,
        hint_count: int = 0,
    ) -> GeneratedSqlPlan:
        """Generate CREATE TABLE / CREATE VIEW for TABLE_SYNCABLE diffs (schema already exists)."""
        dialect = get_dialect(target_type)
        statements: list[str] = []
        warnings: list[str] = []

        for schema_name in sorted(existing_schema_owner_fixes or []):
            role_sql, role_warnings = dialect.build_ensure_role_sql(
                schema_name,
                (role_hashes or {}).get(schema_name),
            )
            owner_sql, owner_warnings = dialect.build_alter_schema_owner_sql(schema_name)
            statements.append(role_sql)
            statements.append(owner_sql)
            warnings.extend(role_warnings)
            warnings.extend(owner_warnings)

        for od in sorted(object_diffs, key=lambda o: (o.schema, o.object_name)):
            if od.source_object is None:
                continue
            if od.source_object.object_type == ObjectType.TABLE:
                sql, w = dialect.build_create_table_sql(od.schema, od.source_object)
                extra_sql, extra_warnings = dialect.build_post_create_table_sql(od.schema, od.source_object)
            else:
                sql, w = dialect.build_create_view_sql(od.schema, od.source_object)
                extra_sql, extra_warnings = dialect.build_post_create_view_sql(od.schema, od.source_object)
            statements.append(sql)
            statements.extend(extra_sql)
            warnings.extend(w)
            warnings.extend(extra_warnings)

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

    def generate_schema_creates(
        self,
        schema_object_diffs: List[ObjectDiff],
        target_type: DatabaseType,
        manual_count: int = 0,
        hint_count: int = 0,
        role_hashes: Optional[dict] = None,
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
            # 1. CREATE ROLE（使用源库密码哈希，保持密码不变；无哈希则回退到占位符）
            pw_hash = (role_hashes or {}).get(schema_name)
            role_sql, role_warnings = dialect.build_ensure_role_sql(schema_name, pw_hash)
            statements.append(role_sql)
            warnings.extend(role_warnings)

            # 2. CREATE SCHEMA AUTHORIZATION role
            schema_sql, schema_warnings = dialect.build_create_schema_sql(schema_name)
            statements.append(schema_sql)
            warnings.extend(schema_warnings)

            # 3. CREATE TABLE (tables only) + transfer ownership
            q = dialect.quote_identifier
            view_defs = []
            for table_def in sorted(tables_by_schema[schema_name], key=lambda t: t.name):
                if table_def.object_type == ObjectType.TABLE:
                    tbl_sql, tbl_warnings = dialect.build_create_table_sql(schema_name, table_def)
                    post_sql, post_warnings = dialect.build_post_create_table_sql(schema_name, table_def)
                    statements.append(tbl_sql)
                    statements.extend(post_sql)
                    warnings.extend(tbl_warnings)
                    warnings.extend(post_warnings)
                    # 将表所有权转给 schema 同名角色，确保该角色登录后可见和访问
                    statements.append(
                        f"ALTER TABLE {q(schema_name)}.{q(table_def.name)} OWNER TO {q(schema_name)};"
                    )
                elif table_def.object_type == ObjectType.VIEW:
                    view_defs.append(table_def)

            # 4. CREATE VIEW (use stored DDL when available) + transfer ownership
            for view_def in view_defs:
                view_sql, view_warnings = dialect.build_create_view_sql(schema_name, view_def)
                post_sql, post_warnings = dialect.build_post_create_view_sql(schema_name, view_def)
                statements.append(view_sql)
                statements.extend(post_sql)
                warnings.extend(view_warnings)
                warnings.extend(post_warnings)
                # 只在有真实 DDL 时才 ALTER OWNER（注释占位行无法 ALTER）
                if (view_def.view_definition or "").strip():
                    statements.append(
                        f"ALTER VIEW {q(schema_name)}.{q(view_def.name)} OWNER TO {q(schema_name)};"
                    )

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

    def generate_view_rebuilds(
        self,
        views: list,
        target_type: DatabaseType,
        manual_count: int = 0,
        hint_count: int = 0,
    ) -> GeneratedSqlPlan:
        """Backup existing views then recreate them from source definitions.

        Backup naming: <view_name>_YYYYMMDD (conflict -> append _1, _2 ...).
        """
        dialect = get_dialect(target_type)
        q = dialect.quote_identifier
        statements: list[str] = []
        warnings: list[str] = []
        day = datetime.now().strftime("%Y%m%d")

        for view_def in sorted(views, key=lambda v: (v.schema, v.name)):
            view_sql, view_warnings = dialect.build_create_view_sql(view_def.schema, view_def)
            if view_sql.strip().startswith("--"):
                warnings.extend(view_warnings)
                warnings.append(
                    f"视图 {q(view_def.schema)}.{q(view_def.name)} 缺少可用定义，已跳过重建。"
                )
                continue

            # 备份旧视图：view -> view_YYYYMMDD（冲突则自动追加 _n）
            statements.append(
                "DO $$ "
                "DECLARE "
                f"base_name text := {dialect.quote_literal(view_def.name + '_' + day)}; "
                "candidate text := base_name; "
                "idx int := 0; "
                "BEGIN "
                "WHILE EXISTS ("
                "SELECT 1 FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                f"WHERE n.nspname = {dialect.quote_literal(view_def.schema)} "
                "AND c.relkind IN ('v','m') "
                "AND c.relname = candidate"
                ") LOOP "
                "idx := idx + 1; "
                "candidate := base_name || '_' || idx::text; "
                "END LOOP; "
                "EXECUTE format('ALTER VIEW %I.%I RENAME TO %I', "
                f"{dialect.quote_literal(view_def.schema)}, "
                f"{dialect.quote_literal(view_def.name)}, "
                "candidate); "
                "END $$;"
            )
            statements.append(view_sql)
            post_sql, post_warnings = dialect.build_post_create_view_sql(view_def.schema, view_def)
            statements.extend(post_sql)
            warnings.extend(view_warnings)
            warnings.extend(post_warnings)

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
