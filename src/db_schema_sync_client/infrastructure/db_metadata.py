"""Database metadata loading and row parsing."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from db_schema_sync_client.domain.models import ColumnDefinition, ConnectionProfile, ObjectType, SchemaSnapshot, TableDefinition

from .db_connection import DatabaseConnectionFactory


def parse_table_row(row: Any) -> TableDefinition:
    table_type = row["table_type"]
    object_type = ObjectType.TABLE if table_type == "BASE TABLE" else ObjectType.VIEW
    return TableDefinition(
        schema=row["table_schema"],
        name=row["table_name"],
        object_type=object_type,
        columns=(),
    )


def parse_column_row(row: Any) -> ColumnDefinition:
    default_value = row["column_default"]
    return ColumnDefinition(
        name=row["column_name"],
        ordinal_position=row["ordinal_position"],
        data_type=row["data_type"],
        character_maximum_length=row["character_maximum_length"],
        numeric_precision=row["numeric_precision"],
        numeric_scale=row["numeric_scale"],
        is_nullable=row["is_nullable"] == "YES",
        column_default=default_value,
        is_sequence_related=bool(default_value and "nextval(" in default_value),
    )


@dataclass(frozen=True)
class MetadataFilters:
    # 支持多个前缀（逗号分隔后转为 tuple），空 tuple 表示不过滤前缀
    schema_prefixes: tuple[str, ...] = ("df_",)
    # owner 前缀：空 tuple 时自动跟随 schema_prefixes（schema = 用户名的场景）
    owner_prefixes: tuple[str, ...] = ()
    schema_names: tuple[str, ...] = field(default_factory=tuple)
    object_name_keyword: Optional[str] = None
    include_tables: bool = True
    include_views: bool = True

    # ── 向后兼容属性 ───────────────────────────────────────────────────
    @property
    def schema_prefix(self) -> str:
        """第一个 schema 前缀（向后兼容用）。"""
        return self.schema_prefixes[0] if self.schema_prefixes else ""

    @property
    def owner_prefix(self) -> str:
        """第一个 owner 前缀（向后兼容用）。"""
        effective = self.owner_prefixes if self.owner_prefixes else self.schema_prefixes
        return effective[0] if effective else ""

    @property
    def _effective_owner_prefixes(self) -> tuple[str, ...]:
        """实际使用的 owner 前缀列表；空时跟随 schema_prefixes。"""
        return self.owner_prefixes if self.owner_prefixes else self.schema_prefixes

    @staticmethod
    def from_prefix_text(text: str, **kwargs) -> "MetadataFilters":
        """将逗号分隔的前缀字符串解析为 MetadataFilters。
        
        例如 ``from_prefix_text("df_,jk_")`` 会同时过滤两个前缀。
        空字符串表示不限制前缀。
        """
        parts = tuple(p.strip() for p in text.split(",") if p.strip())
        return MetadataFilters(schema_prefixes=parts, **kwargs)


class MetadataReader:
    def __init__(self, connection_factory: Optional[DatabaseConnectionFactory] = None) -> None:
        self.connection_factory = connection_factory or DatabaseConnectionFactory()

    def load_snapshot(
        self,
        profile: ConnectionProfile,
        password: str,
        filters: Optional[MetadataFilters] = None,
    ) -> SchemaSnapshot:
        active_filters = filters or MetadataFilters(
            schema_prefixes=(profile.schema_prefix,),
            owner_prefixes=(profile.owner_prefix,),
        )
        conn = self.connection_factory.connect(profile, password)
        try:
            tables = self._load_tables(conn, active_filters)
            columns = self._load_columns(conn, active_filters)
            view_defs = self._load_view_definitions(conn, active_filters)
        finally:
            conn.close()

        columns_by_object: dict[tuple[str, str], list[ColumnDefinition]] = defaultdict(list)
        for row in columns:
            columns_by_object[(row["table_schema"], row["table_name"])].append(parse_column_row(row))

        view_def_map: dict[tuple[str, str], str] = {}
        for row in view_defs:
            view_def_map[(row["table_schema"], row["table_name"])] = row["view_definition"] or ""

        snapshot_tables = []
        for row in tables:
            table = parse_table_row(row)
            table_columns = tuple(
                sorted(
                    columns_by_object[(table.schema, table.name)],
                    key=lambda item: item.ordinal_position,
                )
            )
            snapshot_tables.append(
                TableDefinition(
                    schema=table.schema,
                    name=table.name,
                    object_type=table.object_type,
                    columns=table_columns,
                    view_definition=view_def_map.get((table.schema, table.name)),
                )
            )

        return SchemaSnapshot(database_name=profile.database, tables=tuple(snapshot_tables))

    def _cursor(self, conn: Any):
        """Return a dict-based cursor so rows support string-key access."""
        try:
            import psycopg2.extras  # type: ignore
            return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        except Exception:
            return conn.cursor()

    def _load_tables(self, conn: Any, filters: MetadataFilters) -> Iterable[Any]:
        conditions: list[str] = []
        params: list[Any] = []

        # schema 前缀条件：支持多个前缀
        if filters.schema_prefixes:
            prefix_clauses = " OR ".join(
                "t.table_schema LIKE %s" for _ in filters.schema_prefixes
            )
            conditions.append(f"({prefix_clauses})")
            params.extend(f"{p}%" for p in filters.schema_prefixes)
        # else: 无前缀限制，查全部 schema

        conditions.append("(t.table_type = 'BASE TABLE' OR t.table_type = 'VIEW')")

        if filters.schema_names:
            conditions.append("t.table_schema = ANY(%s)")
            params.append(list(filters.schema_names))
        if filters.object_name_keyword:
            conditions.append("t.table_name ILIKE %s")
            params.append(f"%{filters.object_name_keyword}%")
        if not filters.include_tables:
            conditions.append("t.table_type <> 'BASE TABLE'")
        if not filters.include_views:
            conditions.append("t.table_type <> 'VIEW'")

        # owner 前缀条件：schema = 用户名时与 schema 前缀一致，支持多个前缀
        effective_owner_prefixes = filters._effective_owner_prefixes
        if effective_owner_prefixes:
            owner_clauses = " OR ".join(
                "pg_get_userbyid(n.nspowner) LIKE %s" for _ in effective_owner_prefixes
            )
            conditions.append(f"({owner_clauses})")
            params.extend(f"{p}%" for p in effective_owner_prefixes)

        sql = f"""
            SELECT t.table_schema, t.table_name, t.table_type
            FROM information_schema.tables t
            JOIN pg_namespace n ON n.nspname = t.table_schema
            WHERE {" AND ".join(conditions)}
            ORDER BY t.table_schema, t.table_name
        """
        with self._cursor(conn) as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def _load_view_definitions(self, conn: Any, filters: MetadataFilters) -> Iterable[Any]:
        """Load view DDL from information_schema.views matching the same schema prefix filters."""
        conditions: list[str] = []
        params: list[Any] = []

        if filters.schema_prefixes:
            prefix_clauses = " OR ".join(
                "v.table_schema LIKE %s" for _ in filters.schema_prefixes
            )
            conditions.append(f"({prefix_clauses})")
            params.extend(f"{p}%" for p in filters.schema_prefixes)

        if filters.schema_names:
            conditions.append("v.table_schema = ANY(%s)")
            params.append(list(filters.schema_names))

        sql = f"""
            SELECT v.table_schema, v.table_name, v.view_definition
            FROM information_schema.views v
            WHERE {" AND ".join(conditions) if conditions else "1=1"}
            ORDER BY v.table_schema, v.table_name
        """
        with self._cursor(conn) as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()
        conditions: list[str] = []
        params: list[Any] = []

        # schema 前缀条件：支持多个前缀
        if filters.schema_prefixes:
            prefix_clauses = " OR ".join(
                "c.table_schema LIKE %s" for _ in filters.schema_prefixes
            )
            conditions.append(f"({prefix_clauses})")
            params.extend(f"{p}%" for p in filters.schema_prefixes)
        # else: 无前缀限制

        if filters.schema_names:
            conditions.append("c.table_schema = ANY(%s)")
            params.append(list(filters.schema_names))
        if filters.object_name_keyword:
            conditions.append("c.table_name ILIKE %s")
            params.append(f"%{filters.object_name_keyword}%")

        sql = f"""
            SELECT
                c.table_schema,
                c.table_name,
                c.column_name,
                c.ordinal_position,
                c.data_type,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.is_nullable,
                c.column_default
            FROM information_schema.columns c
            WHERE {" AND ".join(conditions) if conditions else "1=1"}
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
        """
        with self._cursor(conn) as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()
