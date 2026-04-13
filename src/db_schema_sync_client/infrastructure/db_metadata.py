"""Database metadata loading and row parsing."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from db_schema_sync_client.domain.models import (
    ColumnDefinition,
    ConnectionProfile,
    IndexDefinition,
    ObjectType,
    PrimaryKeyDefinition,
    SchemaSnapshot,
    TableDefinition,
)

from .db_connection import DatabaseConnectionFactory


def parse_table_row(row: Any) -> TableDefinition:
    table_type = row["table_type"]
    object_type = ObjectType.TABLE if table_type == "BASE TABLE" else ObjectType.VIEW
    return TableDefinition(
        schema=row["table_schema"],
        name=row["table_name"],
        object_type=object_type,
        columns=(),
        comment=row.get("table_comment"),
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
        comment=row.get("column_comment"),
    )


@dataclass(frozen=True)
class MetadataFilters:
    # 支持多个前缀（逗号分隔后转为 tuple），空 tuple 表示不过滤前缀
    schema_prefixes: tuple[str, ...] = ("df_",)
    # owner 前缀：空 tuple 时自动跟随 schema_prefixes（schema = 用户名的场景）
    owner_prefixes: tuple[str, ...] = ()
    schema_names: tuple[str, ...] = field(default_factory=tuple)
    # 排除的 schema 名称（精确匹配），这些 schema 不参与比对/同步
    exclude_schema_names: tuple[str, ...] = field(default_factory=tuple)
    object_name_keyword: Optional[str] = None
    include_tables: bool = True
    include_views: bool = True
    filter_owner_prefix: bool = True

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
        if not self.filter_owner_prefix:
            return ()
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
            primary_keys = self._load_primary_keys(conn, active_filters)
            indexes = self._load_indexes(conn, active_filters)
            role_hash_rows = self._load_role_hashes(conn, active_filters)
        finally:
            conn.close()

        columns_by_object: dict[tuple[str, str], list[ColumnDefinition]] = defaultdict(list)
        for row in columns:
            columns_by_object[(row["table_schema"], row["table_name"])].append(parse_column_row(row))

        view_def_map: dict[tuple[str, str], str] = {}
        for row in view_defs:
            view_def_map[(row["table_schema"], row["table_name"])] = row["view_definition"] or ""

        primary_key_map: dict[tuple[str, str], PrimaryKeyDefinition] = {}
        for row in primary_keys:
            primary_key_map[(row["table_schema"], row["table_name"])] = PrimaryKeyDefinition(
                name=row["constraint_name"],
                column_names=tuple(row["column_names"] or ()),
            )

        indexes_map: dict[tuple[str, str], list[IndexDefinition]] = defaultdict(list)
        for row in indexes:
            indexes_map[(row["table_schema"], row["table_name"])] .append(
                IndexDefinition(
                    name=row["index_name"],
                    definition=row["index_definition"],
                    is_unique=bool(row["is_unique"]),
                )
            )

        snapshot_tables = []
        schema_owners: dict[str, str] = {}
        for row in tables:
            table = parse_table_row(row)
            owner = row.get("schema_owner")
            if owner:
                schema_owners[table.schema] = owner
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
                    comment=table.comment,
                    primary_key=primary_key_map.get((table.schema, table.name)),
                    indexes=tuple(indexes_map.get((table.schema, table.name), [])),
                )
            )

        role_hashes: dict[str, str] = {}
        for row in role_hash_rows:
            name = row["rolname"]
            pw = row["rolpassword"]
            if name and pw:
                role_hashes[name] = pw

        return SchemaSnapshot(
            database_name=profile.database,
            tables=tuple(snapshot_tables),
            schema_owners=schema_owners,
            role_hashes=role_hashes,
        )

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
        if filters.exclude_schema_names:
            conditions.append("t.table_schema != ALL(%s)")
            params.append(list(filters.exclude_schema_names))
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
            SELECT
                t.table_schema,
                t.table_name,
                t.table_type,
                pg_get_userbyid(n.nspowner) AS schema_owner,
                pgd.description AS table_comment
            FROM information_schema.tables t
            JOIN pg_namespace n ON n.nspname = t.table_schema
            JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = t.table_name
            LEFT JOIN pg_description pgd ON pgd.objoid = c.oid AND pgd.objsubid = 0
            WHERE {" AND ".join(conditions)}
            ORDER BY t.table_schema, t.table_name
        """
        with self._cursor(conn) as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def _load_role_hashes(self, conn: Any, filters: MetadataFilters) -> Iterable[Any]:
        """从 pg_authid 读取匹配前缀的角色密码哈希。

        需要超级用户权限。若权限不足则静默返回空列表，
        此时 build_create_role_sql 将回退到占位符 CHANGE_ME。
        """
        conditions: list[str] = ["r.rolpassword IS NOT NULL", "r.rolcanlogin = TRUE"]
        params: list[Any] = []

        if filters.schema_prefixes:
            prefix_clauses = " OR ".join(
                "r.rolname LIKE %s" for _ in filters.schema_prefixes
            )
            conditions.append(f"({prefix_clauses})")
            params.extend(f"{p}%" for p in filters.schema_prefixes)
        elif filters._effective_owner_prefixes:
            prefix_clauses = " OR ".join(
                "r.rolname LIKE %s" for _ in filters._effective_owner_prefixes
            )
            conditions.append(f"({prefix_clauses})")
            params.extend(f"{p}%" for p in filters._effective_owner_prefixes)

        sql = f"""
            SELECT r.rolname, r.rolpassword
            FROM pg_authid r
            WHERE {" AND ".join(conditions)}
            ORDER BY r.rolname
        """
        try:
            with self._cursor(conn) as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()
        except Exception:
            # 无超级用户权限时（pg_authid 不可读），静默回退
            return []

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
        if filters.exclude_schema_names:
            conditions.append("v.table_schema != ALL(%s)")
            params.append(list(filters.exclude_schema_names))

        sql = f"""
            SELECT v.table_schema, v.table_name, v.view_definition
            FROM information_schema.views v
            WHERE {" AND ".join(conditions) if conditions else "1=1"}
            ORDER BY v.table_schema, v.table_name
        """
        with self._cursor(conn) as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def _load_columns(self, conn: Any, filters: MetadataFilters) -> Iterable[Any]:
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
        if filters.exclude_schema_names:
            conditions.append("c.table_schema != ALL(%s)")
            params.append(list(filters.exclude_schema_names))
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
                c.column_default,
                pgd.description AS column_comment
            FROM information_schema.columns c
            JOIN pg_namespace n ON n.nspname = c.table_schema
            JOIN pg_class cls ON cls.relnamespace = n.oid AND cls.relname = c.table_name
            JOIN pg_attribute a ON a.attrelid = cls.oid AND a.attname = c.column_name
            LEFT JOIN pg_description pgd ON pgd.objoid = cls.oid AND pgd.objsubid = a.attnum
            WHERE {" AND ".join(conditions) if conditions else "1=1"}
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
        """
        with self._cursor(conn) as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def _load_primary_keys(self, conn: Any, filters: MetadataFilters) -> Iterable[Any]:
        conditions: list[str] = ["con.contype = 'p'"]
        params: list[Any] = []

        if filters.schema_prefixes:
            prefix_clauses = " OR ".join(
                "ns.nspname LIKE %s" for _ in filters.schema_prefixes
            )
            conditions.append(f"({prefix_clauses})")
            params.extend(f"{p}%" for p in filters.schema_prefixes)
        if filters.schema_names:
            conditions.append("ns.nspname = ANY(%s)")
            params.append(list(filters.schema_names))
        if filters.exclude_schema_names:
            conditions.append("ns.nspname != ALL(%s)")
            params.append(list(filters.exclude_schema_names))
        if filters.object_name_keyword:
            conditions.append("cls.relname ILIKE %s")
            params.append(f"%{filters.object_name_keyword}%")

        sql = f"""
            SELECT
                ns.nspname AS table_schema,
                cls.relname AS table_name,
                con.conname AS constraint_name,
                ARRAY_AGG(att.attname ORDER BY ord.ordinality) AS column_names
            FROM pg_constraint con
            JOIN pg_class cls ON cls.oid = con.conrelid
            JOIN pg_namespace ns ON ns.oid = cls.relnamespace
            JOIN unnest(con.conkey) WITH ORDINALITY AS ord(attnum, ordinality) ON TRUE
            JOIN pg_attribute att ON att.attrelid = cls.oid AND att.attnum = ord.attnum
            WHERE {" AND ".join(conditions)}
            GROUP BY ns.nspname, cls.relname, con.conname
            ORDER BY ns.nspname, cls.relname
        """
        with self._cursor(conn) as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def _load_indexes(self, conn: Any, filters: MetadataFilters) -> Iterable[Any]:
        conditions: list[str] = ["NOT i.indisprimary"]
        params: list[Any] = []

        if filters.schema_prefixes:
            prefix_clauses = " OR ".join(
                "ns.nspname LIKE %s" for _ in filters.schema_prefixes
            )
            conditions.append(f"({prefix_clauses})")
            params.extend(f"{p}%" for p in filters.schema_prefixes)
        if filters.schema_names:
            conditions.append("ns.nspname = ANY(%s)")
            params.append(list(filters.schema_names))
        if filters.exclude_schema_names:
            conditions.append("ns.nspname != ALL(%s)")
            params.append(list(filters.exclude_schema_names))
        if filters.object_name_keyword:
            conditions.append("tbl.relname ILIKE %s")
            params.append(f"%{filters.object_name_keyword}%")

        sql = f"""
            SELECT
                ns.nspname AS table_schema,
                tbl.relname AS table_name,
                idx.relname AS index_name,
                i.indisunique AS is_unique,
                pg_get_indexdef(i.indexrelid) AS index_definition
            FROM pg_index i
            JOIN pg_class tbl ON tbl.oid = i.indrelid
            JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
            JOIN pg_class idx ON idx.oid = i.indexrelid
            WHERE {" AND ".join(conditions)}
            ORDER BY ns.nspname, tbl.relname, idx.relname
        """
        with self._cursor(conn) as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()
