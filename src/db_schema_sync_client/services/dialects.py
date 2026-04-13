"""Database dialect helpers for SQL generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from db_schema_sync_client.domain.models import ColumnDefinition, DatabaseType, ObjectType, TableDefinition


# Types that must NOT have precision/scale appended — they are fixed-width
_NO_PRECISION_TYPES = {
    "integer", "int", "int4", "int2", "int8",
    "bigint", "smallint", "boolean", "bool",
    "text", "date", "oid", "uuid",
    "json", "jsonb", "bytea", "real",
    "double precision", "float", "float4", "float8",
    "serial", "bigserial", "smallserial",
}


@dataclass(frozen=True)
class Dialect:
    database_type: DatabaseType

    def quote_identifier(self, name: str) -> str:
        return f'"{ name.replace(chr(34), chr(34) * 2) }"'

    def quote_literal(self, value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def format_column_type(self, column: ColumnDefinition) -> str:
        data_type = column.data_type
        base = data_type.lower()

        # Fixed-width types must not have any precision suffixed
        if base in _NO_PRECISION_TYPES:
            return data_type

        if column.character_maximum_length:
            return f"{data_type}({column.character_maximum_length})"
        if column.numeric_precision is not None and column.numeric_scale is not None:
            return f"{data_type}({column.numeric_precision}, {column.numeric_scale})"
        if column.numeric_precision is not None and base in {"numeric", "decimal"}:
            return f"{data_type}({column.numeric_precision})"
        return data_type

    def build_add_column_sql(
        self,
        schema_name: str,
        table_name: str,
        column: ColumnDefinition,
    ) -> tuple[str, list[str]]:
        warnings: List[str] = []
        parts = [
            f"ALTER TABLE {self.quote_identifier(schema_name)}.{self.quote_identifier(table_name)}",
            f"ADD COLUMN {self.quote_identifier(column.name)} {self.format_column_type(column)}",
        ]

        if column.column_default:
            parts.append(f"DEFAULT {column.column_default}")

        if not column.is_nullable:
            if column.column_default:
                parts.append("NOT NULL")
            else:
                warnings.append(
                    f"Column {self.quote_identifier(schema_name)}."
                    f"{self.quote_identifier(table_name)}."
                    f"{self.quote_identifier(column.name)} requires manual NOT NULL enforcement after backfill."
                )

        if column.is_sequence_related:
            warnings.append(
                f"Column {self.quote_identifier(schema_name)}."
                f"{self.quote_identifier(table_name)}."
                f"{self.quote_identifier(column.name)} uses sequence/identity semantics and should be reviewed manually."
            )

        return " ".join(parts) + ";", warnings

    # ── Schema / Table 创建 ─────────────────────────────────────────────────

    def _get_serial_type(self, column: ColumnDefinition) -> str:
        """将序列相关字段映射为 SERIAL/BIGSERIAL/SMALLSERIAL 简写类型。
        
        返回空字符串表示无法识别，需要人工处理。
        """
        dt = column.data_type.lower()
        if dt in ("integer", "int", "int4"):
            return "SERIAL"
        if dt in ("bigint", "int8"):
            return "BIGSERIAL"
        if dt in ("smallint", "int2"):
            return "SMALLSERIAL"
        return ""

    def build_create_role_sql(self, schema_name: str, password_hash: Optional[str] = None) -> tuple[str, list[str]]:
        """生成 CREATE ROLE 语句。

        若提供 password_hash（从源库 pg_authid 读取），直接写入目标库，
        用户可用原密码登录。若无哈希则使用占位符 CHANGE_ME。
        若角色已存在，该语句将失败并显示错误，不影响后续步骤执行。
        """
        q = self.quote_identifier
        if password_hash:
            sql = f"CREATE ROLE {q(schema_name)} WITH LOGIN PASSWORD '{password_hash}';"
            warnings: List[str] = []
        else:
            sql = f"CREATE ROLE {q(schema_name)} WITH LOGIN PASSWORD 'CHANGE_ME';"
            warnings = [
                f'角色 "{schema_name}" 未能从源库读取密码哈希（需超级用户权限），'
                f"密码已设置为占位符 CHANGE_ME，请在执行前将 SQL 中的 CHANGE_ME 替换为实际密码。",
            ]
        return sql, warnings

    def build_ensure_role_sql(self, schema_name: str, password_hash: Optional[str] = None) -> tuple[str, list[str]]:
        q = self.quote_identifier
        if password_hash:
            sql = (
                "DO $$ BEGIN "
                f"IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {self.quote_literal(schema_name)}) THEN "
                f"CREATE ROLE {q(schema_name)} WITH LOGIN PASSWORD '{password_hash}'; "
                "END IF; END $$;"
            )
            warnings: List[str] = []
        else:
            sql = (
                "DO $$ BEGIN "
                f"IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {self.quote_literal(schema_name)}) THEN "
                f"CREATE ROLE {q(schema_name)} WITH LOGIN PASSWORD 'CHANGE_ME'; "
                "END IF; END $$;"
            )
            warnings = [
                f'角色 "{schema_name}" 未能从源库读取密码哈希（需超级用户权限），'
                f"如目标端不存在该角色，将以占位符 CHANGE_ME 创建，请在执行前替换为实际密码。",
            ]
        return sql, warnings

    def build_create_schema_sql(self, schema_name: str) -> tuple[str, list[str]]:
        """生成 CREATE SCHEMA 语句。

        使用 AUTHORIZATION schema_name 确保 schema 的 owner 与 schema 名称相同，
        从而通过元数据读取时 pg_get_userbyid(n.nspowner) LIKE 'df_%' 过滤条件。

        若对应角色不存在，该语句会失败并在执行结果中提示，
        需要先手动创建对应的用户/角色。
        """
        q = self.quote_identifier
        sql = f"CREATE SCHEMA IF NOT EXISTS {q(schema_name)} AUTHORIZATION {q(schema_name)};"
        warnings = [
            f'新建 Schema "{schema_name}": 若对应角色不存在，'
            f"请先执行以下语句后重新同步: "
            f"CREATE ROLE {q(schema_name)} LOGIN PASSWORD 'your_password';"
        ]
        return sql, warnings

    def build_alter_schema_owner_sql(self, schema_name: str) -> tuple[str, list[str]]:
        q = self.quote_identifier
        return f"ALTER SCHEMA {q(schema_name)} OWNER TO {q(schema_name)};", []

    def build_create_table_sql(
        self,
        schema_name: str,
        table_def: TableDefinition,
    ) -> tuple[str, list[str]]:
        """生成 CREATE TABLE 语句，自动处理序列字段。"""
        q = self.quote_identifier
        warnings: List[str] = []
        col_defs: List[str] = []

        for col in table_def.columns:
            if col.is_sequence_related:
                serial_type = self._get_serial_type(col)
                if serial_type:
                    col_defs.append(f"    {q(col.name)} {serial_type}")
                else:
                    # 无法自动映射：用原始类型，跳过 nextval 默认值，添加警告
                    col_defs.append(f"    {q(col.name)} {self.format_column_type(col)}")
                    warnings.append(
                        f"字段 {q(schema_name)}.{q(table_def.name)}.{q(col.name)} "
                        f"使用序列/identity，请在建表后手动创建序列并更新默认值。"
                    )
            else:
                col_type = self.format_column_type(col)
                col_def = f"    {q(col.name)} {col_type}"
                if col.column_default:
                    col_def += f" DEFAULT {col.column_default}"
                if not col.is_nullable:
                    col_def += " NOT NULL"
                col_defs.append(col_def)

        body = ",\n".join(col_defs)
        sql = f"CREATE TABLE IF NOT EXISTS {q(schema_name)}.{q(table_def.name)} (\n{body}\n);"
        return sql, warnings

    def build_post_create_table_sql(
        self,
        schema_name: str,
        table_def: TableDefinition,
    ) -> tuple[list[str], list[str]]:
        q = self.quote_identifier
        statements: list[str] = []
        warnings: list[str] = []

        if table_def.primary_key and table_def.primary_key.column_names:
            cols = ", ".join(q(name) for name in table_def.primary_key.column_names)
            statements.append(
                f"ALTER TABLE {q(schema_name)}.{q(table_def.name)} "
                f"ADD CONSTRAINT {q(table_def.primary_key.name)} PRIMARY KEY ({cols});"
            )

        for index_def in table_def.indexes:
            sql = index_def.definition.strip()
            if sql and not sql.endswith(";"):
                sql += ";"
            if sql:
                statements.append(sql)

        if table_def.comment:
            statements.append(
                f"COMMENT ON TABLE {q(schema_name)}.{q(table_def.name)} "
                f"IS {self.quote_literal(table_def.comment)};"
            )

        for col in table_def.columns:
            if col.comment:
                statements.append(
                    f"COMMENT ON COLUMN {q(schema_name)}.{q(table_def.name)}.{q(col.name)} "
                    f"IS {self.quote_literal(col.comment)};"
                )

        return statements, warnings

    def build_create_view_sql(
        self,
        schema_name: str,
        view_def: TableDefinition,
    ) -> tuple[str, list[str]]:
        """生成 CREATE VIEW 语句。

        若视图定义 SQL 可用，则生成完整的 CREATE OR REPLACE VIEW。
        若无定义（元数据读取未返回视图内容），生成注释并添加警告。
        """
        q = self.quote_identifier
        warnings: List[str] = []
        view_def_sql = (view_def.view_definition or "").strip()

        if view_def_sql:
            # information_schema.views 返回的定义不包含 SELECT 前的屣尊空白
            sql = (
                f"CREATE OR REPLACE VIEW {q(schema_name)}.{q(view_def.name)} AS\n"
                f"{view_def_sql}"
            )
            if not sql.rstrip().endswith(";"):
                sql += ";"
        else:
            # 无定义：生成占位注释语句（不可执行，作为提示）
            sql = f"-- 请手动创建视图: CREATE OR REPLACE VIEW {q(schema_name)}.{q(view_def.name)} AS <view_definition>;"
            warnings.append(
                f'视图 "{schema_name}"."{view_def.name}" 的定义 SQL 未读取到，已生成占位注释。'
            )
        return sql, warnings

    def build_post_create_view_sql(
        self,
        schema_name: str,
        view_def: TableDefinition,
    ) -> tuple[list[str], list[str]]:
        q = self.quote_identifier
        statements: list[str] = []
        if view_def.comment:
            statements.append(
                f"COMMENT ON VIEW {q(schema_name)}.{q(view_def.name)} "
                f"IS {self.quote_literal(view_def.comment)};"
            )
        for col in view_def.columns:
            if col.comment:
                statements.append(
                    f"COMMENT ON COLUMN {q(schema_name)}.{q(view_def.name)}.{q(col.name)} "
                    f"IS {self.quote_literal(col.comment)};"
                )
        return statements, []


class PostgreSQLDialect(Dialect):
    pass


class KingBaseDialect(Dialect):
    pass


def get_dialect(database_type: DatabaseType) -> Dialect:
    if database_type == DatabaseType.POSTGRESQL:
        return PostgreSQLDialect(database_type=database_type)
    if database_type == DatabaseType.KINGBASE:
        return KingBaseDialect(database_type=database_type)
    raise ValueError(f"Unsupported database type: {database_type}")
