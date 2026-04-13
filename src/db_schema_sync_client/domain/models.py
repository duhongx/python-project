"""Core domain models for schema comparison."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Tuple


class DatabaseType(str, Enum):
    POSTGRESQL = "postgresql"
    KINGBASE = "kingbase"


class ObjectType(str, Enum):
    TABLE = "table"
    VIEW = "view"


class ConnectionRole(str, Enum):
    SOURCE = "source"
    TARGET = "target"


@dataclass(frozen=True)
class PrimaryKeyDefinition:
    name: str
    column_names: Tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class IndexDefinition:
    name: str
    definition: str
    is_unique: bool = False


@dataclass(frozen=True)
class ConnectionProfile:
    name: str
    role: ConnectionRole
    db_type: DatabaseType
    host: str
    port: int
    database: str
    username: str
    schema_prefix: str = "df_"
    owner_prefix: str = "df_"
    # 精确 Schema 名称过滤，逗号或斜杠分隔，如 "df_etl,df_esb"；空字符串表示不限制
    schema_names_filter: str = ""
    id: Optional[int] = None
    credential_key: Optional[str] = None
    is_default: bool = False
    last_test_status: Optional[str] = None
    last_test_message: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "schema_prefix", self.schema_prefix or "df_")
        object.__setattr__(self, "owner_prefix", self.owner_prefix or "df_")


@dataclass(frozen=True)
class ColumnDefinition:
    name: str
    ordinal_position: int
    data_type: str
    character_maximum_length: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]
    is_nullable: bool
    column_default: Optional[str]
    is_sequence_related: bool = False
    comment: Optional[str] = None


@dataclass(frozen=True)
class TableDefinition:
    schema: str
    name: str
    object_type: ObjectType
    columns: Tuple[ColumnDefinition, ...] = field(default_factory=tuple)
    view_definition: Optional[str] = None  # 视图 DDL，僅视图对象有效
    comment: Optional[str] = None
    primary_key: Optional[PrimaryKeyDefinition] = None
    indexes: Tuple[IndexDefinition, ...] = field(default_factory=tuple)

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def columns_by_name(self) -> Dict[str, ColumnDefinition]:
        return {column.name: column for column in self.columns}


@dataclass(frozen=True)
class SchemaSnapshot:
    database_name: str
    tables: Tuple[TableDefinition, ...] = field(default_factory=tuple)
    schema_owners: Dict[str, str] = field(default_factory=dict)
    # schema_name → 密码哈希（从源库 pg_authid 读取，可直接用于 CREATE ROLE ... PASSWORD）
    # 若源库无超级用户权限则为空 dict
    role_hashes: Dict[str, str] = field(default_factory=dict)

    @property
    def qualified_objects(self) -> Dict[str, TableDefinition]:
        return {table.qualified_name: table for table in self.tables}
