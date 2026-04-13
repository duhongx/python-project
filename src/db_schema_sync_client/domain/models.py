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


@dataclass(frozen=True)
class TableDefinition:
    schema: str
    name: str
    object_type: ObjectType
    columns: Tuple[ColumnDefinition, ...] = field(default_factory=tuple)
    view_definition: Optional[str] = None  # 视图 DDL，僅视图对象有效

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

    @property
    def qualified_objects(self) -> Dict[str, TableDefinition]:
        return {table.qualified_name: table for table in self.tables}
