"""Diff models for schema comparison results."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Tuple

from .models import ColumnDefinition, ObjectType, TableDefinition


class DiffStatus(str, Enum):
    MATCH = "match"
    ONLY_SOURCE = "only_source"
    ONLY_TARGET = "only_target"
    TYPE_MISMATCH = "type_mismatch"
    NULLABILITY_MISMATCH = "nullability_mismatch"
    DEFAULT_MISMATCH = "default_mismatch"
    UNSUPPORTED = "unsupported"


class DiffCategory(str, Enum):
    AUTO_SYNCABLE = "auto_syncable"
    SCHEMA_SYNCABLE = "schema_syncable"  # 目标端缺少整个 Schema，可生成 CREATE SCHEMA + CREATE TABLE
    TABLE_SYNCABLE = "table_syncable"   # Schema 存在但目标端缺少表/视图，可生成 CREATE TABLE/VIEW
    VIEW_REBUILD_SYNCABLE = "view_rebuild_syncable"  # 目标端视图字段缺失，通过备份+重建视图同步
    MANUAL_REQUIRED = "manual_required"
    ONLY_HINT = "only_hint"


@dataclass(frozen=True)
class ColumnDiff:
    schema: str
    object_name: str
    column_name: str
    status: DiffStatus
    category: DiffCategory
    source_column: Optional[ColumnDefinition]
    target_column: Optional[ColumnDefinition]
    object_type: Optional[ObjectType] = None
    reason: Optional[str] = None


@dataclass(frozen=True)
class ObjectDiff:
    schema: str
    object_name: str
    status: DiffStatus
    category: DiffCategory
    source_object: Optional[TableDefinition]
    target_object: Optional[TableDefinition]
    reason: Optional[str] = None


@dataclass(frozen=True)
class SchemaDiff:
    object_diffs: Tuple[ObjectDiff, ...] = field(default_factory=tuple)
    column_diffs: Tuple[ColumnDiff, ...] = field(default_factory=tuple)
