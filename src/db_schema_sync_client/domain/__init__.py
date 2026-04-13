"""Domain models for schema comparison."""
"""Domain layer exports."""

from .diff import ColumnDiff, DiffCategory, DiffStatus, ObjectDiff, SchemaDiff
from .models import (
    ColumnDefinition,
    ConnectionProfile,
    ConnectionRole,
    DatabaseType,
    ObjectType,
    SchemaSnapshot,
    TableDefinition,
)

__all__ = [
    "ColumnDefinition",
    "ColumnDiff",
    "ConnectionProfile",
    "ConnectionRole",
    "DatabaseType",
    "DiffCategory",
    "DiffStatus",
    "ObjectDiff",
    "ObjectType",
    "SchemaDiff",
    "SchemaSnapshot",
    "TableDefinition",
]
