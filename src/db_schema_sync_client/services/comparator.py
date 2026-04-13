"""Schema comparison service."""

from __future__ import annotations

from typing import Dict, List, Set

from db_schema_sync_client.domain.diff import ColumnDiff, DiffCategory, DiffStatus, ObjectDiff, SchemaDiff
from db_schema_sync_client.domain.models import ColumnDefinition, ObjectType, SchemaSnapshot, TableDefinition


class SchemaComparator:
    def compare(self, source: SchemaSnapshot, target: SchemaSnapshot) -> SchemaDiff:
        object_diffs: List[ObjectDiff] = []
        column_diffs: List[ColumnDiff] = []

        source_objects = source.qualified_objects
        target_objects = target.qualified_objects
        target_schemas: Set[str] = {item.schema for item in target.tables}
        source_schemas: Set[str] = {item.schema for item in source.tables}

        for qualified_name, source_object in source_objects.items():
            target_object = target_objects.get(qualified_name)
            if target_object is None:
                object_diffs.append(self._build_missing_object_diff(source_object, target_schemas))
                continue

            column_diffs.extend(self._compare_columns(source_object, target_object))

        for qualified_name, target_object in target_objects.items():
            if qualified_name in source_objects:
                continue

            object_diffs.append(
                ObjectDiff(
                    schema=target_object.schema,
                    object_name=target_object.name,
                    status=DiffStatus.ONLY_TARGET,
                    category=DiffCategory.ONLY_HINT,
                    source_object=None,
                    target_object=target_object,
                    reason=(
                        "extra_schema" if target_object.schema not in source_schemas else "extra_object"
                    ),
                )
            )

        return SchemaDiff(
            object_diffs=tuple(object_diffs),
            column_diffs=tuple(column_diffs),
        )

    def _build_missing_object_diff(
        self,
        source_object: TableDefinition,
        target_schemas: Set[str],
    ) -> ObjectDiff:
        if source_object.schema not in target_schemas:
            reason = "missing_schema"
            # 整个 Schema 缺失：可自动生成 CREATE SCHEMA + CREATE TABLE
            category = DiffCategory.SCHEMA_SYNCABLE
        elif source_object.object_type == ObjectType.TABLE:
            reason = "missing_table"
            category = DiffCategory.TABLE_SYNCABLE
        else:
            reason = "missing_view"
            category = DiffCategory.TABLE_SYNCABLE

        return ObjectDiff(
            schema=source_object.schema,
            object_name=source_object.name,
            status=DiffStatus.ONLY_SOURCE,
            category=category,
            source_object=source_object,
            target_object=None,
            reason=reason,
        )

    def _compare_columns(
        self,
        source_object: TableDefinition,
        target_object: TableDefinition,
    ) -> List[ColumnDiff]:
        diffs: List[ColumnDiff] = []
        source_columns: Dict[str, ColumnDefinition] = source_object.columns_by_name
        target_columns: Dict[str, ColumnDefinition] = target_object.columns_by_name

        for column_name, source_column in source_columns.items():
            target_column = target_columns.get(column_name)
            if target_column is None:
                diffs.append(
                    ColumnDiff(
                        schema=source_object.schema,
                        object_name=source_object.name,
                        column_name=column_name,
                        status=DiffStatus.ONLY_SOURCE,
                        category=self._missing_target_column_category(source_object, source_column),
                        source_column=source_column,
                        target_column=None,
                        object_type=source_object.object_type,
                        reason="missing_target_column",
                    )
                )
                continue

            if not self._same_data_type(source_column, target_column):
                diffs.append(
                    self._manual_column_diff(
                        source_object,
                        column_name,
                        source_column,
                        target_column,
                        DiffStatus.TYPE_MISMATCH,
                    )
                )
            if source_column.is_nullable != target_column.is_nullable:
                diffs.append(
                    self._manual_column_diff(
                        source_object,
                        column_name,
                        source_column,
                        target_column,
                        DiffStatus.NULLABILITY_MISMATCH,
                    )
                )
            if (source_column.column_default or None) != (target_column.column_default or None):
                diffs.append(
                    self._manual_column_diff(
                        source_object,
                        column_name,
                        source_column,
                        target_column,
                        DiffStatus.DEFAULT_MISMATCH,
                    )
                )

        for column_name, target_column in target_columns.items():
            if column_name in source_columns:
                continue
            diffs.append(
                ColumnDiff(
                    schema=target_object.schema,
                    object_name=target_object.name,
                    column_name=column_name,
                    status=DiffStatus.ONLY_TARGET,
                    category=DiffCategory.ONLY_HINT,
                    source_column=None,
                    target_column=target_column,
                    object_type=target_object.object_type,
                    reason="extra_target_column",
                )
            )

        return diffs

    def _manual_column_diff(
        self,
        source_object: TableDefinition,
        column_name: str,
        source_column: ColumnDefinition,
        target_column: ColumnDefinition,
        status: DiffStatus,
    ) -> ColumnDiff:
        return ColumnDiff(
            schema=source_object.schema,
            object_name=source_object.name,
            column_name=column_name,
            status=status,
            category=DiffCategory.MANUAL_REQUIRED,
            source_column=source_column,
            target_column=target_column,
            object_type=source_object.object_type,
            reason=status.value,
        )

    def _missing_target_column_category(
        self,
        source_object: TableDefinition,
        source_column: ColumnDefinition,
    ) -> DiffCategory:
        if source_object.object_type == ObjectType.VIEW:
            return DiffCategory.VIEW_REBUILD_SYNCABLE
        if source_object.object_type != ObjectType.TABLE:
            return DiffCategory.ONLY_HINT
        if source_column.is_sequence_related:
            return DiffCategory.MANUAL_REQUIRED
        return DiffCategory.AUTO_SYNCABLE

    def _same_data_type(self, left: ColumnDefinition, right: ColumnDefinition) -> bool:
        return (
            left.data_type == right.data_type
            and left.character_maximum_length == right.character_maximum_length
            and left.numeric_precision == right.numeric_precision
            and left.numeric_scale == right.numeric_scale
        )
