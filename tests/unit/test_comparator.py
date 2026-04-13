from db_schema_sync_client.domain.diff import ColumnDiff, DiffCategory, DiffStatus
from db_schema_sync_client.domain.models import (
    ColumnDefinition,
    ConnectionRole,
    ConnectionProfile,
    DatabaseType,
    ObjectType,
    SchemaSnapshot,
    TableDefinition,
)
from db_schema_sync_client.services.comparator import SchemaComparator


def test_database_type_values():
    assert DatabaseType.POSTGRESQL.value == "postgresql"
    assert DatabaseType.KINGBASE.value == "kingbase"


def test_diff_category_auto_syncable_value():
    assert DiffCategory.AUTO_SYNCABLE.value == "auto_syncable"


def test_connection_role_values():
    assert ConnectionRole.SOURCE.value == "source"
    assert ConnectionRole.TARGET.value == "target"


def test_object_type_values():
    assert ObjectType.TABLE.value == "table"
    assert ObjectType.VIEW.value == "view"


def test_missing_target_column_diff_has_source_column_only():
    source_column = ColumnDefinition(
        name="display_name",
        ordinal_position=2,
        data_type="character varying",
        character_maximum_length=100,
        numeric_precision=None,
        numeric_scale=None,
        is_nullable=True,
        column_default=None,
        is_sequence_related=False,
    )

    diff = ColumnDiff(
        schema="df_test",
        object_name="users",
        column_name="display_name",
        status=DiffStatus.ONLY_SOURCE,
        category=DiffCategory.AUTO_SYNCABLE,
        source_column=source_column,
        target_column=None,
    )

    assert diff.source_column == source_column
    assert diff.target_column is None


def test_schema_snapshot_indexes_tables_by_qualified_name():
    column = ColumnDefinition(
        name="id",
        ordinal_position=1,
        data_type="integer",
        character_maximum_length=None,
        numeric_precision=32,
        numeric_scale=0,
        is_nullable=False,
        column_default="nextval('df_test.users_id_seq'::regclass)",
        is_sequence_related=True,
    )
    table = TableDefinition(
        schema="df_test",
        name="users",
        object_type=ObjectType.TABLE,
        columns=(column,),
    )

    snapshot = SchemaSnapshot(database_name="demo", tables=(table,))

    assert snapshot.qualified_objects["df_test.users"] == table


def test_connection_profile_normalizes_prefix_defaults():
    profile = ConnectionProfile(
        name="source-main",
        role=ConnectionRole.SOURCE,
        db_type=DatabaseType.POSTGRESQL,
        host="127.0.0.1",
        port=5432,
        database="demo",
        username="demo_user",
        schema_prefix="",
        owner_prefix="",
    )

    assert profile.schema_prefix == "df_"
    assert profile.owner_prefix == "df_"


def test_compare_missing_schema_is_hint_only():
    source = SchemaSnapshot(
        database_name="source",
        tables=(
            TableDefinition(
                schema="df_source",
                name="users",
                object_type=ObjectType.TABLE,
                columns=(),
            ),
        ),
    )
    target = SchemaSnapshot(database_name="target", tables=())

    diff = SchemaComparator().compare(source, target)

    assert len(diff.object_diffs) == 1
    assert diff.object_diffs[0].status == DiffStatus.ONLY_SOURCE
    assert diff.object_diffs[0].category == DiffCategory.SCHEMA_SYNCABLE
    assert diff.object_diffs[0].reason == "missing_schema"


def test_compare_missing_table_is_table_syncable():
    source = SchemaSnapshot(
        database_name="source",
        tables=(
            TableDefinition(
                schema="df_demo",
                name="users",
                object_type=ObjectType.TABLE,
                columns=(),
            ),
        ),
    )
    target = SchemaSnapshot(
        database_name="target",
        tables=(
            TableDefinition(
                schema="df_demo",
                name="audit_log",
                object_type=ObjectType.TABLE,
                columns=(),
            ),
        ),
    )

    diff = SchemaComparator().compare(source, target)

    assert diff.object_diffs[0].object_name == "users"
    assert diff.object_diffs[0].status == DiffStatus.ONLY_SOURCE
    assert diff.object_diffs[0].category == DiffCategory.TABLE_SYNCABLE
    assert diff.object_diffs[0].reason == "missing_table"


def test_compare_missing_target_table_field_is_auto_syncable():
    source = SchemaSnapshot(
        database_name="source",
        tables=(
            TableDefinition(
                schema="df_demo",
                name="users",
                object_type=ObjectType.TABLE,
                columns=(
                    ColumnDefinition(
                        name="display_name",
                        ordinal_position=1,
                        data_type="character varying",
                        character_maximum_length=100,
                        numeric_precision=None,
                        numeric_scale=None,
                        is_nullable=True,
                        column_default=None,
                        is_sequence_related=False,
                    ),
                ),
            ),
        ),
    )
    target = SchemaSnapshot(
        database_name="target",
        tables=(
            TableDefinition(
                schema="df_demo",
                name="users",
                object_type=ObjectType.TABLE,
                columns=(),
            ),
        ),
    )

    diff = SchemaComparator().compare(source, target)

    assert diff.column_diffs[0].status == DiffStatus.ONLY_SOURCE
    assert diff.column_diffs[0].category == DiffCategory.AUTO_SYNCABLE
    assert diff.column_diffs[0].column_name == "display_name"


def test_compare_target_only_field_is_hint_only():
    source = SchemaSnapshot(
        database_name="source",
        tables=(
            TableDefinition(
                schema="df_demo",
                name="users",
                object_type=ObjectType.TABLE,
                columns=(),
            ),
        ),
    )
    target = SchemaSnapshot(
        database_name="target",
        tables=(
            TableDefinition(
                schema="df_demo",
                name="users",
                object_type=ObjectType.TABLE,
                columns=(
                    ColumnDefinition(
                        name="legacy_code",
                        ordinal_position=1,
                        data_type="character varying",
                        character_maximum_length=20,
                        numeric_precision=None,
                        numeric_scale=None,
                        is_nullable=True,
                        column_default=None,
                        is_sequence_related=False,
                    ),
                ),
            ),
        ),
    )

    diff = SchemaComparator().compare(source, target)

    assert diff.column_diffs[0].status == DiffStatus.ONLY_TARGET
    assert diff.column_diffs[0].category == DiffCategory.ONLY_HINT


def test_compare_type_nullability_and_default_mismatches_require_manual_work():
    source_column = ColumnDefinition(
        name="amount",
        ordinal_position=1,
        data_type="numeric",
        character_maximum_length=None,
        numeric_precision=12,
        numeric_scale=2,
        is_nullable=False,
        column_default="0",
        is_sequence_related=False,
    )
    target_column = ColumnDefinition(
        name="amount",
        ordinal_position=1,
        data_type="integer",
        character_maximum_length=None,
        numeric_precision=32,
        numeric_scale=0,
        is_nullable=True,
        column_default=None,
        is_sequence_related=False,
    )

    source = SchemaSnapshot(
        database_name="source",
        tables=(
            TableDefinition(
                schema="df_demo",
                name="orders",
                object_type=ObjectType.TABLE,
                columns=(source_column,),
            ),
        ),
    )
    target = SchemaSnapshot(
        database_name="target",
        tables=(
            TableDefinition(
                schema="df_demo",
                name="orders",
                object_type=ObjectType.TABLE,
                columns=(target_column,),
            ),
        ),
    )

    diff = SchemaComparator().compare(source, target)

    statuses = {item.status for item in diff.column_diffs}
    categories = {item.category for item in diff.column_diffs}

    assert statuses == {
        DiffStatus.TYPE_MISMATCH,
        DiffStatus.NULLABILITY_MISMATCH,
        DiffStatus.DEFAULT_MISMATCH,
    }
    assert categories == {DiffCategory.MANUAL_REQUIRED}


def test_view_missing_target_column_is_hint_and_marked_as_view():
    source = SchemaSnapshot(
        database_name="source",
        tables=(
            TableDefinition(
                schema="df_demo",
                name="v_orders",
                object_type=ObjectType.VIEW,
                columns=(
                    ColumnDefinition(
                        name="order_no",
                        ordinal_position=1,
                        data_type="character varying",
                        character_maximum_length=32,
                        numeric_precision=None,
                        numeric_scale=None,
                        is_nullable=True,
                        column_default=None,
                        is_sequence_related=False,
                    ),
                ),
            ),
        ),
    )
    target = SchemaSnapshot(
        database_name="target",
        tables=(
            TableDefinition(
                schema="df_demo",
                name="v_orders",
                object_type=ObjectType.VIEW,
                columns=(),
            ),
        ),
    )

    diff = SchemaComparator().compare(source, target)

    assert len(diff.column_diffs) == 1
    cd = diff.column_diffs[0]
    assert cd.status == DiffStatus.ONLY_SOURCE
    assert cd.reason == "missing_target_column"
    assert cd.category == DiffCategory.VIEW_REBUILD_SYNCABLE
    assert cd.object_type == ObjectType.VIEW
