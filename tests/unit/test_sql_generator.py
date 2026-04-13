from db_schema_sync_client.domain.diff import DiffCategory
from db_schema_sync_client.domain.models import ColumnDefinition, DatabaseType
from db_schema_sync_client.services.sql_generator import SqlGenerator


def test_diff_category_auto_syncable_is_stable():
    assert DiffCategory.AUTO_SYNCABLE.value == "auto_syncable"


def test_generate_postgresql_add_column_sql():
    generator = SqlGenerator()
    column = ColumnDefinition(
        name="name",
        ordinal_position=1,
        data_type="character varying",
        character_maximum_length=100,
        numeric_precision=None,
        numeric_scale=None,
        is_nullable=True,
        column_default=None,
        is_sequence_related=False,
    )

    plan = generator.generate_missing_columns(
        items=[("df_test", "users", column)],
        target_type=DatabaseType.POSTGRESQL,
    )

    assert plan.statements == [
        'ALTER TABLE "df_test"."users" ADD COLUMN "name" character varying(100);'
    ]


def test_generate_kingbase_add_column_sql():
    generator = SqlGenerator()
    column = ColumnDefinition(
        name="name",
        ordinal_position=1,
        data_type="character varying",
        character_maximum_length=100,
        numeric_precision=None,
        numeric_scale=None,
        is_nullable=True,
        column_default=None,
        is_sequence_related=False,
    )

    plan = generator.generate_missing_columns(
        items=[("df_test", "users", column)],
        target_type=DatabaseType.KINGBASE,
    )

    assert plan.target_type == DatabaseType.KINGBASE
    assert plan.statements == [
        'ALTER TABLE "df_test"."users" ADD COLUMN "name" character varying(100);'
    ]


def test_generator_counts_manual_and_hint_items():
    generator = SqlGenerator()
    column = ColumnDefinition(
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

    plan = generator.generate_missing_columns(
        items=[("df_test", "users", column)],
        target_type=DatabaseType.POSTGRESQL,
        manual_count=2,
        hint_count=3,
    )

    assert plan.auto_syncable_count == 1
    assert plan.manual_required_count == 2
    assert plan.hint_only_count == 3
