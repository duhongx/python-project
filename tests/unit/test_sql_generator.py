from db_schema_sync_client.domain.diff import DiffCategory, DiffStatus, ObjectDiff
from db_schema_sync_client.domain.models import (
    ColumnDefinition,
    DatabaseType,
    IndexDefinition,
    ObjectType,
    PrimaryKeyDefinition,
    TableDefinition,
)
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


def test_generate_object_creates_includes_pk_indexes_and_comments():
    generator = SqlGenerator()
    table = TableDefinition(
        schema="df_test",
        name="users",
        object_type=ObjectType.TABLE,
        columns=(
            ColumnDefinition(
                name="id",
                ordinal_position=1,
                data_type="bigint",
                character_maximum_length=None,
                numeric_precision=None,
                numeric_scale=None,
                is_nullable=False,
                column_default=None,
                is_sequence_related=False,
                comment="主键",
            ),
            ColumnDefinition(
                name="name",
                ordinal_position=2,
                data_type="character varying",
                character_maximum_length=100,
                numeric_precision=None,
                numeric_scale=None,
                is_nullable=True,
                column_default=None,
                is_sequence_related=False,
                comment="姓名",
            ),
        ),
        comment="用户表",
        primary_key=PrimaryKeyDefinition(name="users_pkey", column_names=("id",)),
        indexes=(
            IndexDefinition(
                name="idx_users_name",
                definition='CREATE INDEX "idx_users_name" ON "df_test"."users" USING btree ("name")',
                is_unique=False,
            ),
        ),
    )
    diff = ObjectDiff(
        schema="df_test",
        object_name="users",
        status=DiffStatus.ONLY_SOURCE,
        category=DiffCategory.TABLE_SYNCABLE,
        source_object=table,
        target_object=None,
        reason="missing_table",
    )

    plan = generator.generate_object_creates(
        object_diffs=[diff],
        target_type=DatabaseType.POSTGRESQL,
    )

    assert plan.statements[0].startswith('CREATE TABLE IF NOT EXISTS "df_test"."users"')
    assert 'ADD CONSTRAINT "users_pkey" PRIMARY KEY ("id")' in plan.statements[1]
    assert plan.statements[2] == 'CREATE INDEX "idx_users_name" ON "df_test"."users" USING btree ("name");'
    assert plan.statements[3] == 'COMMENT ON TABLE "df_test"."users" IS \'用户表\';'
    assert plan.statements[4] == 'COMMENT ON COLUMN "df_test"."users"."id" IS \'主键\';'
    assert plan.statements[5] == 'COMMENT ON COLUMN "df_test"."users"."name" IS \'姓名\';'


def test_generate_object_creates_view_includes_comments():
    generator = SqlGenerator()
    view = TableDefinition(
        schema="df_test",
        name="v_users",
        object_type=ObjectType.VIEW,
        columns=(
            ColumnDefinition(
                name="name",
                ordinal_position=1,
                data_type="character varying",
                character_maximum_length=100,
                numeric_precision=None,
                numeric_scale=None,
                is_nullable=True,
                column_default=None,
                is_sequence_related=False,
                comment="姓名列",
            ),
        ),
        view_definition="SELECT name FROM df_test.users",
        comment="用户视图",
    )
    diff = ObjectDiff(
        schema="df_test",
        object_name="v_users",
        status=DiffStatus.ONLY_SOURCE,
        category=DiffCategory.TABLE_SYNCABLE,
        source_object=view,
        target_object=None,
        reason="missing_view",
    )

    plan = generator.generate_object_creates(
        object_diffs=[diff],
        target_type=DatabaseType.POSTGRESQL,
    )

    assert plan.statements[0].startswith('CREATE OR REPLACE VIEW "df_test"."v_users" AS')
    assert plan.statements[1] == 'COMMENT ON VIEW "df_test"."v_users" IS \'用户视图\';'
    assert plan.statements[2] == 'COMMENT ON COLUMN "df_test"."v_users"."name" IS \'姓名列\';'


def test_generate_object_creates_includes_existing_schema_owner_fix():
    generator = SqlGenerator()
    table = TableDefinition(
        schema="df_wuzi",
        name="gy_demo",
        object_type=ObjectType.TABLE,
        columns=(),
    )
    diff = ObjectDiff(
        schema="df_wuzi",
        object_name="gy_demo",
        status=DiffStatus.ONLY_SOURCE,
        category=DiffCategory.TABLE_SYNCABLE,
        source_object=table,
        target_object=None,
        reason="missing_table",
    )

    plan = generator.generate_object_creates(
        object_diffs=[diff],
        target_type=DatabaseType.POSTGRESQL,
        existing_schema_owner_fixes=["df_wuzi"],
        role_hashes={"df_wuzi": "md5xxxx"},
    )

    assert "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'df_wuzi')" in plan.statements[0]
    assert plan.statements[1] == 'ALTER SCHEMA "df_wuzi" OWNER TO "df_wuzi";'
    assert plan.statements[2].startswith('CREATE TABLE IF NOT EXISTS "df_wuzi"."gy_demo"')


def test_generate_view_rebuilds_contains_backup_and_replace():
    generator = SqlGenerator()
    view = TableDefinition(
        schema="df_test",
        name="v_orders",
        object_type=ObjectType.VIEW,
        columns=(),
        view_definition="SELECT 1 AS id",
    )

    plan = generator.generate_view_rebuilds(
        views=[view],
        target_type=DatabaseType.POSTGRESQL,
    )

    assert "ALTER VIEW" in plan.statements[0]
    assert 'v_orders_' in plan.statements[0]
    assert plan.statements[1].startswith('CREATE OR REPLACE VIEW "df_test"."v_orders" AS')
