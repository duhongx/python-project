from db_schema_sync_client.domain.models import ColumnDefinition, DatabaseType
from db_schema_sync_client.services.dialects import get_dialect


def test_postgresql_quotes_identifiers_with_double_quotes():
    dialect = get_dialect(DatabaseType.POSTGRESQL)

    assert dialect.quote_identifier("users") == '"users"'


def test_kingbase_quotes_identifiers_with_double_quotes():
    dialect = get_dialect(DatabaseType.KINGBASE)

    assert dialect.quote_identifier("users") == '"users"'


def test_quote_identifier_escapes_embedded_quotes():
    dialect = get_dialect(DatabaseType.POSTGRESQL)

    assert dialect.quote_identifier('bad"name') == '"bad""name"'


def test_quote_identifier_handles_chinese_names():
    dialect = get_dialect(DatabaseType.KINGBASE)

    assert dialect.quote_identifier("用户表") == '"用户表"'


def test_format_character_varying_length():
    dialect = get_dialect(DatabaseType.POSTGRESQL)
    column = ColumnDefinition(
        name="display_name",
        ordinal_position=1,
        data_type="character varying",
        character_maximum_length=100,
        numeric_precision=None,
        numeric_scale=None,
        is_nullable=True,
        column_default=None,
        is_sequence_related=False,
    )

    assert dialect.format_column_type(column) == "character varying(100)"


def test_format_numeric_precision_and_scale():
    dialect = get_dialect(DatabaseType.KINGBASE)
    column = ColumnDefinition(
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

    assert dialect.format_column_type(column) == "numeric(12, 2)"


def test_build_add_column_sql_downgrades_not_null_without_default():
    dialect = get_dialect(DatabaseType.POSTGRESQL)
    column = ColumnDefinition(
        name="code",
        ordinal_position=1,
        data_type="character varying",
        character_maximum_length=32,
        numeric_precision=None,
        numeric_scale=None,
        is_nullable=False,
        column_default=None,
        is_sequence_related=False,
    )

    sql, warnings = dialect.build_add_column_sql("df_demo", "users", column)

    assert sql == 'ALTER TABLE "df_demo"."users" ADD COLUMN "code" character varying(32);'
    assert warnings == [
        'Column "df_demo"."users"."code" requires manual NOT NULL enforcement after backfill.'
    ]


def test_build_add_column_sql_warns_for_sequence_related_default():
    dialect = get_dialect(DatabaseType.KINGBASE)
    column = ColumnDefinition(
        name="id",
        ordinal_position=1,
        data_type="integer",
        character_maximum_length=None,
        numeric_precision=32,
        numeric_scale=0,
        is_nullable=False,
        column_default="nextval('df_demo.users_id_seq'::regclass)",
        is_sequence_related=True,
    )

    _, warnings = dialect.build_add_column_sql("df_demo", "users", column)

    assert warnings == [
        'Column "df_demo"."users"."id" uses sequence/identity semantics and should be reviewed manually.'
    ]
