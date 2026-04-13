from db_schema_sync_client.domain.models import ObjectType
from db_schema_sync_client.infrastructure.db_metadata import parse_column_row, parse_table_row


def test_parse_table_row_maps_base_table():
    row = {
        "table_schema": "df_demo",
        "table_name": "users",
        "table_type": "BASE TABLE",
    }

    parsed = parse_table_row(row)

    assert parsed.schema == "df_demo"
    assert parsed.name == "users"
    assert parsed.object_type == ObjectType.TABLE


def test_parse_table_row_maps_view():
    row = {
        "table_schema": "df_demo",
        "table_name": "v_users",
        "table_type": "VIEW",
    }

    parsed = parse_table_row(row)

    assert parsed.object_type == ObjectType.VIEW


def test_parse_column_row_preserves_numeric_precision_scale_and_default():
    row = {
        "column_name": "amount",
        "ordinal_position": 3,
        "data_type": "numeric",
        "character_maximum_length": None,
        "numeric_precision": 12,
        "numeric_scale": 2,
        "is_nullable": "NO",
        "column_default": "0",
    }

    parsed = parse_column_row(row)

    assert parsed.numeric_precision == 12
    assert parsed.numeric_scale == 2
    assert parsed.column_default == "0"
    assert parsed.is_nullable is False


def test_parse_column_row_marks_sequence_defaults():
    row = {
        "column_name": "id",
        "ordinal_position": 1,
        "data_type": "integer",
        "character_maximum_length": None,
        "numeric_precision": 32,
        "numeric_scale": 0,
        "is_nullable": "NO",
        "column_default": "nextval('df_demo.users_id_seq'::regclass)",
    }

    parsed = parse_column_row(row)

    assert parsed.is_sequence_related is True
