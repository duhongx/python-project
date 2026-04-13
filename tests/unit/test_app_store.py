import sqlite3

import pytest

from db_schema_sync_client.domain.models import ConnectionProfile, ConnectionRole, DatabaseType
from db_schema_sync_client.infrastructure.app_store import AppStore
from db_schema_sync_client.infrastructure.credentials import InMemoryCredentialStore


def test_initialize_creates_users_and_default_admin(tmp_path):
    db_path = tmp_path / "app.db"
    store = AppStore(db_path, credential_store=InMemoryCredentialStore())

    store.initialize()

    conn = sqlite3.connect(db_path)
    try:
        username, password_hash = conn.execute(
            "SELECT username, password_hash FROM users WHERE username = ?",
            ("admin",),
        ).fetchone()
    finally:
        conn.close()

    assert username == "admin"
    assert password_hash != "cloudhis@2123"
    assert store.verify_user("admin", "cloudhis@2123") is True
    assert store.verify_user("admin", "wrong-password") is False


def test_initialize_is_idempotent_for_default_admin(tmp_path):
    db_path = tmp_path / "app.db"
    store = AppStore(db_path, credential_store=InMemoryCredentialStore())

    store.initialize()
    store.initialize()

    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM users WHERE username = ?",
            ("admin",),
        ).fetchone()[0]
    finally:
        conn.close()

    assert count == 1


def test_save_profile_persists_metadata_without_plaintext_password(tmp_path):
    db_path = tmp_path / "app.db"
    credential_store = InMemoryCredentialStore()
    store = AppStore(db_path, credential_store=credential_store)
    store.initialize()

    saved = store.save_profile(
        ConnectionProfile(
            name="source-main",
            role=ConnectionRole.SOURCE,
            db_type=DatabaseType.POSTGRESQL,
            host="127.0.0.1",
            port=5432,
            database="demo",
            username="demo_user",
            is_default=True,
        ),
        password="secret-1",
    )

    rows = store.list_profiles(ConnectionRole.SOURCE)
    assert len(rows) == 1
    assert rows[0].name == "source-main"
    assert rows[0].credential_key == saved.credential_key
    assert not hasattr(rows[0], "password")
    assert credential_store.get(saved.credential_key) == "secret-1"

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT credential_key, host, port, database_name, username
            FROM connection_profiles
            WHERE id = ?
            """,
            (saved.id,),
        ).fetchone()
    finally:
        conn.close()

    assert row[0] == saved.credential_key
    assert "secret-1" not in row


def test_save_profile_rejects_unsupported_source_database_type(tmp_path):
    store = AppStore(tmp_path / "app.db", credential_store=InMemoryCredentialStore())
    store.initialize()

    with pytest.raises(ValueError, match="Source profiles must use PostgreSQL"):
        store.save_profile(
            ConnectionProfile(
                name="source-kb",
                role=ConnectionRole.SOURCE,
                db_type=DatabaseType.KINGBASE,
                host="127.0.0.1",
                port=5432,
                database="demo",
                username="demo_user",
            ),
            password="secret-1",
        )


def test_save_profile_accepts_postgresql_and_kingbase_targets(tmp_path):
    store = AppStore(tmp_path / "app.db", credential_store=InMemoryCredentialStore())
    store.initialize()

    postgres = store.save_profile(
        ConnectionProfile(
            name="target-pg",
            role=ConnectionRole.TARGET,
            db_type=DatabaseType.POSTGRESQL,
            host="127.0.0.1",
            port=5432,
            database="demo",
            username="demo_user",
        ),
        password="secret-1",
    )
    kingbase = store.save_profile(
        ConnectionProfile(
            name="target-kb",
            role=ConnectionRole.TARGET,
            db_type=DatabaseType.KINGBASE,
            host="127.0.0.1",
            port=5432,
            database="demo2",
            username="demo_user",
            is_default=True,
        ),
        password="secret-2",
    )

    rows = store.list_profiles(ConnectionRole.TARGET)

    assert [row.name for row in rows] == ["target-pg", "target-kb"]
    assert postgres.db_type == DatabaseType.POSTGRESQL
    assert kingbase.db_type == DatabaseType.KINGBASE


def test_set_default_profile_is_scoped_by_role(tmp_path):
    store = AppStore(tmp_path / "app.db", credential_store=InMemoryCredentialStore())
    store.initialize()

    source_a = store.save_profile(
        ConnectionProfile(
            name="source-a",
            role=ConnectionRole.SOURCE,
            db_type=DatabaseType.POSTGRESQL,
            host="127.0.0.1",
            port=5432,
            database="db_a",
            username="user_a",
            is_default=True,
        ),
        password="secret-1",
    )
    source_b = store.save_profile(
        ConnectionProfile(
            name="source-b",
            role=ConnectionRole.SOURCE,
            db_type=DatabaseType.POSTGRESQL,
            host="127.0.0.1",
            port=5432,
            database="db_b",
            username="user_b",
        ),
        password="secret-2",
    )
    target = store.save_profile(
        ConnectionProfile(
            name="target-a",
            role=ConnectionRole.TARGET,
            db_type=DatabaseType.KINGBASE,
            host="127.0.0.1",
            port=5432,
            database="db_t",
            username="user_t",
            is_default=True,
        ),
        password="secret-3",
    )

    store.set_default_profile(ConnectionRole.SOURCE, source_b.id)

    default_source = store.get_default_profile(ConnectionRole.SOURCE)
    default_target = store.get_default_profile(ConnectionRole.TARGET)
    source_rows = store.list_profiles(ConnectionRole.SOURCE)

    assert default_source.id == source_b.id
    assert default_target.id == target.id
    assert [(row.name, row.is_default) for row in source_rows] == [
        ("source-a", False),
        ("source-b", True),
    ]
    assert source_a.id != source_b.id


def test_get_profile_password_reads_from_credential_store(tmp_path):
    credential_store = InMemoryCredentialStore()
    store = AppStore(tmp_path / "app.db", credential_store=credential_store)
    store.initialize()

    saved = store.save_profile(
        ConnectionProfile(
            name="target-a",
            role=ConnectionRole.TARGET,
            db_type=DatabaseType.KINGBASE,
            host="127.0.0.1",
            port=5432,
            database="db_t",
            username="user_t",
        ),
        password="secret-3",
    )

    assert store.get_profile_password(saved) == "secret-3"


def test_delete_profile_removes_record_and_credential(tmp_path):
    credential_store = InMemoryCredentialStore()
    store = AppStore(tmp_path / "app.db", credential_store=credential_store)
    store.initialize()

    saved = store.save_profile(
        ConnectionProfile(
            name="target-a",
            role=ConnectionRole.TARGET,
            db_type=DatabaseType.KINGBASE,
            host="127.0.0.1",
            port=5432,
            database="db_t",
            username="user_t",
        ),
        password="secret-3",
    )

    store.delete_profile(saved.id)

    assert store.list_profiles(ConnectionRole.TARGET) == []
    assert saved.credential_key not in credential_store.values
