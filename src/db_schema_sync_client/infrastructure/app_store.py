"""SQLite-backed local application store."""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from typing import Iterable, Optional

from db_schema_sync_client.domain.models import ConnectionProfile, ConnectionRole, DatabaseType

from .credentials import CredentialStore, KeyringCredentialStore, hash_password, verify_password


DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "cloudhis@2123"


class AppStore:
    def __init__(
        self,
        db_path: Path,
        credential_store: Optional[CredentialStore] = None,
    ) -> None:
        self.db_path = Path(db_path)
        self.credential_store = credential_store or KeyringCredentialStore()

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS connection_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    role TEXT NOT NULL,
                    db_type TEXT NOT NULL,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    database_name TEXT NOT NULL,
                    username TEXT NOT NULL,
                    credential_key TEXT NOT NULL,
                    schema_prefix TEXT NOT NULL,
                    owner_prefix TEXT NOT NULL,
                    is_default INTEGER NOT NULL DEFAULT 0,
                    last_test_status TEXT,
                    last_test_message TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS compare_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_profile_id INTEGER,
                    target_profile_id INTEGER,
                    status TEXT NOT NULL,
                    result_json TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS compare_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    compare_task_id INTEGER NOT NULL,
                    result_type TEXT NOT NULL,
                    object_name TEXT NOT NULL,
                    details_json TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS sync_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_profile_id INTEGER,
                    run_type TEXT NOT NULL DEFAULT 'execute',
                    status TEXT NOT NULL,
                    selected_fields_json TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS sync_statements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sync_run_id INTEGER NOT NULL,
                    statement_text TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            self._seed_default_admin(conn)
            self._migrate_schema(conn)

    def verify_user(self, username: str, password: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT password_hash FROM users WHERE username = ?",
                (username,),
            ).fetchone()

        if row is None:
            return False
        return verify_password(password, row["password_hash"])

    def save_profile(self, profile: ConnectionProfile, password: str) -> ConnectionProfile:
        self._validate_profile(profile)

        credential_key = profile.credential_key or self._build_credential_key(profile)
        self.credential_store.set(credential_key, password)

        with self._connect() as conn:
            if profile.is_default:
                self._clear_default_for_role(conn, profile.role)

            if profile.id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO connection_profiles (
                        name,
                        role,
                        db_type,
                        host,
                        port,
                        database_name,
                        username,
                        credential_key,
                        schema_prefix,
                        owner_prefix,
                        is_default,
                        last_test_status,
                        last_test_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        profile.name,
                        profile.role.value,
                        profile.db_type.value,
                        profile.host,
                        profile.port,
                        profile.database,
                        profile.username,
                        credential_key,
                        profile.schema_prefix,
                        profile.owner_prefix,
                        int(profile.is_default),
                        profile.last_test_status,
                        profile.last_test_message,
                    ),
                )
                profile_id = cursor.lastrowid
            else:
                conn.execute(
                    """
                    UPDATE connection_profiles
                    SET name = ?,
                        role = ?,
                        db_type = ?,
                        host = ?,
                        port = ?,
                        database_name = ?,
                        username = ?,
                        credential_key = ?,
                        schema_prefix = ?,
                        owner_prefix = ?,
                        is_default = ?,
                        last_test_status = ?,
                        last_test_message = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        profile.name,
                        profile.role.value,
                        profile.db_type.value,
                        profile.host,
                        profile.port,
                        profile.database,
                        profile.username,
                        credential_key,
                        profile.schema_prefix,
                        profile.owner_prefix,
                        int(profile.is_default),
                        profile.last_test_status,
                        profile.last_test_message,
                        profile.id,
                    ),
                )
                profile_id = profile.id

            row = conn.execute(
                "SELECT * FROM connection_profiles WHERE id = ?",
                (profile_id,),
            ).fetchone()

        return self._row_to_profile(row)

    def list_profiles(
        self,
        role: Optional[ConnectionRole] = None,
    ) -> list[ConnectionProfile]:
        query = "SELECT * FROM connection_profiles"
        params: Iterable[object] = ()
        if role is not None:
            query += " WHERE role = ?"
            params = (role.value,)
        query += " ORDER BY id"

        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()

        return [self._row_to_profile(row) for row in rows]

    def get_profile(self, profile_id: int) -> Optional[ConnectionProfile]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM connection_profiles WHERE id = ?",
                (profile_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_profile(row)

    def get_default_profile(self, role: ConnectionRole) -> Optional[ConnectionProfile]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM connection_profiles
                WHERE role = ? AND is_default = 1
                ORDER BY id
                LIMIT 1
                """,
                (role.value,),
            ).fetchone()

        if row is None:
            return None
        return self._row_to_profile(row)

    def set_default_profile(self, role: ConnectionRole, profile_id: int) -> None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT role FROM connection_profiles WHERE id = ?",
                (profile_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Profile {profile_id} does not exist")
            if row["role"] != role.value:
                raise ValueError("Profile role does not match requested default scope")

            self._clear_default_for_role(conn, role)
            conn.execute(
                "UPDATE connection_profiles SET is_default = 1 WHERE id = ?",
                (profile_id,),
            )

    def get_profile_password(self, profile: ConnectionProfile) -> str:
        if not profile.credential_key:
            raise ValueError("Profile has no credential key")
        return self.credential_store.get(profile.credential_key)

    def delete_profile(self, profile_id: int) -> None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT credential_key FROM connection_profiles WHERE id = ?",
                (profile_id,),
            ).fetchone()
            if row is None:
                return
            conn.execute(
                "DELETE FROM connection_profiles WHERE id = ?",
                (profile_id,),
            )
        self.credential_store.delete(row["credential_key"])

    def create_sync_run(
        self,
        target_profile_id: Optional[int],
        run_type: str,
        status: str,
        selected_fields_json: Optional[str] = None,
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO sync_runs (target_profile_id, run_type, status, selected_fields_json)
                VALUES (?, ?, ?, ?)
                """,
                (target_profile_id, run_type, status, selected_fields_json),
            )
            return cursor.lastrowid

    def update_sync_run_status(self, run_id: int, status: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE sync_runs SET status = ? WHERE id = ?",
                (status, run_id),
            )

    def add_sync_statement(
        self,
        sync_run_id: int,
        statement_text: str,
        status: str,
        error_message: Optional[str] = None,
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO sync_statements (sync_run_id, statement_text, status, error_message)
                VALUES (?, ?, ?, ?)
                """,
                (sync_run_id, statement_text, status, error_message),
            )
            return cursor.lastrowid

    def list_sync_statements(self, sync_run_id: int) -> list[dict[str, Optional[str]]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, sync_run_id, statement_text, status, error_message, created_at
                FROM sync_statements
                WHERE sync_run_id = ?
                ORDER BY id
                """,
                (sync_run_id,),
            ).fetchall()

        return [dict(row) for row in rows]

    def list_sync_runs(
        self,
        target_profile_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> list[dict]:
        query = "SELECT * FROM sync_runs WHERE 1=1"
        params: list = []
        if target_profile_id is not None:
            query += " AND target_profile_id = ?"
            params.append(target_profile_id)
        if status is not None:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def create_compare_task(
        self,
        source_profile_id: Optional[int],
        target_profile_id: Optional[int],
        status: str,
        result_json: Optional[str] = None,
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO compare_tasks (source_profile_id, target_profile_id, status, result_json)
                VALUES (?, ?, ?, ?)
                """,
                (source_profile_id, target_profile_id, status, result_json),
            )
            return cursor.lastrowid

    def update_compare_task_status(self, task_id: int, status: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE compare_tasks SET status = ? WHERE id = ?",
                (status, task_id),
            )

    def list_compare_tasks(
        self,
        source_profile_id: Optional[int] = None,
        target_profile_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> list[dict]:
        query = "SELECT * FROM compare_tasks WHERE 1=1"
        params: list = []
        if source_profile_id is not None:
            query += " AND source_profile_id = ?"
            params.append(source_profile_id)
        if target_profile_id is not None:
            query += " AND target_profile_id = ?"
            params.append(target_profile_id)
        if status is not None:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        """Add columns that may be missing in databases created by earlier versions."""
        # sync_runs.selected_fields_json
        cols = {row[1] for row in conn.execute("PRAGMA table_info(sync_runs)").fetchall()}
        if "selected_fields_json" not in cols:
            conn.execute("ALTER TABLE sync_runs ADD COLUMN selected_fields_json TEXT")
        # compare_tasks.result_json
        cols = {row[1] for row in conn.execute("PRAGMA table_info(compare_tasks)").fetchall()}
        if "result_json" not in cols:
            conn.execute("ALTER TABLE compare_tasks ADD COLUMN result_json TEXT")

    def _seed_default_admin(self, conn: sqlite3.Connection) -> None:
        row = conn.execute(
            "SELECT id FROM users WHERE username = ?",
            (DEFAULT_ADMIN_USERNAME,),
        ).fetchone()
        if row is not None:
            return

        conn.execute(
            "INSERT INTO users (username, password_hash) VALUES (?, ?)",
            (DEFAULT_ADMIN_USERNAME, hash_password(DEFAULT_ADMIN_PASSWORD)),
        )

    def _validate_profile(self, profile: ConnectionProfile) -> None:
        if profile.role == ConnectionRole.SOURCE and profile.db_type != DatabaseType.POSTGRESQL:
            raise ValueError("Source profiles must use PostgreSQL")

        if profile.role == ConnectionRole.TARGET and profile.db_type not in {
            DatabaseType.POSTGRESQL,
            DatabaseType.KINGBASE,
        }:
            raise ValueError("Target profiles must use PostgreSQL or KingBase")

        if not 1 <= profile.port <= 65535:
            raise ValueError("Port must be between 1 and 65535")

    def _build_credential_key(self, profile: ConnectionProfile) -> str:
        return (
            f"{profile.role.value}:"
            f"{profile.db_type.value}:"
            f"{profile.name}:"
            f"{uuid.uuid4().hex}"
        )

    def _row_to_profile(self, row: sqlite3.Row) -> ConnectionProfile:
        return ConnectionProfile(
            id=row["id"],
            name=row["name"],
            role=ConnectionRole(row["role"]),
            db_type=DatabaseType(row["db_type"]),
            host=row["host"],
            port=row["port"],
            database=row["database_name"],
            username=row["username"],
            credential_key=row["credential_key"],
            schema_prefix=row["schema_prefix"],
            owner_prefix=row["owner_prefix"],
            is_default=bool(row["is_default"]),
            last_test_status=row["last_test_status"],
            last_test_message=row["last_test_message"],
        )

    def _clear_default_for_role(self, conn: sqlite3.Connection, role: ConnectionRole) -> None:
        conn.execute(
            "UPDATE connection_profiles SET is_default = 0 WHERE role = ?",
            (role.value,),
        )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
