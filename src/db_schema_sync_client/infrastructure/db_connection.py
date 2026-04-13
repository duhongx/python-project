"""Database connection helpers for PostgreSQL-compatible backends."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from db_schema_sync_client.domain.models import ConnectionProfile

try:
    import psycopg2
except ImportError:  # pragma: no cover - depends on runtime environment
    psycopg2 = None


@dataclass(frozen=True)
class ConnectionTestResult:
    success: bool
    message: str


class DatabaseConnectionFactory:
    def connect(self, profile: ConnectionProfile, password: str) -> Any:
        if psycopg2 is None:
            raise RuntimeError("psycopg2 is required for database connections")

        return psycopg2.connect(
            host=profile.host,
            port=profile.port,
            database=profile.database,
            user=profile.username,
            password=password,
            client_encoding="utf8",
        )

    def test_connection(self, profile: ConnectionProfile, password: str) -> ConnectionTestResult:
        try:
            conn = self.connect(profile, password)
        except Exception as exc:  # pragma: no cover - exercised in manual/runtime flows
            return ConnectionTestResult(success=False, message=str(exc))

        conn.close()
        return ConnectionTestResult(success=True, message="Connection succeeded")
