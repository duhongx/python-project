"""Credential and password helpers."""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, Protocol

try:
    import keyring
except ImportError:  # pragma: no cover
    keyring = None


DEFAULT_HASH_ITERATIONS = 200_000
PASSWORD_HASH_SCHEME = "pbkdf2_sha256"
KEYRING_SERVICE_NAME = "db-schema-sync-client"

# Fixed app-level secret for SQLitaCredentialStore XOR encryption.
# Not a strong cryptographic guarantee, but prevents plain-text exposure in the file.
_APP_SECRET = b"db-schema-sync-client-v1"


def hash_password(password: str, iterations: int = DEFAULT_HASH_ITERATIONS) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return (
        f"{PASSWORD_HASH_SCHEME}$"
        f"{iterations}$"
        f"{salt.hex()}$"
        f"{digest.hex()}"
    )


def verify_password(password: str, password_hash: str) -> bool:
    try:
        scheme, raw_iterations, salt_hex, digest_hex = password_hash.split("$", 3)
    except ValueError:
        return False

    if scheme != PASSWORD_HASH_SCHEME:
        return False

    iterations = int(raw_iterations)
    expected = bytes.fromhex(digest_hex)
    actual = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt_hex),
        iterations,
    )
    return hmac.compare_digest(actual, expected)


class CredentialStore(Protocol):
    def set(self, key: str, value: str) -> None:
        ...

    def get(self, key: str) -> str:
        ...

    def delete(self, key: str) -> None:
        ...


class SQLiteCredentialStore:
    """Stores connection passwords in the app SQLite DB, XOR-encrypted with the app secret.

    Passwords are not stored in plaintext; the encryption is obfuscation-grade.
    For an offline desktop tool this is adequate to prevent casual exposure.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._init_table()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def set(self, key: str, value: str) -> None:
        encrypted = self._encrypt(key, value)
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO credentials (key, value_encrypted) VALUES (?, ?)",
                (key, encrypted),
            )

    def get(self, key: str) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value_encrypted FROM credentials WHERE key = ?",
                (key,),
            ).fetchone()
        if row is None:
            raise KeyError(key)
        return self._decrypt(key, row[0])

    def delete(self, key: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM credentials WHERE key = ?", (key,))

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _init_table(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS credentials (
                    key TEXT PRIMARY KEY,
                    value_encrypted TEXT NOT NULL
                )"""
            )

    def _keystream(self, key: str) -> bytes:
        """Derive a 32-byte keystream from the credential key + app secret."""
        return hmac.new(_APP_SECRET, key.encode("utf-8"), "sha256").digest()

    def _encrypt(self, key: str, plaintext: str) -> str:
        ks = self._keystream(key)
        data = plaintext.encode("utf-8")
        encrypted = bytes(b ^ ks[i % len(ks)] for i, b in enumerate(data))
        return base64.b64encode(encrypted).decode("ascii")

    def _decrypt(self, key: str, ciphertext: str) -> str:
        ks = self._keystream(key)
        encrypted = base64.b64decode(ciphertext.encode("ascii"))
        data = bytes(b ^ ks[i % len(ks)] for i, b in enumerate(encrypted))
        return data.decode("utf-8")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn


class KeyringCredentialStore:
    """Runtime credential store backed by the OS keyring."""

    def __init__(self, service_name: str = KEYRING_SERVICE_NAME) -> None:
        if keyring is None:
            raise RuntimeError("keyring is required for KeyringCredentialStore")
        self._service_name = service_name

    def set(self, key: str, value: str) -> None:
        keyring.set_password(self._service_name, key, value)

    def get(self, key: str) -> str:
        value: Optional[str] = keyring.get_password(self._service_name, key)
        if value is None:
            raise KeyError(key)
        return value

    def delete(self, key: str) -> None:
        try:
            keyring.delete_password(self._service_name, key)
        except keyring.errors.PasswordDeleteError:
            return


@dataclass
class InMemoryCredentialStore:
    """Test credential store that never persists beyond process memory."""

    values: Dict[str, str] = field(default_factory=dict)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value

    def get(self, key: str) -> str:
        return self.values[key]

    def delete(self, key: str) -> None:
        self.values.pop(key, None)
