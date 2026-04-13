"""Application path helpers."""

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def development_data_dir() -> Path:
    """Return the project-local data directory for development runs."""
    return PROJECT_ROOT / "data"


def development_db_path() -> Path:
    """Return the project-local SQLite path for development runs."""
    return development_data_dir() / "app.db"


def application_data_dir(app_name: str = "db-schema-sync") -> Path:
    """Return the packaged-app data directory for the current platform."""
    home = Path.home()
    if os.name == "nt":
        base_dir = Path(os.environ.get("APPDATA", home / "AppData" / "Roaming"))
    elif os.name == "posix" and Path("/Applications").exists():
        base_dir = home / "Library" / "Application Support"
    else:
        base_dir = Path(os.environ.get("XDG_DATA_HOME", home / ".local" / "share"))
    return base_dir / app_name


def packaged_db_path() -> Path:
    """Return the packaged-app SQLite path."""
    return application_data_dir() / "app.db"
