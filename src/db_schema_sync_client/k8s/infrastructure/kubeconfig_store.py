"""Kubeconfig file storage management.

Uploaded kubeconfig files are copied into the application data directory so they
persist independently of the original file location chosen by the user.
"""

from __future__ import annotations

import shutil
from pathlib import Path


class KubeconfigStore:
    """Manages local storage of uploaded kubeconfig files."""

    def __init__(self, store_dir: Path) -> None:
        self._dir = store_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    def save(self, cluster_id: int, source_path: Path) -> Path:
        """Copy *source_path* into the store and return the destination path."""
        dest = self._dir / f"cluster_{cluster_id}.yaml"
        shutil.copy2(source_path, dest)
        return dest

    def path_for(self, cluster_id: int) -> Path:
        """Return the stored kubeconfig path for a cluster (may not exist yet)."""
        return self._dir / f"cluster_{cluster_id}.yaml"

    def delete(self, cluster_id: int) -> None:
        """Remove the stored kubeconfig file if it exists."""
        dest = self._dir / f"cluster_{cluster_id}.yaml"
        if dest.exists():
            dest.unlink()

    def exists(self, cluster_id: int) -> bool:
        return (self._dir / f"cluster_{cluster_id}.yaml").exists()
