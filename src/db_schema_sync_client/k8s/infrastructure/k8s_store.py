"""SQLite persistence for K8s cluster configs and deployment snapshots.

This store owns three tables:
  - k8s_cluster_configs   : registered clusters (kubeconfig path + namespaces)
  - k8s_deployment_snapshots : named snapshots per cluster+namespace
  - k8s_image_records     : per-container image entries within a snapshot
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import List, Optional

from db_schema_sync_client.k8s.domain.models import (
    ContainerImage,
    DeploymentImageRecord,
    DeploymentSnapshot,
    KubeClusterConfig,
)


class K8sStore:
    """CRUD for K8s-related SQLite tables."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def initialize(self) -> None:
        """Create tables if they don't exist (called once at app startup)."""
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS k8s_cluster_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    kubeconfig_path TEXT NOT NULL,
                    context_name TEXT NOT NULL,
                    namespaces_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS k8s_deployment_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cluster_config_id INTEGER NOT NULL REFERENCES k8s_cluster_configs(id) ON DELETE CASCADE,
                    namespace TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS k8s_image_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL REFERENCES k8s_deployment_snapshots(id) ON DELETE CASCADE,
                    deployment_name TEXT NOT NULL,
                    container_name TEXT NOT NULL,
                    image TEXT NOT NULL
                );
                """
            )

    # ------------------------------------------------------------------
    # KubeClusterConfig
    # ------------------------------------------------------------------

    def save_cluster_config(self, config: KubeClusterConfig) -> KubeClusterConfig:
        namespaces_json = json.dumps(config.namespaces, ensure_ascii=False)
        with self._connect() as conn:
            if config.id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO k8s_cluster_configs
                        (name, kubeconfig_path, context_name, namespaces_json)
                    VALUES (?, ?, ?, ?)
                    """,
                    (config.name, config.kubeconfig_path, config.context_name, namespaces_json),
                )
                new_id = cursor.lastrowid
            else:
                conn.execute(
                    """
                    UPDATE k8s_cluster_configs
                    SET name = ?, kubeconfig_path = ?, context_name = ?,
                        namespaces_json = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        config.name,
                        config.kubeconfig_path,
                        config.context_name,
                        namespaces_json,
                        config.id,
                    ),
                )
                new_id = config.id
            row = conn.execute(
                "SELECT * FROM k8s_cluster_configs WHERE id = ?", (new_id,)
            ).fetchone()
        return self._row_to_cluster_config(row)

    def list_cluster_configs(self) -> List[KubeClusterConfig]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM k8s_cluster_configs ORDER BY id"
            ).fetchall()
        return [self._row_to_cluster_config(r) for r in rows]

    def get_cluster_config(self, config_id: int) -> Optional[KubeClusterConfig]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM k8s_cluster_configs WHERE id = ?", (config_id,)
            ).fetchone()
        return self._row_to_cluster_config(row) if row else None

    def delete_cluster_config(self, config_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM k8s_cluster_configs WHERE id = ?", (config_id,)
            )

    def _row_to_cluster_config(self, row: sqlite3.Row) -> KubeClusterConfig:
        return KubeClusterConfig(
            id=row["id"],
            name=row["name"],
            kubeconfig_path=row["kubeconfig_path"],
            context_name=row["context_name"],
            namespaces=json.loads(row["namespaces_json"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    # ------------------------------------------------------------------
    # DeploymentSnapshot
    # ------------------------------------------------------------------

    def save_snapshot(self, snapshot: DeploymentSnapshot) -> DeploymentSnapshot:
        """Insert a new snapshot together with all its image records."""
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO k8s_deployment_snapshots
                    (cluster_config_id, namespace, note)
                VALUES (?, ?, ?)
                """,
                (snapshot.cluster_config_id, snapshot.namespace, snapshot.note),
            )
            snapshot_id = cursor.lastrowid

            for record in snapshot.records:
                for ci in record.containers:
                    conn.execute(
                        """
                        INSERT INTO k8s_image_records
                            (snapshot_id, deployment_name, container_name, image)
                        VALUES (?, ?, ?, ?)
                        """,
                        (snapshot_id, record.deployment_name, ci.container_name, ci.image),
                    )

            row = conn.execute(
                "SELECT * FROM k8s_deployment_snapshots WHERE id = ?", (snapshot_id,)
            ).fetchone()
        return self._load_snapshot_with_records(row)

    def list_snapshots(
        self,
        cluster_config_id: int,
        namespace: Optional[str] = None,
    ) -> List[DeploymentSnapshot]:
        query = (
            "SELECT * FROM k8s_deployment_snapshots WHERE cluster_config_id = ?"
        )
        params: list = [cluster_config_id]
        if namespace:
            query += " AND namespace = ?"
            params.append(namespace)
        query += " ORDER BY id DESC"

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._load_snapshot_with_records(r) for r in rows]

    def get_snapshot(self, snapshot_id: int) -> Optional[DeploymentSnapshot]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM k8s_deployment_snapshots WHERE id = ?", (snapshot_id,)
            ).fetchone()
        if row is None:
            return None
        return self._load_snapshot_with_records(row)

    def delete_snapshot(self, snapshot_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM k8s_deployment_snapshots WHERE id = ?", (snapshot_id,)
            )

    def _load_snapshot_with_records(self, row: sqlite3.Row) -> DeploymentSnapshot:
        snapshot_id = row["id"]
        with self._connect() as conn:
            image_rows = conn.execute(
                "SELECT * FROM k8s_image_records WHERE snapshot_id = ? ORDER BY id",
                (snapshot_id,),
            ).fetchall()

        # Group by deployment_name
        records_map: dict[str, DeploymentImageRecord] = {}
        for ir in image_rows:
            dep = ir["deployment_name"]
            if dep not in records_map:
                records_map[dep] = DeploymentImageRecord(
                    deployment_name=dep,
                    id=None,
                    snapshot_id=snapshot_id,
                )
            records_map[dep].containers.append(
                ContainerImage(
                    container_name=ir["container_name"],
                    image=ir["image"],
                )
            )

        return DeploymentSnapshot(
            id=snapshot_id,
            cluster_config_id=row["cluster_config_id"],
            namespace=row["namespace"],
            note=row["note"],
            created_at=row["created_at"],
            records=list(records_map.values()),
        )
