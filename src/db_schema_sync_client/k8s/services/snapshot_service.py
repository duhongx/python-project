"""Snapshot service: create and manage deployment image snapshots."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from db_schema_sync_client.k8s.domain.models import (
    ContainerImage,
    DeploymentImageRecord,
    DeploymentInfo,
    DeploymentSnapshot,
    KubeClusterConfig,
)
from db_schema_sync_client.k8s.infrastructure.k8s_client import K8sClient
from db_schema_sync_client.k8s.infrastructure.k8s_store import K8sStore
from db_schema_sync_client.k8s.infrastructure.kubeconfig_store import KubeconfigStore


class SnapshotService:
    """Orchestrates fetching live deployment data and persisting snapshots."""

    def __init__(self, k8s_store: K8sStore, kubeconfig_store: KubeconfigStore) -> None:
        self._k8s_store = k8s_store
        self._kubeconfig_store = kubeconfig_store

    def _make_client(self, config: KubeClusterConfig) -> K8sClient:
        return K8sClient(
            kubeconfig_path=Path(config.kubeconfig_path),
            context_name=config.context_name,
        )

    def list_deployments(
        self, config: KubeClusterConfig, namespace: str
    ) -> List[DeploymentInfo]:
        """Fetch live deployments from the cluster for the given namespace."""
        client = self._make_client(config)
        return client.list_deployments(namespace)

    def create_snapshot(
        self,
        config: KubeClusterConfig,
        namespace: str,
        deployment_names: List[str],
        note: str = "",
    ) -> DeploymentSnapshot:
        """Fetch current images for *deployment_names* and persist a snapshot.

        If *deployment_names* is empty, all deployments in the namespace are
        included (full-namespace snapshot).
        """
        client = self._make_client(config)
        live = client.list_deployments(namespace)

        if deployment_names:
            name_set = set(deployment_names)
            live = [d for d in live if d.name in name_set]

        records = [
            DeploymentImageRecord(
                deployment_name=dep.name,
                containers=[
                    ContainerImage(
                        container_name=ci.container_name,
                        image=ci.image,
                    )
                    for ci in dep.containers
                ],
            )
            for dep in live
        ]

        snapshot = DeploymentSnapshot(
            cluster_config_id=config.id,
            namespace=namespace,
            note=note,
            records=records,
        )
        return self._k8s_store.save_snapshot(snapshot)

    def list_snapshots(
        self,
        cluster_config_id: int,
        namespace: Optional[str] = None,
    ) -> List[DeploymentSnapshot]:
        return self._k8s_store.list_snapshots(cluster_config_id, namespace)

    def get_snapshot(self, snapshot_id: int) -> Optional[DeploymentSnapshot]:
        return self._k8s_store.get_snapshot(snapshot_id)

    def delete_snapshot(self, snapshot_id: int) -> None:
        self._k8s_store.delete_snapshot(snapshot_id)
