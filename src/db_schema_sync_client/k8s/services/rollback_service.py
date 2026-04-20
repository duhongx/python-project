"""Rollback service: restore deployment images from a saved snapshot."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional

from db_schema_sync_client.k8s.domain.models import (
    ContainerImage,
    DeploymentSnapshot,
    KubeClusterConfig,
)
from db_schema_sync_client.k8s.infrastructure.k8s_client import K8sClient
from db_schema_sync_client.k8s.infrastructure.k8s_store import K8sStore


@dataclass
class RollbackResult:
    """Outcome of a rollback operation."""

    total: int
    succeeded: int
    skipped: int
    failed: int
    errors: List[str] = field(default_factory=list)
    skipped_names: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.failed == 0

    @property
    def no_changes(self) -> bool:
        """True when every deployment in the snapshot already has the target images."""
        return self.skipped == self.total and self.failed == 0


class RollbackService:
    """Applies a snapshot's images back to the live cluster."""

    def __init__(self, k8s_store: K8sStore) -> None:
        self._k8s_store = k8s_store

    def _make_client(self, config: KubeClusterConfig) -> K8sClient:
        return K8sClient(
            kubeconfig_path=Path(config.kubeconfig_path),
            context_name=config.context_name,
        )

    # Default timeout per deployment (seconds)
    ROLLOUT_TIMEOUT = 300
    ROLLOUT_POLL_INTERVAL = 3

    def execute_rollback(
        self,
        config: KubeClusterConfig,
        snapshot_id: int,
        progress_cb: Optional[Callable[[str], None]] = None,
    ) -> RollbackResult:
        """Patch each deployment in the snapshot back to its recorded images.

        Returns a :class:`RollbackResult` summarising success/failure per
        deployment.  On partial failure, as many deployments as possible are
        patched before the result is returned.
        """
        snapshot = self._k8s_store.get_snapshot(snapshot_id)
        if snapshot is None:
            raise ValueError(f"Snapshot {snapshot_id} not found")

        client = self._make_client(config)
        total = len(snapshot.records)

        # Build a map of current live images: {deployment: {container: image}}
        # This MUST succeed — if we cannot fetch live state we cannot do a
        # meaningful diff and should not patch blindly.
        live_map: dict[str, dict[str, str]] = {}
        live_deployments = client.list_deployments(snapshot.namespace)
        for dep in live_deployments:
            live_map[dep.name] = {
                ci.container_name: ci.image.strip() for ci in dep.containers
            }

        succeeded = 0
        skipped = 0
        skipped_names: List[str] = []
        errors: List[str] = []

        for record in snapshot.records:
            # Compare snapshot images with live images for this deployment
            live_containers = live_map.get(record.deployment_name, {})
            needs_update = any(
                live_containers.get(ci.container_name, "").strip() != ci.image.strip()
                for ci in record.containers
            )

            if not needs_update:
                skipped += 1
                skipped_names.append(record.deployment_name)
                continue

            try:
                if progress_cb:
                    progress_cb(f"正在更新 {record.deployment_name} 镜像…")

                client.patch_deployment_images(
                    namespace=snapshot.namespace,
                    deployment_name=record.deployment_name,
                    container_images=record.containers,
                )

                if progress_cb:
                    progress_cb(
                        f"等待 {record.deployment_name} Pod 就绪（最多 "
                        f"{self.ROLLOUT_TIMEOUT} 秒）…"
                    )

                ready = client.wait_for_rollout(
                    namespace=snapshot.namespace,
                    deployment_name=record.deployment_name,
                    timeout=self.ROLLOUT_TIMEOUT,
                    poll_interval=self.ROLLOUT_POLL_INTERVAL,
                    progress_cb=progress_cb,
                )

                if ready:
                    succeeded += 1
                    if progress_cb:
                        progress_cb(f"{record.deployment_name} ✓ 已就绪")
                else:
                    errors.append(
                        f"{record.deployment_name}: 等待超时（{self.ROLLOUT_TIMEOUT} 秒内 Pod 未全部就绪）"
                    )

            except Exception as exc:
                errors.append(f"{record.deployment_name}: {exc}")

        return RollbackResult(
            total=total,
            succeeded=succeeded,
            skipped=skipped,
            failed=total - succeeded - skipped,
            errors=errors,
            skipped_names=skipped_names,
        )
