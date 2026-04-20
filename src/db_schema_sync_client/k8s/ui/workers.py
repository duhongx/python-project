"""Background workers for Kubernetes operations."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from PyQt6.QtCore import QThread, pyqtSignal

from db_schema_sync_client.k8s.domain.models import DeploymentInfo, KubeClusterConfig
from db_schema_sync_client.k8s.services.rollback_service import RollbackResult, RollbackService
from db_schema_sync_client.k8s.services.snapshot_service import SnapshotService


class FetchDeploymentsWorker(QThread):
    """Fetch live deployments from the cluster in a background thread."""

    finished = pyqtSignal(list)   # List[DeploymentInfo]
    error = pyqtSignal(str)

    def __init__(
        self,
        snapshot_service: SnapshotService,
        config: KubeClusterConfig,
        namespace: str,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._service = snapshot_service
        self._config = config
        self._namespace = namespace

    def run(self) -> None:
        try:
            deployments = self._service.list_deployments(self._config, self._namespace)
            self.finished.emit(deployments)
        except Exception as exc:
            self.error.emit(str(exc))


class CreateSnapshotWorker(QThread):
    """Create a deployment snapshot in a background thread."""

    finished = pyqtSignal(object)  # DeploymentSnapshot
    error = pyqtSignal(str)

    def __init__(
        self,
        snapshot_service: SnapshotService,
        config: KubeClusterConfig,
        namespace: str,
        deployment_names: List[str],
        note: str = "",
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._service = snapshot_service
        self._config = config
        self._namespace = namespace
        self._deployment_names = deployment_names
        self._note = note

    def run(self) -> None:
        try:
            snapshot = self._service.create_snapshot(
                config=self._config,
                namespace=self._namespace,
                deployment_names=self._deployment_names,
                note=self._note,
            )
            self.finished.emit(snapshot)
        except Exception as exc:
            self.error.emit(str(exc))


class RollbackWorker(QThread):
    """Execute a rollback in a background thread."""

    finished = pyqtSignal(object)  # RollbackResult
    progress = pyqtSignal(str)     # real-time status message
    error = pyqtSignal(str)

    def __init__(
        self,
        rollback_service: RollbackService,
        config: KubeClusterConfig,
        snapshot_id: int,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._service = rollback_service
        self._config = config
        self._snapshot_id = snapshot_id

    def run(self) -> None:
        try:
            result = self._service.execute_rollback(
                self._config,
                self._snapshot_id,
                progress_cb=self.progress.emit,
            )
            self.finished.emit(result)
        except Exception as exc:
            self.error.emit(str(exc))
