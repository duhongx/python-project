"""Domain models for Kubernetes deployment management."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class KubeClusterConfig:
    """A registered Kubernetes cluster with its kubeconfig and namespace list."""

    name: str
    kubeconfig_path: str
    context_name: str
    namespaces: List[str]
    id: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class ContainerImage:
    """A single container's image record within a deployment."""

    container_name: str
    image: str


@dataclass
class DeploymentImageRecord:
    """All container images for a single deployment at snapshot time."""

    deployment_name: str
    containers: List[ContainerImage] = field(default_factory=list)
    id: Optional[int] = None
    snapshot_id: Optional[int] = None


@dataclass
class DeploymentSnapshot:
    """A named snapshot of image versions for a set of deployments in a namespace."""

    cluster_config_id: int
    namespace: str
    note: str
    records: List[DeploymentImageRecord] = field(default_factory=list)
    id: Optional[int] = None
    created_at: Optional[str] = None


@dataclass
class DeploymentInfo:
    """Live deployment information fetched from the cluster."""

    name: str
    namespace: str
    replicas: int
    ready_replicas: int
    containers: List[ContainerImage] = field(default_factory=list)
