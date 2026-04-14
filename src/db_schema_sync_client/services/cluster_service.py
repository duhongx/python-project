"""Cluster overview aggregation service."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional


@dataclass(frozen=True)
class ClusterNodeStatus:
    name: str
    role: str
    status: str
    timeline: str
    lag: str
    pending_restart: bool
    last_seen: str


@dataclass(frozen=True)
class ClusterOverview:
    cluster_name: str
    status: str
    primary_node: str
    replica_count: int
    patroni_healthy_count: int
    patroni_total_count: int
    etcd_healthy_count: int
    etcd_total_count: int
    total_connections: int
    active_connections: int
    topology_lines: tuple[str, ...] = field(default_factory=tuple)
    nodes: tuple[ClusterNodeStatus, ...] = field(default_factory=tuple)
    recent_operations: tuple[dict, ...] = field(default_factory=tuple)


class ClusterService:
    def __init__(
        self,
        patroni_loader: Optional[Callable[[object], Optional[Iterable[dict]]]] = None,
        etcd_loader: Optional[Callable[[object], Optional[Iterable[dict]]]] = None,
        pg_stats_loader: Optional[Callable[[object], Optional[dict]]] = None,
    ) -> None:
        self.patroni_loader = patroni_loader or self._default_patroni_loader
        self.etcd_loader = etcd_loader or self._default_etcd_loader
        self.pg_stats_loader = pg_stats_loader or self._default_pg_stats_loader

    def load_overview(self, cluster, app_store) -> ClusterOverview:
        patroni_nodes = self.patroni_loader(cluster)
        etcd_members = self.etcd_loader(cluster)
        pg_stats = self.pg_stats_loader(cluster)
        recent_operations = app_store.list_cluster_audit_logs(cluster.id, limit=20)
        return self.build_overview(
            cluster_name=cluster.name,
            patroni_nodes=patroni_nodes,
            etcd_members=etcd_members,
            pg_stats=pg_stats,
            recent_operations=recent_operations,
        )

    def build_overview(
        self,
        *,
        cluster_name: str,
        patroni_nodes: Optional[Iterable[dict]],
        etcd_members: Optional[Iterable[dict]],
        pg_stats: Optional[dict],
        recent_operations: Iterable[dict],
    ) -> ClusterOverview:
        patroni_list = list(patroni_nodes or [])
        etcd_list = list(etcd_members or [])
        pg_stats_dict = pg_stats or {}

        normalized_nodes = tuple(self._normalize_node(node) for node in patroni_list)
        primary_node = next((node.name for node in normalized_nodes if node.role.lower() == "primary"), "")
        replica_count = sum(1 for node in normalized_nodes if node.role.lower() == "replica")
        patroni_healthy_count = sum(1 for node in patroni_list if str(node.get("state", "")).lower() == "running")
        etcd_healthy_count = sum(1 for member in etcd_list if bool(member.get("healthy")))

        topology_lines = tuple(
            f"{primary_node or 'unknown'} (Primary)  --->  {node.name} (Replica)"
            for node in normalized_nodes
            if node.role.lower() == "replica"
        )

        source_failures = 0
        if patroni_nodes is None:
            source_failures += 1
        if etcd_members is None:
            source_failures += 1
        if pg_stats is None:
            source_failures += 1

        status = "partial_failure" if source_failures else "healthy"
        if any(node.status not in {"正常", "running"} for node in normalized_nodes):
            status = "warning" if not source_failures else "partial_failure"

        return ClusterOverview(
            cluster_name=cluster_name,
            status=status,
            primary_node=primary_node,
            replica_count=replica_count,
            patroni_healthy_count=patroni_healthy_count,
            patroni_total_count=len(patroni_list),
            etcd_healthy_count=etcd_healthy_count,
            etcd_total_count=len(etcd_list),
            total_connections=int(pg_stats_dict.get("total_connections", 0) or 0),
            active_connections=int(pg_stats_dict.get("active_connections", 0) or 0),
            topology_lines=topology_lines,
            nodes=normalized_nodes,
            recent_operations=tuple(recent_operations),
        )

    def _normalize_node(self, node: dict) -> ClusterNodeStatus:
        role_raw = str(node.get("role", "")).lower()
        role = "Primary" if role_raw in {"leader", "primary"} else "Replica"
        state = str(node.get("state", "")).lower()
        status = "正常" if state == "running" else (node.get("status") or "告警")
        return ClusterNodeStatus(
            name=str(node.get("name", "")),
            role=role,
            status=str(status),
            timeline=str(node.get("timeline", "")),
            lag="-" if node.get("lag") in (None, "") else str(node.get("lag")),
            pending_restart=bool(node.get("pending_restart")),
            last_seen=str(node.get("last_seen", "")),
        )

    def _default_patroni_loader(self, cluster) -> list[dict]:
        return [
            {"name": "pg01", "role": "leader", "state": "running", "timeline": 1, "lag": None, "pending_restart": False, "last_seen": "-"},
            {"name": "pg02", "role": "replica", "state": "running", "timeline": 1, "lag": "0 MB", "pending_restart": False, "last_seen": "-"},
        ]

    def _default_etcd_loader(self, cluster) -> list[dict]:
        return [{"name": "etcd01", "healthy": True}]

    def _default_pg_stats_loader(self, cluster) -> dict:
        return {"total_connections": 0, "active_connections": 0}
