from db_schema_sync_client.services.cluster_service import ClusterService


def test_cluster_service_builds_overview_summary():
    service = ClusterService()

    overview = service.build_overview(
        cluster_name="HIS-PROD",
        patroni_nodes=[
            {"name": "pg01", "role": "leader", "state": "running", "timeline": 12, "lag": None, "pending_restart": False, "last_seen": "10:21:03"},
            {"name": "pg02", "role": "replica", "state": "running", "timeline": 12, "lag": "0 MB", "pending_restart": False, "last_seen": "10:21:01"},
            {"name": "pg03", "role": "replica", "state": "running", "timeline": 12, "lag": "84 MB", "pending_restart": True, "last_seen": "10:20:54"},
        ],
        etcd_members=[
            {"name": "etcd01", "healthy": True},
            {"name": "etcd02", "healthy": True},
            {"name": "etcd03", "healthy": True},
        ],
        pg_stats={"total_connections": 120, "active_connections": 18},
        recent_operations=[
            {"created_at": "2026-04-14 10:03", "operator": "admin", "action": "reload", "status": "success", "detail": "pg02"},
        ],
    )

    assert overview.primary_node == "pg01"
    assert overview.replica_count == 2
    assert overview.patroni_healthy_count == 3
    assert overview.etcd_healthy_count == 3
    assert overview.total_connections == 120
    assert overview.active_connections == 18
    assert overview.status == "healthy"
    assert len(overview.nodes) == 3


def test_cluster_service_marks_partial_failure_when_a_source_is_missing():
    service = ClusterService()

    overview = service.build_overview(
        cluster_name="HIS-UAT",
        patroni_nodes=[
            {"name": "pg01", "role": "leader", "state": "running", "timeline": 5, "lag": None, "pending_restart": False, "last_seen": "10:21:03"},
        ],
        etcd_members=None,
        pg_stats={"total_connections": 20, "active_connections": 4},
        recent_operations=[],
    )

    assert overview.status == "partial_failure"
    assert overview.primary_node == "pg01"
    assert overview.etcd_healthy_count == 0
