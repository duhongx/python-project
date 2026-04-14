from db_schema_sync_client.domain.models import ClusterEnvironment, ClusterProfile
from db_schema_sync_client.infrastructure.app_store import AppStore
from db_schema_sync_client.infrastructure.credentials import InMemoryCredentialStore


def test_save_cluster_profile_persists_metadata_without_plaintext_password(tmp_path):
    credential_store = InMemoryCredentialStore()
    store = AppStore(tmp_path / "app.db", credential_store=credential_store)
    store.initialize()

    saved = store.save_cluster_profile(
        ClusterProfile(
            name="HIS-PROD",
            environment=ClusterEnvironment.PROD,
            description="Primary production cluster",
            patroni_endpoints=("http://patroni-1:8008", "http://patroni-2:8008"),
            pg_host="10.0.0.10",
            pg_port=5432,
            pg_database="postgres",
            pg_username="postgres",
            etcd_endpoints=("http://etcd-1:2379", "http://etcd-2:2379"),
            is_enabled=True,
        ),
        password="cluster-secret",
    )

    clusters = store.list_cluster_profiles()

    assert len(clusters) == 1
    assert clusters[0].name == "HIS-PROD"
    assert clusters[0].credential_key == saved.credential_key
    assert credential_store.get(saved.credential_key) == "cluster-secret"


def test_list_cluster_profiles_filters_by_environment_and_enabled(tmp_path):
    store = AppStore(tmp_path / "app.db", credential_store=InMemoryCredentialStore())
    store.initialize()

    store.save_cluster_profile(
        ClusterProfile(
            name="HIS-PROD",
            environment=ClusterEnvironment.PROD,
            patroni_endpoints=("http://patroni-prod:8008",),
            pg_host="10.0.0.10",
            pg_port=5432,
            pg_database="postgres",
            pg_username="postgres",
            etcd_endpoints=("http://etcd-prod:2379",),
            is_enabled=True,
        ),
        password="secret-1",
    )
    store.save_cluster_profile(
        ClusterProfile(
            name="HIS-UAT",
            environment=ClusterEnvironment.UAT,
            patroni_endpoints=("http://patroni-uat:8008",),
            pg_host="10.0.0.20",
            pg_port=5432,
            pg_database="postgres",
            pg_username="postgres",
            etcd_endpoints=("http://etcd-uat:2379",),
            is_enabled=False,
        ),
        password="secret-2",
    )

    prod_clusters = store.list_cluster_profiles(environment=ClusterEnvironment.PROD)
    enabled_clusters = store.list_cluster_profiles(enabled_only=True)

    assert [cluster.name for cluster in prod_clusters] == ["HIS-PROD"]
    assert [cluster.name for cluster in enabled_clusters] == ["HIS-PROD"]


def test_save_cluster_profile_updates_existing_record(tmp_path):
    store = AppStore(tmp_path / "app.db", credential_store=InMemoryCredentialStore())
    store.initialize()

    saved = store.save_cluster_profile(
        ClusterProfile(
            name="HIS-UAT",
            environment=ClusterEnvironment.UAT,
            patroni_endpoints=("http://patroni-uat:8008",),
            pg_host="10.0.0.20",
            pg_port=5432,
            pg_database="postgres",
            pg_username="postgres",
            etcd_endpoints=("http://etcd-uat:2379",),
            is_enabled=True,
        ),
        password="secret-1",
    )

    updated = store.save_cluster_profile(
        ClusterProfile(
            id=saved.id,
            credential_key=saved.credential_key,
            name="HIS-UAT-NEW",
            environment=ClusterEnvironment.UAT,
            description="updated",
            patroni_endpoints=("http://patroni-uat:8008", "http://patroni-uat-2:8008"),
            pg_host="10.0.0.21",
            pg_port=5432,
            pg_database="postgres",
            pg_username="postgres",
            etcd_endpoints=("http://etcd-uat:2379",),
            is_enabled=False,
        ),
        password="secret-2",
    )

    clusters = store.list_cluster_profiles()

    assert len(clusters) == 1
    assert updated.id == saved.id
    assert clusters[0].name == "HIS-UAT-NEW"
    assert clusters[0].is_enabled is False
