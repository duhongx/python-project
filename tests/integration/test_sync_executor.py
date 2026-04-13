from db_schema_sync_client.domain.models import ConnectionProfile, ConnectionRole, DatabaseType
from db_schema_sync_client.infrastructure.app_store import AppStore
from db_schema_sync_client.infrastructure.credentials import InMemoryCredentialStore
from db_schema_sync_client.services.sql_generator import GeneratedSqlPlan
from db_schema_sync_client.services.sync_executor import SyncExecutor


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, sql):
        self.connection.executed.append(sql)
        if sql in self.connection.fail_on:
            raise RuntimeError(f"boom: {sql}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self, fail_on=None):
        self.fail_on = set(fail_on or [])
        self.executed = []
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        self.closed = True


class FakeConnectionFactory:
    def __init__(self, connection):
        self.connection = connection

    def connect(self, profile, password):
        return self.connection


def make_profile() -> ConnectionProfile:
    return ConnectionProfile(
        id=1,
        name="target-kb",
        role=ConnectionRole.TARGET,
        db_type=DatabaseType.KINGBASE,
        host="127.0.0.1",
        port=5432,
        database="demo",
        username="demo_user",
        credential_key="cred-1",
        is_default=True,
    )


def test_sync_executor_requires_confirmation(tmp_path):
    store = AppStore(tmp_path / "app.db", credential_store=InMemoryCredentialStore())
    store.initialize()
    executor = SyncExecutor(
        app_store=store,
        connection_factory=FakeConnectionFactory(FakeConnection()),
    )

    plan = GeneratedSqlPlan(
        target_type=DatabaseType.KINGBASE,
        statements=['ALTER TABLE "df_demo"."users" ADD COLUMN "name" character varying(100);'],
    )

    try:
        executor.execute(plan, make_profile(), password="secret", confirmed=False)
    except ValueError as exc:
        assert str(exc) == "Sync execution requires explicit confirmation"
    else:
        raise AssertionError("expected ValueError when confirmed=False")


def test_sync_executor_executes_each_statement_and_persists_history(tmp_path):
    store = AppStore(tmp_path / "app.db", credential_store=InMemoryCredentialStore())
    store.initialize()
    fake_connection = FakeConnection()
    executor = SyncExecutor(
        app_store=store,
        connection_factory=FakeConnectionFactory(fake_connection),
    )

    plan = GeneratedSqlPlan(
        target_type=DatabaseType.KINGBASE,
        statements=[
            'ALTER TABLE "df_demo"."users" ADD COLUMN "name" character varying(100);',
            'ALTER TABLE "df_demo"."users" ADD COLUMN "code" character varying(32);',
        ],
    )

    result = executor.execute(plan, make_profile(), password="secret", confirmed=True)

    assert result.success_count == 2
    assert result.failure_count == 0
    assert fake_connection.commit_count == 2
    assert fake_connection.rollback_count == 0
    assert fake_connection.closed is True

    statements = store.list_sync_statements(result.run_id)
    assert [item["status"] for item in statements] == ["success", "success"]
    assert all("secret" not in item["statement_text"] for item in statements)


def test_sync_executor_rolls_back_failed_statement_and_continues(tmp_path):
    store = AppStore(tmp_path / "app.db", credential_store=InMemoryCredentialStore())
    store.initialize()
    failing_sql = 'ALTER TABLE "df_demo"."users" ADD COLUMN "code" character varying(32);'
    fake_connection = FakeConnection(fail_on={failing_sql})
    executor = SyncExecutor(
        app_store=store,
        connection_factory=FakeConnectionFactory(fake_connection),
    )

    plan = GeneratedSqlPlan(
        target_type=DatabaseType.KINGBASE,
        statements=[
            'ALTER TABLE "df_demo"."users" ADD COLUMN "name" character varying(100);',
            failing_sql,
            'ALTER TABLE "df_demo"."users" ADD COLUMN "age" integer;',
        ],
    )

    result = executor.execute(plan, make_profile(), password="secret", confirmed=True)

    assert result.success_count == 2
    assert result.failure_count == 1
    assert fake_connection.commit_count == 2
    assert fake_connection.rollback_count == 1
    assert result.results[1].status == "failed"
    assert "secret" not in result.results[1].error_message

    statements = store.list_sync_statements(result.run_id)
    assert [item["status"] for item in statements] == ["success", "failed", "success"]
