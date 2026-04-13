"""Safe statement-by-statement sync execution."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Callable, Optional

from db_schema_sync_client.domain.models import ConnectionProfile
from db_schema_sync_client.infrastructure.app_store import AppStore
from db_schema_sync_client.infrastructure.db_connection import DatabaseConnectionFactory

from .sql_generator import GeneratedSqlPlan


@dataclass(frozen=True)
class ExecutedStatementResult:
    statement: str
    status: str
    error_message: Optional[str]


@dataclass(frozen=True)
class SyncExecutionResult:
    run_id: int
    success_count: int
    failure_count: int
    results: tuple[ExecutedStatementResult, ...] = field(default_factory=tuple)


class SyncExecutor:
    def __init__(
        self,
        app_store: AppStore,
        connection_factory: Optional[DatabaseConnectionFactory] = None,
    ) -> None:
        self.app_store = app_store
        self.connection_factory = connection_factory or DatabaseConnectionFactory()

    def execute(
        self,
        plan: GeneratedSqlPlan,
        target_profile: ConnectionProfile,
        password: str,
        *,
        confirmed: bool,
        run_type: str = "execute",
        progress_callback: Optional[Callable[[int], None]] = None,
        selected_fields: Optional[list[dict]] = None,
    ) -> SyncExecutionResult:
        if not confirmed:
            raise ValueError("Sync execution requires explicit confirmation")

        selected_json = json.dumps(selected_fields, ensure_ascii=False) if selected_fields else None
        run_id = self.app_store.create_sync_run(
            target_profile_id=target_profile.id,
            run_type=run_type,
            status="running",
            selected_fields_json=selected_json,
        )
        results: list[ExecutedStatementResult] = []
        success_count = 0
        failure_count = 0

        conn = self.connection_factory.connect(target_profile, password)
        try:
            for idx, statement in enumerate(plan.statements):
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(statement)
                    conn.commit()
                except Exception as exc:
                    conn.rollback()
                    failure_count += 1
                    error_message = str(exc).replace(password, "***")
                    result = ExecutedStatementResult(
                        statement=statement,
                        status="failed",
                        error_message=error_message,
                    )
                else:
                    success_count += 1
                    result = ExecutedStatementResult(
                        statement=statement,
                        status="success",
                        error_message=None,
                    )

                results.append(result)
                self.app_store.add_sync_statement(
                    sync_run_id=run_id,
                    statement_text=result.statement,
                    status=result.status,
                    error_message=result.error_message,
                )
                if progress_callback is not None:
                    progress_callback(idx + 1)
        finally:
            conn.close()

        final_status = "success" if failure_count == 0 else "partial_failure"
        self.app_store.update_sync_run_status(run_id, final_status)
        return SyncExecutionResult(
            run_id=run_id,
            success_count=success_count,
            failure_count=failure_count,
            results=tuple(results),
        )
