"""Background workers for long-running operations (metadata loading, comparison, sync)."""

from __future__ import annotations

from typing import Optional

from PyQt6.QtCore import QThread, pyqtSignal

from db_schema_sync_client.domain.diff import SchemaDiff
from db_schema_sync_client.domain.models import ConnectionProfile, SchemaSnapshot
from db_schema_sync_client.infrastructure.db_metadata import MetadataFilters, MetadataReader
from db_schema_sync_client.services.comparator import SchemaComparator
from db_schema_sync_client.services.sql_generator import GeneratedSqlPlan
from db_schema_sync_client.services.sync_executor import SyncExecutionResult, SyncExecutor


class MetadataWorker(QThread):
    """Load a SchemaSnapshot from a database in a background thread."""

    finished = pyqtSignal(object)  # SchemaSnapshot or None
    error = pyqtSignal(str)
    progress = pyqtSignal(int, int)  # current, total

    def __init__(
        self,
        reader: MetadataReader,
        profile: ConnectionProfile,
        password: str,
        filters: Optional[MetadataFilters] = None,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.reader = reader
        self.profile = profile
        self.password = password
        self.filters = filters

    def run(self) -> None:
        try:
            self.progress.emit(0, 0)  # indeterminate start
            snapshot = self.reader.load_snapshot(self.profile, self.password, self.filters)
            self.progress.emit(1, 1)
            self.finished.emit(snapshot)
        except Exception as exc:
            error_msg = str(exc).replace(self.password, "***")
            self.error.emit(error_msg)


class CompareWorker(QThread):
    """Run schema comparison in a background thread."""

    finished = pyqtSignal(object)  # SchemaDiff
    error = pyqtSignal(str)
    progress = pyqtSignal(int, int)  # current, total

    def __init__(
        self,
        source_snapshot: SchemaSnapshot,
        target_snapshot: SchemaSnapshot,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.source_snapshot = source_snapshot
        self.target_snapshot = target_snapshot

    def run(self) -> None:
        try:
            self.progress.emit(0, 0)
            comparator = SchemaComparator()
            diff = comparator.compare(self.source_snapshot, self.target_snapshot)
            self.progress.emit(1, 1)
            self.finished.emit(diff)
        except Exception as exc:
            self.error.emit(str(exc))


class SyncWorker(QThread):
    """Execute sync SQL statements in a background thread."""

    finished = pyqtSignal(object)  # SyncExecutionResult
    error = pyqtSignal(str)
    progress = pyqtSignal(int, int)  # current, total

    def __init__(
        self,
        executor: SyncExecutor,
        plan: GeneratedSqlPlan,
        target_profile: ConnectionProfile,
        password: str,
        selected_fields: Optional[list] = None,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.executor = executor
        self.plan = plan
        self.target_profile = target_profile
        self.password = password
        self.selected_fields = selected_fields

    def run(self) -> None:
        try:
            total = len(self.plan.statements)
            self.progress.emit(0, total)
            result = self.executor.execute(
                self.plan,
                self.target_profile,
                self.password,
                confirmed=True,
                progress_callback=lambda current: self.progress.emit(current, total),
                selected_fields=self.selected_fields,
            )
            self.finished.emit(result)
        except Exception as exc:
            error_msg = str(exc).replace(self.password, "***")
            self.error.emit(error_msg)
