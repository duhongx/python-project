#!/usr/bin/env python3
"""Start the desktop application.

Usage:
    python3 start.py

Behaviour:
    1. Find any running process whose command line contains the module path
       'db_schema_sync_client.app'.
    2. Kill those processes (SIGTERM, then SIGKILL if they don't exit in time).
    3. Launch the application using the project venv (if present) or the
       system python3.
"""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
VENV_PYTHON = PROJECT_ROOT / "venv" / "bin" / "python"
MODULE = "db_schema_sync_client.app"


def find_running_pids() -> list[int]:
    """Return PIDs of any running instance of the application."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", MODULE],
            capture_output=True,
            text=True,
        )
        pids = [int(p) for p in result.stdout.split() if p.strip().isdigit()]
        # Exclude the current process and its parent (this script itself)
        current = os.getpid()
        return [p for p in pids if p != current]
    except Exception:
        return []


def kill_running() -> None:
    pids = find_running_pids()
    if not pids:
        return

    print(f"发现已运行的进程: {pids}，正在终止…")
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    # Wait up to 3 seconds for graceful exit
    deadline = time.time() + 3
    remaining = list(pids)
    while remaining and time.time() < deadline:
        time.sleep(0.2)
        remaining = [p for p in remaining if _pid_exists(p)]

    # Force-kill survivors
    for pid in remaining:
        try:
            print(f"进程 {pid} 未响应 SIGTERM，强制终止 (SIGKILL)…")
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    print("已终止所有旧进程。")


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists but not ours to kill


def resolve_python() -> str:
    """Return the python executable to use: venv first, then system python3."""
    if VENV_PYTHON.exists():
        return str(VENV_PYTHON)
    return sys.executable


def start() -> None:
    python = resolve_python()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src")

    print(f"启动应用: {python} -m {MODULE}")
    subprocess.Popen(
        [python, "-m", MODULE],
        env=env,
        cwd=str(PROJECT_ROOT),
    )
    print("应用已启动。")


if __name__ == "__main__":
    kill_running()
    start()
