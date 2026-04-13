#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD/src"
"${PYTHON:-python3}" -m db_schema_sync_client.app
