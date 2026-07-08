#!/usr/bin/env bash
# Start a local MLflow tracking server (Phase 2: SQLite backend, local artifact store).
# Runs in the foreground — launch inside tmux/screen, or append `&` yourself.
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p mlflow-data

exec uv run mlflow server \
    --host 0.0.0.0 \
    --port "${MLFLOW_PORT:-5000}" \
    --backend-store-uri "sqlite:///mlflow-data/mlflow.db" \
    --artifacts-destination "./mlflow-data/artifacts"
