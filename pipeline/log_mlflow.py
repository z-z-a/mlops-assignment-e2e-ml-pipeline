"""Log a completed runs/<run-id>/ folder to MLflow.

Usage (from the project root, inside the project venv):

    uv run python -m pipeline.log_mlflow --run-dir runs/<run-id>

Environment:
    MLFLOW_TRACKING_URI   default http://localhost:5000
    MLFLOW_EXPERIMENT     default swe-bench-agent-eval

Reads config.json / metrics.json / manifest.json from the run dir and logs
params, metrics, tags (incl. the S3 artifact_uri if uploaded), and the small
key artifacts. Writes the resulting MLflow run id back into manifest.json.
"""

import argparse
import json
import os
import sys
from pathlib import Path

import mlflow

PARAM_KEYS = (
    "run_id",
    "split",
    "subset",
    "dataset_name",
    "model",
    "task_slice",
    "workers",
    "cost_limit",
)


def log_run(run_dir: Path) -> str:
    config = json.loads((run_dir / "config.json").read_text())
    metrics = json.loads((run_dir / "metrics.json").read_text())
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(os.environ.get("MLFLOW_EXPERIMENT", "swe-bench-agent-eval"))

    with mlflow.start_run(run_name=config["run_id"]) as run:
        mlflow.log_params({k: config.get(k) for k in PARAM_KEYS})
        mlflow.log_metrics({k: v for k, v in metrics.items() if isinstance(v, (int, float))})

        mlflow.set_tag("run_dir", str(run_dir))
        if config.get("airflow_dag_run_id"):
            mlflow.set_tag("airflow_dag_run_id", config["airflow_dag_run_id"])
        if manifest.get("artifact_uri"):
            mlflow.set_tag("artifact_uri", manifest["artifact_uri"])

        # Small, high-signal artifacts only; the full tree lives in object storage.
        for rel in ("config.json", "metrics.json", "manifest.json", "run-agent/preds.json"):
            path = run_dir / rel
            if path.exists():
                mlflow.log_artifact(str(path))
        if manifest.get("eval_report") and (run_dir / manifest["eval_report"]).exists():
            mlflow.log_artifact(str(run_dir / manifest["eval_report"]))

        mlflow_run_id = run.info.run_id

    if manifest:
        manifest["mlflow_run_id"] = mlflow_run_id
        manifest["mlflow_tracking_uri"] = tracking_uri
        manifest_path.write_text(json.dumps(manifest, indent=2))

    print(f"Logged to MLflow: run_id={mlflow_run_id} tracking_uri={tracking_uri}")
    return mlflow_run_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, help="Path to runs/<run-id>/")
    args = parser.parse_args()

    run_dir = Path(args.run_dir).resolve()
    if not (run_dir / "metrics.json").exists():
        sys.exit(f"metrics.json not found in {run_dir} — run the summarize task first")
    log_run(run_dir)


if __name__ == "__main__":
    main()
