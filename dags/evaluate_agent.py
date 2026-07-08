"""evaluate_agent: run mini-swe-agent on a SWE-bench slice and evaluate the patches.

Pipeline:
    prepare_run -> run_agent -> run_eval -> summarize -> upload_artifacts -> log_mlflow

Artifacts (Phase 2 layout):
    runs/<run-id>/
      config.json
      run-agent/            preds.json + trajectories + agent logs
      run-eval/             harness report + logs/run_evaluation/<run-id>/
      metrics.json
      manifest.json         pointers + S3 artifact_uri + MLflow run id

Design notes:
- Airflow standalone runs in its own `uv tool` environment which does NOT have the
  project deps. Anything that needs mini-swe-agent / swebench / boto3 / mlflow is
  executed via `uv run ...` subprocesses so it uses the project venv instead
  (see pipeline/upload_artifacts.py and pipeline/log_mlflow.py).
- `.env` at the project root is loaded explicitly and merged into subprocess
  environments, so the DAG works regardless of how Airflow was started.
- The batch command `mini-extra swebench` has no --cost-limit flag; the cost limit
  is passed as a config override: `-c swebench.yaml -c agent.cost_limit=<v>` (the
  default config file must be re-passed explicitly whenever -c is used).
"""

import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_ROOT = PROJECT_ROOT / "runs"

DATASET_BY_SUBSET = {
    "verified": "princeton-nlp/SWE-bench_Verified",
    "lite": "princeton-nlp/SWE-bench_Lite",
    "full": "princeton-nlp/SWE-bench",
}


def _load_dotenv() -> dict:
    """Parse PROJECT_ROOT/.env (KEY=VALUE lines) without external deps."""
    env = {}
    dotenv = PROJECT_ROOT / ".env"
    if dotenv.exists():
        for line in dotenv.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip().strip("'\"")
    return env


def _sh(cmd: list, cwd: Path, extra_env: dict | None = None, log_file: Path | None = None):
    """Run a command with .env merged in, teeing output to a log file in the run dir."""
    cmd = [str(c) for c in cmd]
    env = {**os.environ, **_load_dotenv(), **(extra_env or {})}
    print(f"Running: {' '.join(cmd)} (cwd={cwd})")
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with open(log_file, "w") as fh:
            proc = subprocess.run(cmd, cwd=cwd, env=env, stdout=fh, stderr=subprocess.STDOUT)
        print(f"Output written to {log_file}; last lines:")
        print("\n".join(log_file.read_text().splitlines()[-20:]))
    else:
        proc = subprocess.run(cmd, cwd=cwd, env=env)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {proc.returncode}: {' '.join(cmd)}")


@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["swe-bench", "mini-swe-agent"],
    params={
        # --- required by the assignment ---
        "split": Param("test", type="string", description="SWE-bench split (test/dev)"),
        "subset": Param(
            "verified",
            type="string",
            enum=sorted(DATASET_BY_SUBSET),
            description="SWE-bench subset",
        ),
        "workers": Param(4, type="integer", minimum=1,
                         description="Parallel workers for agent and evaluation"),
        # --- optional but useful ---
        "model": Param("nebius/moonshotai/Kimi-K2.6", type="string",
                       description="LiteLLM model id used by mini-swe-agent"),
        "task_slice": Param("0:3", type="string",
                            description="Slice of dataset instances, e.g. 0:3"),
        "run_id": Param(None, type=["null", "string"],
                        description="Run id; auto-generated from UTC timestamp if empty"),
        "cost_limit": Param(None, type=["null", "number"], minimum=0,
                            description="Per-instance cost limit in $ (agent.cost_limit "
                                        "override). Empty keeps the config default; 0 disables."),
        "upload": Param(True, type="boolean",
                        description="Upload runs/<run-id>/ to S3/MinIO after evaluation"),
    },
)
def evaluate_agent():

    @task
    def prepare_run(**context) -> dict:
        """Resolve params into an immutable run config and create runs/<run-id>/."""
        p = context["params"]
        run_id = p["run_id"] or f"run-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
        run_dir = RUNS_ROOT / run_id
        (run_dir / "run-agent").mkdir(parents=True, exist_ok=True)
        (run_dir / "run-eval").mkdir(parents=True, exist_ok=True)

        config = {
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "split": p["split"],
            "subset": p["subset"],
            "dataset_name": DATASET_BY_SUBSET[p["subset"]],
            "workers": int(p["workers"]),
            "model": p["model"],
            "task_slice": p["task_slice"],
            "cost_limit": p["cost_limit"],
            "airflow_dag_run_id": context["dag_run"].run_id,
        }
        (run_dir / "config.json").write_text(json.dumps(config, indent=2))
        print(f"Prepared {run_dir}")
        return config

    @task(retries=1, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(hours=2))
    def run_agent(config: dict) -> str:
        """Run the mini-swe-agent batch; writes trajectories + preds.json to run-agent/."""
        run_dir = RUNS_ROOT / config["run_id"]
        agent_dir = run_dir / "run-agent"
        cmd = [
            "uv", "run", "mini-extra", "swebench",
            "--subset", config["subset"],
            "--split", config["split"],
            "--model", config["model"],
            "--slice", config["task_slice"],
            "--workers", config["workers"],
            "-o", agent_dir,
        ]
        if config["cost_limit"] is not None:
            # -c replaces the default config, so re-pass it before the override.
            cmd += ["-c", "swebench.yaml", "-c", f"agent.cost_limit={config['cost_limit']}"]
        _sh(
            cmd,
            cwd=PROJECT_ROOT,
            extra_env={"MSWEA_COST_TRACKING": "ignore_errors"},
            log_file=agent_dir / "agent-stdout.log",
        )
        preds = agent_dir / "preds.json"
        if not preds.exists():
            raise FileNotFoundError(f"Agent did not produce {preds}")
        return str(preds)

    @task(retries=1, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(hours=2))
    def run_eval(config: dict, preds_path: str) -> str:
        """Evaluate preds.json with the SWE-bench harness; report + logs land in run-eval/."""
        run_dir = RUNS_ROOT / config["run_id"]
        eval_dir = run_dir / "run-eval"
        # cwd=eval_dir: the harness writes its report and logs/ relative to CWD.
        _sh(
            [
                "uv", "run", "python", "-m", "swebench.harness.run_evaluation",
                "--dataset_name", config["dataset_name"],
                "--predictions_path", preds_path,
                "--max_workers", config["workers"],
                "--run_id", config["run_id"],
            ],
            cwd=eval_dir,
            log_file=eval_dir / "eval-stdout.log",
        )
        reports = sorted(eval_dir.glob(f"*.{config['run_id']}.json"))
        if not reports:
            raise FileNotFoundError(
                f"No evaluation report *.{config['run_id']}.json in {eval_dir}"
            )
        return str(reports[0])

    @task
    def summarize(config: dict, report_path: str) -> dict:
        """Parse the harness report into metrics.json and write manifest.json."""
        run_dir = RUNS_ROOT / config["run_id"]
        report = json.loads(Path(report_path).read_text())

        submitted = report.get("submitted_instances", 0)
        resolved = report.get("resolved_instances", 0)
        metrics = {
            "submitted_instances": submitted,
            "completed_instances": report.get("completed_instances", 0),
            "resolved_instances": resolved,
            "unresolved_instances": report.get("unresolved_instances", 0),
            "empty_patch_instances": report.get("empty_patch_instances", 0),
            "error_instances": report.get("error_instances", 0),
            "resolved_rate": round(resolved / submitted, 4) if submitted else 0.0,
        }
        (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))

        manifest = {
            "run_id": config["run_id"],
            "config": "config.json",
            "predictions": "run-agent/preds.json",
            "trajectories": "run-agent/",
            "eval_report": str(Path(report_path).relative_to(run_dir)),
            "eval_logs": f"run-eval/logs/run_evaluation/{config['run_id']}/",
            "metrics": "metrics.json",
            "artifact_uri": None,   # set by upload_artifacts
            "mlflow_run_id": None,  # set by log_mlflow
        }
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
        print(f"Metrics: {json.dumps(metrics)}")
        return metrics

    @task(retries=2, retry_delay=timedelta(seconds=30),
          execution_timeout=timedelta(minutes=15))
    def upload_artifacts(config: dict, metrics: dict, **context) -> str | None:
        """Sync runs/<run-id>/ to s3://$S3_BUCKET/runs/<run-id>/ (MinIO or cloud S3)."""
        if not context["params"]["upload"]:
            raise AirflowSkipException("upload=false — skipping object-storage upload")
        run_dir = RUNS_ROOT / config["run_id"]
        _sh(
            ["uv", "run", "python", "-m", "pipeline.upload_artifacts", "--run-dir", run_dir],
            cwd=PROJECT_ROOT,
        )
        manifest = json.loads((run_dir / "manifest.json").read_text())
        return manifest.get("artifact_uri")

    @task(retries=2, retry_delay=timedelta(seconds=30), trigger_rule="none_failed")
    def log_mlflow(config: dict) -> None:
        """Log params/metrics/artifact refs to MLflow (runs after upload, or its skip)."""
        run_dir = RUNS_ROOT / config["run_id"]
        _sh(
            ["uv", "run", "python", "-m", "pipeline.log_mlflow", "--run-dir", run_dir],
            cwd=PROJECT_ROOT,
        )

    config = prepare_run()
    preds = run_agent(config)
    report = run_eval(config, preds)
    metrics = summarize(config, report)
    uploaded = upload_artifacts(config, metrics)
    logged = log_mlflow(config)
    uploaded >> logged


evaluate_agent()
