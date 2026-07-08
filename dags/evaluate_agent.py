"""evaluate_agent: run mini-swe-agent on a SWE-bench slice and evaluate the patches.

Pipeline: prepare_run -> run_agent -> run_eval -> summarize_and_log
Artifacts: runs/<run-id>/{config.json, run-agent/, run-eval/, metrics.json, manifest.json}
"""
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_ROOT = PROJECT_ROOT / "runs"

DATASET_BY_SUBSET = {
    "verified": "princeton-nlp/SWE-bench_Verified",
    "lite": "princeton-nlp/SWE-bench_Lite",
    "full": "princeton-nlp/SWE-bench",
}


def _sh(cmd: list[str], cwd: Path, extra_env: dict | None = None, log_file: Path | None = None):
    """Run a command, teeing stdout/stderr to a log file inside the run dir."""
    env = {**os.environ, **(extra_env or {})}
    print("Running:", " ".join(map(str, cmd)), "cwd:", cwd)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with open(log_file, "w") as fh:
            proc = subprocess.run(list(map(str, cmd)), cwd=cwd, env=env,
                                  stdout=fh, stderr=subprocess.STDOUT)
    else:
        proc = subprocess.run(list(map(str, cmd)), cwd=cwd, env=env)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {cmd}")


@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["swe-bench", "mini-swe-agent"],
    params={
        # --- required ---
        "split": Param("test", type="string", description="SWE-bench split"),
        "subset": Param("verified", type="string", enum=list(DATASET_BY_SUBSET),
                        description="SWE-bench subset"),
        "workers": Param(4, type="integer", minimum=1,
                         description="Parallel workers for agent and eval"),
        # --- optional but useful ---
        "model": Param("nebius/moonshotai/Kimi-K2.6", type="string",
                       description="LiteLLM model id used by mini-swe-agent"),
        "task_slice": Param("0:3", type="string",
                            description="Python-style slice of dataset instances, e.g. 0:3"),
        "run_id": Param("", type=["null", "string"],
                        description="Run id; auto-generated from timestamp if empty"),
        "cost_limit": Param(0, type="number",
                            description="Per-instance cost limit; 0 disables"),
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
        return config

    @task
    def run_agent(config: dict) -> str:
        """Run the mini-swe-agent batch; writes trajectories + preds.json to run-agent/."""
        run_dir = RUNS_ROOT / config["run_id"]
        agent_dir = run_dir / "run-agent"
        _sh(
            [
                "uv", "run", "mini-extra", "swebench",
                "--subset", config["subset"],
                "--split", config["split"],
                "--model", config["model"],
                "--slice", config["task_slice"],
                "--workers", config["workers"],
                "-o", agent_dir,
            ],
            cwd=PROJECT_ROOT,
            extra_env={"MSWEA_COST_TRACKING": "ignore_errors"},
            log_file=agent_dir / "agent-stdout.log",
        )
        preds = agent_dir / "preds.json"
        if not preds.exists():
            raise FileNotFoundError(f"Agent did not produce {preds}")
        return str(preds)

    @task
    def run_eval(config: dict, preds_path: str) -> str:
        """Evaluate preds.json with the SWE-bench harness; writes logs + report to run-eval/."""
        run_dir = RUNS_ROOT / config["run_id"]
        eval_dir = run_dir / "run-eval"
        # Run from inside run-eval/ so the harness drops report + logs/ there.
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
        reports = list(eval_dir.glob(f"*.{config['run_id']}.json"))
        if not reports:
            raise FileNotFoundError(f"No evaluation report *.{config['run_id']}.json in {eval_dir}")
        return str(reports[0])

    @task
    def summarize_and_log(config: dict, report_path: str) -> dict:
        """Parse the eval report -> metrics.json + manifest.json, then log to MLflow."""
        import mlflow

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
            "resolved_rate": (resolved / submitted) if submitted else 0.0,
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
            "artifact_uri": None,  # filled by upload_artifacts in Phase 2
        }
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

        mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
        mlflow.set_experiment("swe-bench-agent-eval")
        with mlflow.start_run(run_name=config["run_id"]):
            mlflow.log_params({k: config[k] for k in
                               ("run_id", "split", "subset", "model",
                                "task_slice", "workers", "cost_limit")})
            mlflow.log_metrics(metrics)
            mlflow.set_tag("run_dir", str(run_dir))
            for f in ("config.json", "metrics.json", "manifest.json"):
                mlflow.log_artifact(str(run_dir / f))
            mlflow.log_artifact(str(run_dir / "run-agent" / "preds.json"))
        return metrics

    config = prepare_run()
    preds = run_agent(config)
    report = run_eval(config, preds)
    summarize_and_log(config, report)


evaluate_agent()
