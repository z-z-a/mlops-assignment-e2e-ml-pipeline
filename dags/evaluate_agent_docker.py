"""evaluate_agent_docker: Phase 3 variant of evaluate_agent using DockerOperator.

Same pipeline as dags/evaluate_agent.py, but the heavy steps run in isolated
containers built from the project Dockerfile (image: agent-eval:latest):

    prepare_run (python) -> run_agent (docker) -> run_eval (docker)
      -> summarize (python) -> upload_artifacts (docker) -> log_mlflow (docker)

Path vantage points (the same runs/ directory, three views):
- HOST_PROJECT_DIR/runs         on the VM (bind-mount source for sibling containers)
- /opt/airflow/runs             inside Airflow containers (compose volume)
- /mlops-assignment/runs        inside agent-eval containers (DockerOperator mount)

Notes:
- Airflow talks to the host Docker daemon through /var/run/docker.sock, so
  DockerOperator containers are SIBLINGS on the host: every Mount source must be
  a host-absolute path (hence HOST_PROJECT_DIR).
- run_agent and run_eval both mount the docker socket: mini-swe-agent sandboxes
  each instance in a docker container, and the SWE-bench harness spawns
  per-instance test containers.
- Containers join the compose network (DOCKER_NETWORK) so they can reach
  http://minio:9000 and http://mlflow:5000 by service name.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Inside Airflow containers (compose mounts ./runs here):
AIRFLOW_RUNS_ROOT = Path(os.environ.get("AIRFLOW_RUNS_ROOT", "/opt/airflow/runs"))
# On the VM — required for DockerOperator bind mounts (sibling containers):
HOST_PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", "")
# Inside agent-eval containers (image WORKDIR is /mlops-assignment):
CONTAINER_RUNS = "/mlops-assignment/runs"

AGENT_IMAGE = os.environ.get("AGENT_IMAGE", "agent-eval:latest")
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "mlops_default")

DATASET_BY_SUBSET = {
    "verified": "princeton-nlp/SWE-bench_Verified",
    "lite": "princeton-nlp/SWE-bench_Lite",
    "full": "princeton-nlp/SWE-bench",
}

# Jinja helper: the resolved run config pushed to XCom by prepare_run.
CFG = "ti.xcom_pull(task_ids='prepare_run')"


def _docker_task(task_id: str, bash: str, *, docker_sock: bool = False,
                 timeout_hours: float = 2, retries: int = 1) -> DockerOperator:
    """A DockerOperator running `bash -c <script>` in the agent-eval image."""
    mounts = [Mount(source=f"{HOST_PROJECT_DIR}/runs", target=CONTAINER_RUNS, type="bind")]
    if docker_sock:
        mounts.append(Mount(source="/var/run/docker.sock",
                            target="/var/run/docker.sock", type="bind"))
    return DockerOperator(
        task_id=task_id,
        image=AGENT_IMAGE,
        command=["bash", "-c", bash],
        environment={
            "NEBIUS_API_KEY": os.environ.get("NEBIUS_API_KEY", ""),
            "MSWEA_COST_TRACKING": "ignore_errors",
            "MLFLOW_TRACKING_URI": os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
            "MLFLOW_EXPERIMENT": os.environ.get("MLFLOW_EXPERIMENT", "swe-bench-agent-eval"),
            "S3_ENDPOINT_URL": os.environ.get("S3_ENDPOINT_URL", "http://minio:9000"),
            "S3_BUCKET": os.environ.get("S3_BUCKET", ""),
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        },
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        auto_remove="success",
        retries=retries,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=timeout_hours),
    )


@dag(
    dag_id="evaluate_agent_docker",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["swe-bench", "mini-swe-agent", "docker"],
    params={
        "split": Param("test", type="string", description="SWE-bench split (test/dev)"),
        "subset": Param("verified", type="string", enum=sorted(DATASET_BY_SUBSET),
                        description="SWE-bench subset"),
        "workers": Param(4, type="integer", minimum=1,
                         description="Parallel workers for agent and evaluation"),
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
def evaluate_agent_docker():

    @task
    def prepare_run(**context) -> dict:
        """Resolve params into an immutable run config and create runs/<run-id>/."""
        p = context["params"]
        run_id = p["run_id"] or f"run-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
        run_dir = AIRFLOW_RUNS_ROOT / run_id
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
            "executor": "DockerOperator",
            "image": AGENT_IMAGE,
        }
        (run_dir / "config.json").write_text(json.dumps(config, indent=2))
        print(f"Prepared {run_dir}")
        return config

    # Jinja renders the resolved config from XCom into the container command.
    run_agent = _docker_task(
        "run_agent",
        "set -o pipefail; "
        f"{{% set cfg = {CFG} %}}"
        "mini-extra swebench"
        " --subset {{ cfg['subset'] }}"
        " --split {{ cfg['split'] }}"
        " --model {{ cfg['model'] }}"
        " --slice {{ cfg['task_slice'] }}"
        " --workers {{ cfg['workers'] }}"
        f" -o {CONTAINER_RUNS}/{{{{ cfg['run_id'] }}}}/run-agent"
        "{% if cfg['cost_limit'] is not none %}"
        " -c swebench.yaml -c agent.cost_limit={{ cfg['cost_limit'] }}"
        "{% endif %}"
        f" 2>&1 | tee {CONTAINER_RUNS}/{{{{ cfg['run_id'] }}}}/run-agent/agent-stdout.log; "
        f"test -f {CONTAINER_RUNS}/{{{{ cfg['run_id'] }}}}/run-agent/preds.json",
        docker_sock=True,  # mini-swe-agent sandboxes each instance in a docker container
    )

    run_eval = _docker_task(
        "run_eval",
        "set -o pipefail; "
        f"{{% set cfg = {CFG} %}}"
        f"cd {CONTAINER_RUNS}/{{{{ cfg['run_id'] }}}}/run-eval && "
        "python -m swebench.harness.run_evaluation"
        " --dataset_name {{ cfg['dataset_name'] }}"
        " --predictions_path ../run-agent/preds.json"
        " --max_workers {{ cfg['workers'] }}"
        " --run_id {{ cfg['run_id'] }}"
        " 2>&1 | tee eval-stdout.log; "
        "ls *.{{ cfg['run_id'] }}.json",
        docker_sock=True,  # the harness spawns per-instance test containers
    )

    @task
    def summarize(config: dict) -> dict:
        """Parse the harness report into metrics.json and write manifest.json."""
        run_dir = AIRFLOW_RUNS_ROOT / config["run_id"]
        eval_dir = run_dir / "run-eval"
        reports = sorted(eval_dir.glob(f"*.{config['run_id']}.json"))
        if not reports:
            raise FileNotFoundError(f"No evaluation report in {eval_dir}")
        report = json.loads(reports[0].read_text())

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
            "eval_report": str(reports[0].relative_to(run_dir)),
            "eval_logs": f"run-eval/logs/run_evaluation/{config['run_id']}/",
            "metrics": "metrics.json",
            "artifact_uri": None,   # set by upload_artifacts
            "mlflow_run_id": None,  # set by log_mlflow
        }
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
        print(f"Metrics: {json.dumps(metrics)}")
        return metrics

    upload_artifacts = _docker_task(
        "upload_artifacts",
        f"{{% set cfg = {CFG} %}}"
        "{% if params.upload %}"
        "python -m pipeline.upload_artifacts --run-dir runs/{{ cfg['run_id'] }}"
        "{% else %}"
        "echo 'upload=false — skipping object-storage upload'"
        "{% endif %}",
        timeout_hours=0.25,
        retries=2,
    )

    log_mlflow = _docker_task(
        "log_mlflow",
        f"{{% set cfg = {CFG} %}}"
        "python -m pipeline.log_mlflow --run-dir runs/{{ cfg['run_id'] }}",
        timeout_hours=0.25,
        retries=2,
    )

    config = prepare_run()
    metrics = summarize(config)
    config >> run_agent >> run_eval >> metrics >> upload_artifacts >> log_mlflow


evaluate_agent_docker()
