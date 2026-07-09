# REPORT — Coding-Agent Evaluation Pipeline

Airflow pipeline that runs [mini-swe-agent](https://github.com/SWE-agent/mini-swe-agent) on a
configurable slice of SWE-bench, evaluates the produced patches with the official SWE-bench
harness, uploads the full run folder to object storage (MinIO, S3 API), and logs the experiment
to MLflow.

## Architecture

```
prepare_run ──▶ run_agent ──▶ run_eval ──▶ summarize ──▶ upload_artifacts ──▶ log_mlflow
```

| Task | What it does |
|---|---|
| `prepare_run` | Resolves Airflow params into an immutable `runs/<run-id>/config.json`; creates the run dir |
| `run_agent` | `mini-extra swebench` batch → trajectories + `preds.json` into `run-agent/` |
| `run_eval` | `swebench.harness.run_evaluation` (Docker-based) executed with `cwd=run-eval/`, so its report and logs land inside the run folder |
| `summarize` | Parses the harness report → `metrics.json`; writes `manifest.json` |
| `upload_artifacts` | Syncs the whole `runs/<run-id>/` tree to `s3://$S3_BUCKET/runs/<run-id>/` (MinIO), records `artifact_uri` in the manifest |
| `log_mlflow` | Logs params, metrics, tags (incl. `artifact_uri`), and key artifacts to MLflow; records `mlflow_run_id` in the manifest |

Everything is joined by a single **`run_id`**: it is the run folder name, the SWE-bench harness
`--run_id`, the S3 prefix, and the MLflow run name — one key reconstructs the whole experiment
across all systems.

The pipeline exists in two variants that produce identical run folders:

| DAG | Execution | Use |
|---|---|---|
| [`dags/evaluate_agent.py`](dags/evaluate_agent.py) | `uv run` subprocesses on the VM | easy mode / standalone Airflow |
| [`dags/evaluate_agent_docker.py`](dags/evaluate_agent_docker.py) | **DockerOperator** — each heavy step runs in an isolated container built from the project [`Dockerfile`](Dockerfile) (`agent-eval:latest`) | production mode / Docker Compose |

In the Docker variant, only the stdlib-only tasks (`prepare_run`, `summarize`) run inside
Airflow; `run_agent`, `run_eval`, `upload_artifacts`, and `log_mlflow` are sibling containers on
the host daemon. `run_agent` and `run_eval` mount `/var/run/docker.sock` because both
mini-swe-agent (instance sandboxes) and the SWE-bench harness (test containers) spawn containers
themselves. Each run's `config.json` records which executor produced it
(`"executor": "DockerOperator", "image": "agent-eval:latest"`).

Implementation notes:

- Helper modules [`pipeline/upload_artifacts.py`](pipeline/upload_artifacts.py) and
  [`pipeline/log_mlflow.py`](pipeline/log_mlflow.py) run via `uv run` subprocesses (standalone) or
  inside the project image (docker), so heavy deps (boto3, mlflow, swebench) never live in
  Airflow's environment.
- No hard-coded experiment values: everything flows from Airflow params through `config.json`.
- Retries/timeouts: agent and eval have `retries=1, execution_timeout=2h`; upload and MLflow
  logging have `retries=2`. Both heavy steps are idempotent per `run_id`.
- `cost_limit` is passed as a mini-swe-agent config override
  (`-c swebench.yaml -c agent.cost_limit=<v>`), since the batch CLI has no `--cost-limit` flag.

## Deployment: Docker Compose (production mode)

The whole stack runs from [`docker-compose.yaml`](docker-compose.yaml) on one Nebius VM
(8 vCPU / 32 GB / 200 GB disk): Airflow 3.1 (LocalExecutor — api-server, scheduler,
dag-processor, triggerer, init, backed by Postgres), MLflow, and MinIO:

```bash
cp .env.example .env          # fill NEBIUS_API_KEY, AIRFLOW_UID=$(id -u),
                              # DOCKER_GID=$(getent group docker | cut -d: -f3),
                              # HOST_PROJECT_DIR=<absolute path of this repo on the VM>
docker build -t agent-eval:latest .
mkdir -p logs plugins config runs
docker compose up airflow-init   # DB migration + UI user (airflow/airflow); must exit 0
docker compose up -d
```

Deployed stack (`docker compose ps`):

```text
NAME                            IMAGE                           STATUS                 PORTS
mlops-airflow-apiserver-1       apache/airflow:3.1.0            Up (healthy)           0.0.0.0:8080->8080
mlops-airflow-dag-processor-1   apache/airflow:3.1.0            Up
mlops-airflow-scheduler-1       apache/airflow:3.1.0            Up (healthy)
mlops-airflow-triggerer-1       apache/airflow:3.1.0            Up
mlops-minio-1                   quay.io/minio/minio:latest      Up (healthy)           0.0.0.0:9000-9001->9000-9001
mlops-mlflow-1                  ghcr.io/mlflow/mlflow:v3.14.0   Up                     0.0.0.0:5000->5000
mlops-postgres-1                postgres:16                     Up (healthy)
```

UIs (SSH-forwarded): Airflow `:8080` (login `airflow`/`airflow`), MLflow `:5000`,
MinIO console `:9001`. Configuration lives in `.env` (see [`.env.example`](.env.example)).
`HOST_PROJECT_DIR` matters because DockerOperator containers are *siblings* on the host daemon —
their bind mounts must be host-absolute paths.

### Alternative: standalone mode (easy mode)

The subprocess variant runs without compose (uses the same `.env`; the DAG loads it itself):

```bash
uv sync
bash scripts/start-minio.sh                              # MinIO :9000/:9001 + bucket
tmux new -s mlflow  -d 'bash scripts/start-mlflow.sh'    # MLflow :5000
tmux new -s airflow -d 'bash run-airflow-standalone.sh'  # Airflow :8080 (admin/admin)
```

The two modes share storage (`./minio-data`, `./mlflow-data/mlflow.db`), so run history carries
over — but they collide on ports 8080/5000/9000, so run one at a time.

## How to trigger a run

Airflow UI (`http://localhost:8080`, via `ssh -L 8080:localhost:8080 <vm>`) →
DAG **`evaluate_agent_docker`** (or `evaluate_agent` in standalone mode) → *Trigger w/ config*:

```json
{
  "split": "test",
  "subset": "verified",
  "workers": 4,
  "model": "nebius/moonshotai/Kimi-K2.6",
  "task_slice": "0:2",
  "cost_limit": 0,
  "upload": true
}
```

Params: `split`, `subset`, `workers` (required); `model`, `task_slice`, `run_id`, `cost_limit`,
`upload` (optional). Leaving `run_id` empty auto-generates `run-<UTC timestamp>`.

## Artifact layout

Each run produces a self-contained folder, mirrored to object storage:

```
runs/<run-id>/                                   ── also at s3://mlops-agent-eval-runs/runs/<run-id>/
  config.json                # resolved params = the experiment definition
  run-agent/
    preds.json               # instance_id → model patch
    <instance_id>/<instance_id>.traj.json        # full agent trajectories
    minisweagent.log, agent-stdout.log
  run-eval/
    <model>.<run-id>.json    # harness report (resolved/unresolved/error ids)
    logs/run_evaluation/<run-id>/<model>/<instance_id>/   # patch.diff, test_output.txt, report.json
    eval-stdout.log
  metrics.json               # counts + resolved_rate
  manifest.json              # pointers + artifact_uri + mlflow_run_id
```

`manifest.json` is the entry point: it references every important file and records where the
durable copy lives (`artifact_uri`) and which MLflow run tracks it (`mlflow_run_id`).

## Completed evaluation: `run-20260708-193808`

Triggered from the Airflow UI with `subset=verified`, `split=test`, `task_slice=0:2`,
`workers=4`, `model=nebius/moonshotai/Kimi-K2.6`, `cost_limit=0`.

**Result: 1 / 2 resolved (`resolved_rate = 0.5`)**

| Instance | Outcome |
|---|---|
| `astropy__astropy-12907` | ✅ resolved — patch passed the unit tests |
| `astropy__astropy-13033` | ❌ unresolved — patch produced, but tests failed |

No errors, no empty patches.

### Failure analysis: `astropy__astropy-13033`

The agent's fix was **logically correct but failed on exact message formatting**. The run
artifacts tell the whole story:

- `report.json` (tests_status): all **20 PASS_TO_PASS regression tests pass**, and exactly one
  FAIL_TO_PASS test fails — `test_sampled.py::test_required_columns`. The fix works; no
  regressions were introduced.
- `test_output.txt` shows the failing assertion is a string near-miss. The hidden test expects
  the required columns rendered as a Python list repr, while the agent chose a comma-joined
  format:

  ```
  expected:  "... expected ['time', 'a'] as the first columns but found ['time', 'b']"
  agent:     "... expected 'time', 'a' as the first columns but found 'time', 'b'"
  ```

- `patch.diff` confirms the agent correctly identified the buggy exception in
  `astropy/timeseries/core.py` and fixed the misleading behavior described in the issue.

Root cause: SWE-bench applies the (hidden) updated test *after* the agent produces its patch, so
the agent had to guess which message format the astropy maintainers chose — it guessed wrong by
four square brackets. This is an inherent hard case for exact-string assertions, not a pipeline
or harness defect.

Follow-up experiments this pipeline supports directly (each is one DAG trigger, compared in
MLflow): resampling the same config (nonzero temperature → pass@k), swapping the `model` param,
lowering temperature via a config override (`-c model.model_kwargs.temperature=...`), or a
prompt-template tweak instructing the agent to match existing repo message conventions when
editing user-facing strings. We deliberately did **not** hand-edit the prediction to flip the
result: traceable 1/2 beats untraceable 2/2.

Full details:

- Metrics: [`runs/run-20260708-193808/metrics.json`](runs/run-20260708-193808/metrics.json)
- Why 13033 failed: `runs/run-20260708-193808/run-eval/logs/run_evaluation/run-20260708-193808/nebius__moonshotai__Kimi-K2.6/astropy__astropy-13033/`
  (`patch.diff`, `test_output.txt`, `report.json`)
- Agent reasoning: `runs/run-20260708-193808/run-agent/astropy__astropy-13033/…traj.json`
- Object storage: `s3://mlops-agent-eval-runs/runs/run-20260708-193808/` (MinIO)
- MLflow run: `382e6d8cf7ba4c58980e88f45891d885` in experiment `swe-bench-agent-eval`
  (tracking server `http://localhost:5000`)

## Completed evaluation (DockerOperator): `run-20260709-115838`

The same experiment executed through the **`evaluate_agent_docker`** DAG on the Compose stack —
every heavy step in an isolated `agent-eval:latest` container:

- **Result: 1 / 2 resolved (`resolved_rate = 0.5`)** — identical outcome to the standalone run
  (`astropy__astropy-12907` ✅, `astropy__astropy-13033` ❌ on the same message-format
  assertion), a nice reproducibility check across execution environments.
- `config.json` records the provenance: `"executor": "DockerOperator", "image": "agent-eval:latest"`.
- Object storage: `s3://mlops-agent-eval-runs/runs/run-20260709-115838/` (MinIO)
- MLflow run: `38775a3ef44c4d3084d537689b7b52f2` — comparable side-by-side with the standalone
  runs in the same experiment.

All runs, comparable in the MLflow UI:

| run_id | DAG variant | task_slice | resolved |
|---|---|---|---|
| `run-20260708-180037` | subprocess | `0:3` | 2/3 |
| `run-20260708-182650` | subprocess | `0:3` | 2/3 |
| `run-20260708-183831` | subprocess | `0:2` | 1/2 |
| `run-20260708-193808` | subprocess | `0:2` | 1/2 |
| `run-20260709-115838` | DockerOperator | `0:2` | 1/2 |

(`run-20260708-174849` and `run-20260709-095226/100409/114033` are partial runs kept from
debugging the pipeline — see the engineering notes below; their failure modes are part of the
run history on purpose.)

## How to rerun / reproduce

**Rerun the same experiment (new run id):** trigger the DAG with the values from the run's
`config.json` — e.g. the JSON block above reproduces `run-20260708-193808`.

**Re-execute under the same run id:** pass `"run_id": "run-20260708-193808"` in the trigger
config. Tasks are idempotent per run id: the agent skips already-finished instances, the harness
caches built images, and upload/MLflow logging simply overwrite/append.

**Reconstruct a run from storage alone:** download the folder and read `manifest.json`:

```bash
docker run --rm --network host --entrypoint sh quay.io/minio/mc -c \
  "mc alias set local http://localhost:9000 $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY && \
   mc cp -r local/mlops-agent-eval-runs/runs/run-20260708-193808 ./restored/"
```

## Evidence

| Screenshot | Shows |
|---|---|
| [`screenshots/airflow_dag.png`](screenshots/airflow_dag.png) | Completed `evaluate_agent` (standalone) DAG run in the Airflow UI |
| [`screenshots/airflow_dag_docker.png`](screenshots/airflow_dag_docker.png) | Completed `evaluate_agent_docker` (DockerOperator) DAG run on the Compose stack |
| [`screenshots/mlflow_runs.png`](screenshots/mlflow_runs.png), [`mlflow_runs2.png`](screenshots/mlflow_runs2.png) | MLflow experiment `swe-bench-agent-eval` with logged params/metrics per run |
| [`screenshots/mlflow_runs_docker.png`](screenshots/mlflow_runs_docker.png) | MLflow showing the DockerOperator run alongside standalone runs |
| [`screenshots/object_storage_artifacts.png`](screenshots/object_storage_artifacts.png) | MinIO console showing uploaded `runs/<run-id>/` artifacts |

## Engineering notes: what broke on the way to Phase 3 (and the fixes)

Kept deliberately — each of these is a production lesson, and the partial runs they produced are
in the run history:

1. **Runtime pip installs are fragile (hit twice).** `airflow-init` (running as root) refused
   `_PIP_ADDITIONAL_REQUIREMENTS` → blanked it for init. The MLflow container's startup
   `pip install boto3` failed on DNS → removed the runtime install entirely and kept MLflow
   artifacts on the local volume. Production fix for both: bake custom images instead of
   installing at container startup.
2. **DockerOperator siblings need the docker socket everywhere containers are spawned.**
   `run_agent` failed until it also mounted `/var/run/docker.sock` — mini-swe-agent sandboxes
   every instance in a container, not just the eval harness (`run-20260709-095226`).
3. **`.env` hygiene**: a missing trailing newline merged two variables into
   `S3_BUCKET=...AIRFLOW_UID=1001`, producing an invalid bucket name at upload time — and
   silently left Airflow running as the wrong UID (`run-20260709-100409`).
4. **YAML folded scalars (`>`)**: continuation lines indented deeper than the first keep their
   newlines — the MLflow `command:` silently became three shell commands
   (`sh: --backend-store-uri: not found`).
5. **Version skew between venv and container**: the standalone MLflow (venv, 3.14.0) migrated
   `mlflow.db` past what the `v2.16.2`/`v3.4.0` images understood
   (`alembic ... Can't locate revision`). Fix: pin `MLFLOW_IMAGE` to the venv version.
6. **MLflow 3.x DNS-rebinding protection** rejects non-localhost `Host` headers with 403 —
   compose-internal clients calling `http://mlflow:5000` need
   `MLFLOW_SERVER_ALLOWED_HOSTS` (which *replaces* the default list, so localhost must be
   re-added for the port-forwarded UI) (`run-20260709-114033`).

