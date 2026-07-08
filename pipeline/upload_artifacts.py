"""Upload a runs/<run-id>/ folder to S3-compatible object storage (MinIO or cloud S3).

Usage (from the project root, inside the project venv):

    uv run python -m pipeline.upload_artifacts --run-dir runs/<run-id>

Environment:
    S3_BUCKET          required, e.g. mlops-agent-eval-runs
    S3_ENDPOINT_URL    optional, e.g. http://localhost:9000 for local MinIO
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY   credentials (MinIO root user/password)

Uploads every file under the run dir to s3://$S3_BUCKET/runs/<run-id>/...,
records the URI in manifest.json (artifact_uri), and prints the URI.
"""

import argparse
import json
import os
import sys
from pathlib import Path

import boto3


def upload_run_dir(run_dir: Path) -> str:
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        sys.exit(
            "S3_BUCKET is not set. Start local MinIO with `bash scripts/start-minio.sh` "
            "and set S3_BUCKET / S3_ENDPOINT_URL / AWS credentials in .env "
            "(see .env.example), or trigger the DAG with upload=false."
        )

    s3 = boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None)
    prefix = f"runs/{run_dir.name}"

    files = sorted(p for p in run_dir.rglob("*") if p.is_file())
    if not files:
        sys.exit(f"Nothing to upload: {run_dir} contains no files")

    total_bytes = 0
    for path in files:
        key = f"{prefix}/{path.relative_to(run_dir)}"
        s3.upload_file(str(path), bucket, key)
        total_bytes += path.stat().st_size

    uri = f"s3://{bucket}/{prefix}/"

    # Record the remote location so the manifest points at the durable copy.
    manifest_path = run_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        manifest["artifact_uri"] = uri
        manifest_path.write_text(json.dumps(manifest, indent=2))
        # Re-upload the updated manifest so the remote copy is self-describing too.
        s3.upload_file(str(manifest_path), bucket, f"{prefix}/manifest.json")

    print(f"Uploaded {len(files)} files ({total_bytes / 1e6:.1f} MB) to {uri}")
    return uri


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, help="Path to runs/<run-id>/")
    args = parser.parse_args()

    run_dir = Path(args.run_dir).resolve()
    if not run_dir.is_dir():
        sys.exit(f"Run dir not found: {run_dir}")
    upload_run_dir(run_dir)


if __name__ == "__main__":
    main()
