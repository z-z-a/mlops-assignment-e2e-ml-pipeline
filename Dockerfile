FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    docker.io \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /mlops-assignment

COPY pyproject.toml .
COPY uv.lock .

RUN uv sync --locked

ENV PATH="/mlops-assignment/.venv/bin:$PATH"

COPY scripts scripts/
COPY pipeline pipeline/

# Optional but useful if your script lacks executable bit or shebang issues:
RUN chmod +x scripts/*.sh
