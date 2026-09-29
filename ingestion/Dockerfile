FROM python:3.12.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY pyproject.toml alembic.ini ./
COPY ingestion ./ingestion
COPY scripts ./scripts
RUN python -m pip install --no-cache-dir .

ENTRYPOINT ["python", "-m", "ingestion"]
