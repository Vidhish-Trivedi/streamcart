FROM python:3.12-slim-bookworm
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml /app/pyproject.toml
COPY streamcart /app/streamcart
COPY producers /app/producers
COPY scripts /app/scripts
COPY schemas /app/schemas
COPY README.md /app/README.md
RUN pip install --no-cache-dir .
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "producers.generator"]
