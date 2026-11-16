FROM python:3.14.3 AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

RUN python -m venv .venv
COPY requirements.txt ./
RUN .venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.14.3-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY --from=builder /app/.venv .venv/
COPY . .

# Runtime writes live here (trades.db, demo_wallet.json, logs, TG state).
# On Fly this directory is a persistent volume.
RUN mkdir -p /app/data

# Fail the BUILD (not the machine at 3am) if any source file has a syntax error.
RUN .venv/bin/python -m compileall -q /app/src /app/main.py /app/dashboard.py

CMD ["/app/.venv/bin/python", "main.py", "worker"]
