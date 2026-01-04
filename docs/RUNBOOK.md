# Runbook

## 1) Start services

```bash
docker compose up -d
```

## 2) NiFi

- Import the flow JSON from `nifi/flows/`.
- Ensure the Kafka broker matches the docker-compose network (commonly `kafka:29092`).
- Ensure any `GetFile` processors point to the correct container-mounted path.

## 3) ClickHouse

- Initialize schema via `config/clickhouse/init.sql` (done automatically if wired in docker-compose).

## 4) Start the Kafka  ClickHouse consumer

```bash
python scripts/kafka_to_clickhouse.py
```

Environment variables are read from `.env` (see `.env.example`).

## 5) Power BI

- Connect to ClickHouse.
- Recommended: filter visuals by an `ingestion_timestamp` window to avoid mixing older and newer generator logic.
