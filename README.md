# Airline Delays Streaming (NiFi  Kafka  ClickHouse  Power BI)

This repository contains a pipeline to generate and stream airline delay data (NiFi  Kafka), load it into ClickHouse, and visualize it in Power BI.

Note: each streamed JSON record represents an aggregated observation (multiple flights), not a single flight. Fields like `arr_flights`, `arr_del15`, and the cause count fields (`*_ct`) are therefore expected to be larger than small per-flight thresholds.

**Project structure**
- `nifi/flows/`: NiFi flow exports to import in the NiFi UI
- `data/`: datasets and templates (including `data/Nifi_Templates_1500.csv`)
- `scripts/`: Python utilities (Kafka  ClickHouse consumer, template generator)
- `config/clickhouse/init.sql`: ClickHouse schema initialization
- `docs/`: documentation and runbooks

**Quick start**

1) Start the stack

```bash
docker compose up -d
```

2) (Optional) Create a local Python env

```bash
make venv
make install
```

3) Generate/refresh templates

```bash
make generate-templates
```

4) Import the NiFi flow

- Import `nifi/flows/streaming_flow.json` in the NiFi UI.
- Kafka brokers in the flow should match Docker Compose (typically `kafka:29092`).

5) Run the Kafka  ClickHouse consumer

```bash
make consumer
```

6) Real-time visualization (Python / Streamlit)

This dashboard combines:
- ClickHouse (aggregations + latest ingested records)
- Live Kafka messages consumed directly by the UI (buffered in-memory)

Run locally:

```bash
make streamlit
```

Run inside the stack:

```bash
docker compose up -d streamlit
```

Then open http://localhost:8501

**Docs**
- docs/ARCHITECTURE.md
- docs/RUNBOOK.md
