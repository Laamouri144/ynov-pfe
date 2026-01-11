# Architecture

This project streams synthetic airline delay data using this pipeline:

NiFi  Kafka  ClickHouse  Power BI

**Core datasets**
- `Airline_Delay_Cause.csv`: historical dataset used as a realism reference and/or template source.
- `data/Nifi_Templates_1500.csv`: template rows used by NiFi to generate high-volume streaming records.

**Key KPIs we try to preserve**
- `delay_rate = arr_del15 / arr_flights`
- `avg_delay_min_per_flight = arr_delay / arr_flights`
- `avg_delay_min_per_delayed = arr_delay / arr_del15`
- `cancel_rate = arr_cancelled / arr_flights`
- `divert_rate = arr_diverted / arr_flights`
