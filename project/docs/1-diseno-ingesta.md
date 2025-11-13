## 1. Ingestión

- **Tipo de ingesta:** Batch simple desde `data/drops/*.csv` y `*.ndjson`.

- **Trazabilidad:** Se registró `_ingest_ts`, `_source_file` y `_batch_id` para cada fila.

- **Idempotencia:** Las filas se agregan al dataset de manera que re-procesar el mismo archivo no genera duplicados, gracias a la deduplicación posterior.

- **Persistencia:** Se guardaron tres capas de datos:
  - Raw en **Parquet** y **CSV** (`raw_ventas`).
  - Clean en **Parquet** y **CSV** (`clean_ventas`).
  - Filas inválidas en **CSV** de cuarentena (`ventas_invalidas.csv`).