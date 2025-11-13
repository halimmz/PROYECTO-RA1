## 4. Lecciones aprendidas

- La separación en capas (raw → clean → cuarentena) facilita la trazabilidad y la calidad de datos.

- CSV es suficiente para un entorno sin SQL, pero Parquet ofrece mayor eficiencia y compatibilidad con herramientas de big data.

- La deduplicación y el cálculo de “último gana” garantizan idempotencia en pipelines batch.

- Mantener la información de errores en cuarentena permite auditar problemas de calidad de forma sencilla.

- El reporte en Markdown es ligero y reproducible; se puede exportar a PDF con Pandoc o VS Code.