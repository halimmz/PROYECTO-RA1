-- ================================================================
-- ESQUEMA DE BASE DE DATOS: VENTAS RETAIL MINI
-- ================================================================

-- Tabla RAW: bronce (datos sin limpiar)
CREATE TABLE IF NOT EXISTS raw_ventas(
  fecha TEXT,
  id_cliente TEXT,
  id_producto TEXT,
  unidades TEXT,
  precio_unitario TEXT,
  _ingest_ts TEXT,
  _source_file TEXT,
  _batch_id TEXT,
  causa_error TEXT DEFAULT NULL
);


-- Tabla CLEAN: plata (datos validados y limpios)
CREATE TABLE IF NOT EXISTS clean_ventas (
  fecha TEXT,
  id_cliente TEXT,
  id_producto TEXT,
  unidades REAL,
  precio_unitario REAL,
  importe REAL,           -- 🆕 campo calculado = unidades * precio_unitario
  _ingest_ts TEXT,
  PRIMARY KEY (fecha, id_cliente, id_producto)
);

-- Tabla QUARANTINE: calidad (filas rechazadas)
CREATE TABLE IF NOT EXISTS quarantine_ventas (
  fecha TEXT,
  id_cliente TEXT,
  id_producto TEXT,
  unidades TEXT,
  precio_unitario TEXT,
  _reason TEXT,
  _ingest_ts TEXT,
  _source_file TEXT,
  _batch_id TEXT
);
