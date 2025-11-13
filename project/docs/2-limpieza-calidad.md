## 2. Limpieza y modelado

- **Tipado y coerción:** 
  - `fecha` → datetime, `unidades` → numérico, `precio_unitario` → float.

- **Rangos y dominios:**
  - `unidades` ≥ 0
  - `precio_unitario` ≥ 0
  - `id_cliente` e `id_producto` no vacíos.

- **Cuarentena:** Filas que no cumplen criterios se enviaron a `ventas_invalidas.csv` con motivo de error.

- **Deduplicación:** Clave natural `(fecha, id_cliente, id_producto)`; política “último gana” basada en `_ingest_ts`.

- **Derivación de negocio:** Se calculó `importe = unidades × precio_unitario` para clean.