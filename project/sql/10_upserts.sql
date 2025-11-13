INSERT INTO clean_ventas (
    fecha,
    id_cliente,
    id_producto,
    unidades,
    precio_unitario,
    importe,
    _ingest_ts
)
VALUES (
    :fecha,
    :idc,
    :idp,
    :u,
    :p,
    (:u * :p),
    :ts
)
ON CONFLICT (fecha, id_cliente, id_producto)
DO UPDATE SET
    unidades = excluded.unidades,
    precio_unitario = excluded.precio_unitario,
    importe = excluded.importe,
    _ingest_ts = excluded._ingest_ts;
