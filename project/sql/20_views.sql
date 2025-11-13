-- ================================================================
-- VISTAS DE ANÁLISIS Y REPORTING
-- ================================================================

-- Ventas agregadas por día
CREATE VIEW IF NOT EXISTS ventas_diarias AS
SELECT
    fecha,
    COUNT(*) AS transacciones,
    SUM(importe) AS ingresos,
    AVG(importe) AS ticket_medio
FROM clean_ventas
GROUP BY fecha
ORDER BY fecha;

-- Top productos por importe total
CREATE VIEW IF NOT EXISTS top_productos AS
SELECT
    id_producto,
    SUM(importe) AS ingresos,
    COUNT(*) AS transacciones,
    ROUND(100.0 * SUM(importe) / (SELECT SUM(importe) FROM clean_ventas), 2) AS pct_total
FROM clean_ventas
GROUP BY id_producto
ORDER BY ingresos DESC;
