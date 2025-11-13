# Reporte UT1 · Ventas
**Periodo:** 2025-01-07 a 2025-01-11 · **Fuente:** clean_ventas.parquet · **Generado:** 2025-11-13T15:33:01.298688+00:00

## 1. Titular
Ingresos totales 481.44 €; producto líder: P10.

## 2. KPIs
- **Ingresos netos:** 481.44 €
- **Ticket medio:** 26.75 €
- **Transacciones:** 18

## 3. Top productos
| id_producto   |   importe | pct   |
|:--------------|----------:|:------|
| P10           |    137.5  | 29%   |
| P30           |     90    | 19%   |
| P50           |     80    | 17%   |
| P40           |     75    | 16%   |
| P70           |     59.94 | 12%   |
| P20           |     24    | 5%    |
| P77           |     15    | 3%    |

## 4. Resumen por día
| fecha      |   importe_total |   transacciones |
|:-----------|----------------:|----------------:|
| 2025-01-07 |          120    |               3 |
| 2025-01-08 |           72.44 |               3 |
| 2025-01-09 |          110.5  |               4 |
| 2025-01-10 |           66    |               3 |
| 2025-01-11 |          112.5  |               5 |

## 5. Calidad y cobertura
- Filas bronce: 25 · Plata: 18 · Cuarentena: 7

## 6. Persistencia
- Parquet raw: C:\Users\halim\Downloads\PROYECTO-RA1\BDA_Proyecto_UT1_RA1-main\project\output\parquet\raw_ventas.parquet
- CSV raw: C:\Users\halim\Downloads\PROYECTO-RA1\BDA_Proyecto_UT1_RA1-main\project\output\csv\raw_ventas.csv
- Parquet clean: C:\Users\halim\Downloads\PROYECTO-RA1\BDA_Proyecto_UT1_RA1-main\project\output\parquet\clean_ventas.parquet
- CSV clean: C:\Users\halim\Downloads\PROYECTO-RA1\BDA_Proyecto_UT1_RA1-main\project\output\csv\clean_ventas.csv
- Parquet cuarentena: C:\Users\halim\Downloads\PROYECTO-RA1\BDA_Proyecto_UT1_RA1-main\project\output\quality\ventas_invalidas.parquet
- CSV cuarentena: C:\Users\halim\Downloads\PROYECTO-RA1\BDA_Proyecto_UT1_RA1-main\project\output\csv\ventas_invalidas.csv

## 7. Conclusiones
- Reponer producto líder según demanda.
- Revisar filas en cuarentena (rangos/tipos).
- Valorar particionado por fecha para crecer.
