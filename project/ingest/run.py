from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

# ── Paths ──
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "drops"
OUTPUT = ROOT / "output"
PARQUET_DIR = OUTPUT / "parquet"
CSV_DIR = OUTPUT / "csv"
QUALITY_DIR = OUTPUT / "quality"

# Crear carpetas si no existen
for d in [PARQUET_DIR, CSV_DIR, QUALITY_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── 1) Ingesta ──
files = sorted(DATA.glob("*.csv")) + sorted(DATA.glob("*.ndjson")) + sorted(DATA.glob("*.jsonl"))
raw_dfs = []

for f in files:
    if f.suffix.lower() == ".csv":
        df = pd.read_csv(f, dtype=str)
    else:  # ndjson/jsonl
        df = pd.read_json(f, lines=True, dtype=str)
    df["_source_file"] = f.name
    df["_ingest_ts"] = datetime.now(timezone.utc).isoformat()
    raw_dfs.append(df)

cols = ["fecha","id_cliente","id_producto","unidades","precio_unitario","_source_file","_ingest_ts"]
raw_df = pd.concat(raw_dfs, ignore_index=True) if raw_dfs else pd.DataFrame(columns=cols)

# ── 2) Limpieza y modelado ──
def to_float_money(x):
    try:
        return float(str(x).replace(",", "."))
    except:
        return None

df = raw_df.copy()
for c in ["fecha","id_cliente","id_producto","unidades","precio_unitario"]:
    if c not in df.columns:
        df[c] = None

df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
df["unidades"] = pd.to_numeric(df["unidades"], errors="coerce")
df["precio_unitario"] = df["precio_unitario"].apply(to_float_money)

valid = (
    df["fecha"].notna()
    & df["unidades"].notna() & (df["unidades"] >= 0)
    & df["precio_unitario"].notna() & (df["precio_unitario"] >= 0)
    & df["id_cliente"].notna() & (df["id_cliente"] != "")
    & df["id_producto"].notna() & (df["id_producto"] != "")
)

quarantine = df.loc[~valid].copy()
clean = df.loc[valid].copy()

# Deduplicación "último gana" por _ingest_ts
if not clean.empty:
    clean = clean.sort_values("_ingest_ts").drop_duplicates(
        subset=["fecha","id_cliente","id_producto"], keep="last"
    )
    clean["importe"] = clean["unidades"] * clean["precio_unitario"]

# ── 3) Persistencia ──

# Raw
RAW_PARQUET = PARQUET_DIR / "raw_ventas.parquet"
RAW_CSV = CSV_DIR / "raw_ventas.csv"
raw_df.to_parquet(RAW_PARQUET, index=False)
raw_df.to_csv(RAW_CSV, index=False)
print("✅ Raw Parquet:", RAW_PARQUET)
print("✅ Raw CSV:", RAW_CSV)

# Clean
CLEAN_PARQUET = PARQUET_DIR / "clean_ventas.parquet"
CLEAN_CSV = CSV_DIR / "clean_ventas.csv"
if not clean.empty:
    clean.to_parquet(CLEAN_PARQUET, index=False)
    clean.to_csv(CLEAN_CSV, index=False)
print("✅ Clean Parquet:", CLEAN_PARQUET)
print("✅ Clean CSV:", CLEAN_CSV)

# Quarantine
QUAR_PARQUET = QUALITY_DIR / "ventas_invalidas.parquet"
QUAR_CSV = CSV_DIR / "ventas_invalidas.csv"
if not quarantine.empty:
    quarantine.to_parquet(QUAR_PARQUET, index=False)
    quarantine.to_csv(QUAR_CSV, index=False)
print("✅ Quarantine Parquet:", QUAR_PARQUET)
print("✅ Quarantine CSV:", QUAR_CSV)

# ── 4) Reporte Markdown ──
if not clean.empty:
    ingresos = float(clean["importe"].sum())
    trans = int(len(clean))
    ticket = float(ingresos / trans) if trans > 0 else 0.0

    top = (clean.groupby("id_producto", as_index=False)
               .agg(importe=("importe","sum"))
               .sort_values("importe", ascending=False))
    total_imp = top["importe"].sum() or 1.0
    top["pct"] = (100*top["importe"]/total_imp).round(0).astype(int).astype(str) + "%"

    by_day = (clean.groupby("fecha", as_index=False)
                     .agg(importe_total=("importe","sum"),
                          transacciones=("importe","count")))

    periodo_ini = str(clean["fecha"].min())
    periodo_fin = str(clean["fecha"].max())
    producto_lider = top.iloc[0]["id_producto"] if not top.empty else "—"
else:
    ingresos = 0.0; ticket = 0.0; trans = 0
    top = pd.DataFrame(columns=["id_producto","importe","pct"])
    by_day = pd.DataFrame(columns=["fecha","importe_total","transacciones"])
    periodo_ini = "—"; periodo_fin = "—"; producto_lider = "—"

report_md = (
    "# Reporte UT1 · Ventas\n"
    f"**Periodo:** {periodo_ini} a {periodo_fin} · **Fuente:** clean_ventas.parquet · **Generado:** {datetime.now(timezone.utc).isoformat()}\n\n"
    "## 1. Titular\n"
    f"Ingresos totales {ingresos:.2f} €; producto líder: {producto_lider}.\n\n"
    "## 2. KPIs\n"
    f"- **Ingresos netos:** {ingresos:.2f} €\n"
    f"- **Ticket medio:** {ticket:.2f} €\n"
    f"- **Transacciones:** {trans}\n\n"
    "## 3. Top productos\n"
    f"{(top.to_markdown(index=False) if not top.empty else '_(sin datos)_')}\n\n"
    "## 4. Resumen por día\n"
    f"{(by_day.to_markdown(index=False) if not by_day.empty else '_(sin datos)_')}\n\n"
    "## 5. Calidad y cobertura\n"
    f"- Filas bronce: {len(df)} · Plata: {len(clean)} · Cuarentena: {len(quarantine)}\n\n"
    "## 6. Persistencia\n"
    f"- Parquet raw: {RAW_PARQUET}\n"
    f"- CSV raw: {RAW_CSV}\n"
    f"- Parquet clean: {CLEAN_PARQUET}\n"
    f"- CSV clean: {CLEAN_CSV}\n"
    f"- Parquet cuarentena: {QUAR_PARQUET}\n"
    f"- CSV cuarentena: {QUAR_CSV}\n\n"
    "## 7. Conclusiones\n"
    "- Reponer producto líder según demanda.\n"
    "- Revisar filas en cuarentena (rangos/tipos).\n"
    "- Valorar particionado por fecha para crecer.\n"
)

(REPORT_FILE := OUTPUT / "reporte.md").write_text(report_md, encoding="utf-8")
print("✅ Generado reporte Markdown:", REPORT_FILE)
