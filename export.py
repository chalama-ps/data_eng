import os
import json
import logging
import yaml
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("export.log"),
    ],
)
logger = logging.getLogger(__name__)

# --- Config ---
SERVER     = os.getenv("DB_SERVER")
DATABASE   = os.getenv("DB_DATABASE")
DRIVER     = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")

# --- Load table rules ---
with open("tables.yaml", "r") as f:
    table_config = yaml.safe_load(f)

include_tables = set(table_config.get("include", []) or [])
exclude_tables = set(table_config.get("exclude", []) or [])

# --- Connect using Windows Authentication ---
conn_str = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"Trusted_Connection=yes;"
)
logger.info(f"Connecting to {SERVER}/{DATABASE}...")
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
logger.info("Connected successfully.")

# --- Get all tables from INFORMATION_SCHEMA ---
cursor.execute("""
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
""")
all_tables = [(row.TABLE_SCHEMA, row.TABLE_NAME) for row in cursor.fetchall()]

# Build a lookup set (both "schema.table" and "table" forms)
all_tables_set = {f"{s}.{t}" for s, t in all_tables} | {t for _, t in all_tables}

# --- Warn about included tables not found in INFORMATION_SCHEMA ---
for entry in include_tables:
    if entry not in all_tables_set:
        logger.warning(
            f"Table '{entry}' listed in tables.yaml (include) was not found "
            f"in INFORMATION_SCHEMA. Skipping."
        )

# --- Filter tables ---
def should_export(schema, table):
    full_name  = f"{schema}.{table}"
    short_name = table

    if full_name in exclude_tables or short_name in exclude_tables:
        return False

    if not include_tables:
        return True

    return full_name in include_tables or short_name in include_tables

tables_to_export = [
    (schema, table) for schema, table in all_tables
    if should_export(schema, table)
]

logger.info(f"Tables to export: {len(tables_to_export)}")

# --- Export ---
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

for schema, table in tables_to_export:
    logger.info(f"Exporting {schema}.{table}...")
    df = pd.read_sql(f"SELECT * FROM [{schema}].[{table}]", conn)
    records = df.to_dict(orient="records")

    out_file = Path(OUTPUT_DIR) / f"{schema}__{table}.json"
    with open(out_file, "w") as f:
        json.dump(records, f, indent=2, default=str)

    logger.info(f"  -> {out_file} ({len(records)} rows)")

conn.close()
logger.info("Done.")
