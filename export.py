import os
import json
import math
import logging
import yaml
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

from sql_types import SQL_TYPE_MAP

logger = logging.getLogger(__name__)


def setup_logging():
    """Configure logging to console and file."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("export.log"),
        ],
    )


def load_config():
    """Load environment variables and return connection/config settings."""
    load_dotenv()
    return {
        "server": os.getenv("DB_SERVER"),
        "database": os.getenv("DB_DATABASE"),
        "driver": os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server"),
        "output_dir": os.getenv("OUTPUT_DIR", "output"),
    }


def load_table_rules(path="tables.yaml"):
    """Load include/exclude table rules from a YAML file."""
    with open(path, "r") as f:
        table_config = yaml.safe_load(f)

    include_tables = set(table_config.get("include", []) or [])
    exclude_tables = set(table_config.get("exclude", []) or [])
    return include_tables, exclude_tables


def connect(config):
    """Connect to SQL Server using Windows Authentication."""
    conn_str = (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"Trusted_Connection=yes;"
    )
    logger.info(f"Connecting to {config['server']}/{config['database']}...")
    conn = pyodbc.connect(conn_str)
    logger.info("Connected successfully.")
    return conn


def get_all_tables(cursor):
    """Return a list of (schema, table) tuples for all base tables."""
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
    """)
    return [(row.TABLE_SCHEMA, row.TABLE_NAME) for row in cursor.fetchall()]


def get_column_types(cursor):
    """Return a dict mapping (schema, table) -> {column: data_type}."""
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
    """)
    column_types = {}
    for row in cursor.fetchall():
        key = (row.TABLE_SCHEMA, row.TABLE_NAME)
        column_types.setdefault(key, {})[row.COLUMN_NAME] = row.DATA_TYPE.lower()
    return column_types


def warn_missing_tables(include_tables, all_tables):
    """Warn about included tables not found in INFORMATION_SCHEMA."""
    all_tables_set = {f"{s}.{t}" for s, t in all_tables} | {t for _, t in all_tables}
    for entry in include_tables:
        if entry not in all_tables_set:
            logger.warning(
                f"Table '{entry}' listed in tables.yaml (include) was not found "
                f"in INFORMATION_SCHEMA. Skipping."
            )


def should_export(schema, table, include_tables, exclude_tables):
    """Decide whether a table should be exported based on include/exclude rules."""
    full_name = f"{schema}.{table}"
    short_name = table

    if full_name in exclude_tables or short_name in exclude_tables:
        return False

    if not include_tables:
        return True

    return full_name in include_tables or short_name in include_tables


def filter_tables(all_tables, include_tables, exclude_tables):
    """Return the list of tables that pass the include/exclude rules."""
    return [
        (schema, table)
        for schema, table in all_tables
        if should_export(schema, table, include_tables, exclude_tables)
    ]


def cast_dataframe(df, schema, table, column_types):
    """Cast each column to its proper type based on INFORMATION_SCHEMA."""
    col_map = column_types.get((schema, table), {})
    for col in df.columns:
        sql_type = col_map.get(col, "")
        target = SQL_TYPE_MAP.get(sql_type)
        if not target:
            continue
        try:
            if target.startswith("datetime"):
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(target)
        except (ValueError, TypeError) as e:
            logger.warning(f"  Could not cast column '{col}' ({sql_type}) to {target}: {e}")
    return df


def sanitize_value(v):
    """Convert non-JSON-serialisable / missing values consistently to None."""
    if v is None or v is pd.NA or v is pd.NaT:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    return v


def export_table(conn, schema, table, column_types, output_dir):
    """Load a single table, cast types, sanitise values, and write JSON."""
    logger.info(f"Exporting {schema}.{table}...")
    df = pd.read_sql(f"SELECT * FROM [{schema}].[{table}]", conn)

    df = cast_dataframe(df, schema, table, column_types)

    records = [
        {col: sanitize_value(val) for col, val in row.items()}
        for row in df.to_dict(orient="records")
    ]

    out_file = Path(output_dir) / f"{schema}__{table}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    logger.info(f"  -> {out_file} ({len(records)} rows)")


def main():
    setup_logging()
    config = load_config()
    include_tables, exclude_tables = load_table_rules()

    conn = connect(config)
    cursor = conn.cursor()

    all_tables = get_all_tables(cursor)
    column_types = get_column_types(cursor)

    warn_missing_tables(include_tables, all_tables)

    tables_to_export = filter_tables(all_tables, include_tables, exclude_tables)
    logger.info(f"Tables to export: {len(tables_to_export)}")

    Path(config["output_dir"]).mkdir(parents=True, exist_ok=True)

    for schema, table in tables_to_export:
        export_table(conn, schema, table, column_types, config["output_dir"])

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
