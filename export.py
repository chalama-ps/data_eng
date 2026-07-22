"""Export SQL Server tables to JSON files.

Streams each selected table to its own JSON file using atomic writes so a
failure never leaves a corrupt/partial output behind.

Configuration is driven entirely by environment variables (optionally via a
`.env` file):
    DB_SERVER          (required) SQL Server host/instance.
    DB_DATABASE        (required) Database name.
    DB_SCHEMA          Optional schema filter. When set, only tables in this
                       schema are considered for export.
    DB_DRIVER          ODBC driver name (default: "ODBC Driver 17 for SQL Server").
    OUTPUT_DIR         Output directory for JSON files (default: "output").
    DB_LOGIN_TIMEOUT   Connection login timeout in seconds (default: 30).
    DB_USERNAME        SQL Server login. When set (with DB_PASSWORD), SQL
                       authentication is used; otherwise Windows/Trusted auth.
    DB_PASSWORD        Password for DB_USERNAME (never logged).
    DB_ENCRYPT         "yes"/"no" to force TLS encryption of the connection.
    DB_TRUST_SERVER_CERTIFICATE  "yes" to skip TLS cert validation (dev only).
    STRICT_ROW_COUNT   "yes" (default) to fail a table when the exported row
                       count does not match the source COUNT(*).
    DB_CONNECT_RETRIES Retries for transient connection errors (default: 3).
    DB_RETRY_BACKOFF   Seconds to wait between retries (default: 5).
    FETCH_BATCH_SIZE   Rows fetched from the cursor per round-trip (default: 1000).
    TABLES_CONFIG      Path to include/exclude YAML file (default: "tables.yaml").
    MANIFEST_DIR       Directory for the run manifest (default: "manifests").
                       Kept local; never uploaded to S3.
    LOG_DIR            Directory for log files (default: "logs").
    LOG_FILE           Log file name (default: "export.log"); a timestamp is
                       inserted and it is written under LOG_DIR.
    LOG_LEVEL          Logging level (default: "INFO").
    S3_ENABLED         "yes" to also upload each table's JSON file to S3
                       (default: "no"). Local files are always kept.
    S3_BUCKET          Target S3 bucket (required when S3_ENABLED=yes).
    S3_PREFIX          Optional key prefix within the bucket (default: "").
    DUMP_PERIOD        Optional path segment inserted after S3_PREFIX, e.g. a
                       period/run folder: s3://bucket/<prefix>/<period>/table.json.
    AWS_REGION         AWS region for the S3 client (falls back to
                       AWS_DEFAULT_REGION / the default chain).
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
                       Optional explicit credentials. When omitted, boto3's
                       standard credential chain is used (env, profile, IAM
                       role, SSO, etc.). Only table JSON files go to S3; logs
                       and the manifest always stay local.

Table rules (tables.yaml) may use fully-qualified "schema.table" names, or
bare "table" names which are qualified with DB_SCHEMA.

The source is assumed to be a static, restored database (no concurrent DML),
so no transaction isolation handling is required.

Data integrity guarantees:
    * Each table is streamed in batches so arbitrarily large tables export
      without exhausting memory.
    * Output is written to a temp file and atomically renamed, so a failure
      never leaves a partial/corrupt JSON file.
    * DECIMAL/NUMERIC/MONEY values are serialised as exact strings so no
      precision is ever lost to float rounding.
    * The source COUNT(*) and the written row count are compared; a mismatch
      fails the table (opt-out via STRICT_ROW_COUNT=no).
"""
import os
import sys
import json
import time
import uuid
import logging
import decimal
import datetime
import tempfile
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import yaml
import pyodbc
from dotenv import load_dotenv

logger = logging.getLogger("data_eng.export")

# How many rows to pull from the cursor per fetch. Balances memory vs. round-trips.
# Used as the default when the FETCH_BATCH_SIZE env var is not set.
DEFAULT_FETCH_BATCH_SIZE = 1000

# SQLSTATE prefixes that indicate a retriable/transient condition.
_TRANSIENT_SQLSTATES = ("08", "40001", "40197", "40501", "40613", "HYT00", "HYT01")


class DataIntegrityError(Exception):
    """Raised when exported data fails an integrity check (e.g. row count)."""


def timestamped_filename(base: str) -> str:
    """Insert a YYYYMMDD_HHMMSS timestamp before the file's extension.

    e.g. "export.log" -> "export_20260722_143000.log".
    """
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = Path(base)
    return str(path.with_name(f"{path.stem}_{timestamp}{path.suffix}"))


def setup_logging(log_file: str = "export.log", level: str = "INFO") -> None:
    """Configure logging to console and a rotating file (UTF-8)."""
    log_level = getattr(logging, str(level).upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(log_level)

    # Avoid duplicate handlers if setup_logging is called more than once.
    for handler in list(root.handlers):
        root.removeHandler(handler)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    root.addHandler(console_handler)

    try:
        parent = Path(log_file).parent
        if parent != Path(""):
            parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)
    except OSError as exc:
        # Logging to file is best-effort; never let it stop the run.
        logger.warning(f"Could not open log file '{log_file}': {exc}")


def _get_int_env(name: str, default: int) -> int:
    """Read an integer environment variable, falling back to a default."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning(
            f"Invalid integer for {name}={raw!r}; using default {default}."
        )
        return default


def _get_bool_env(name: str, default: Optional[bool]) -> Optional[bool]:
    """Read a boolean environment variable, falling back to a default."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _is_transient(exc: pyodbc.Error) -> bool:
    """Return True if a pyodbc error looks transient and worth retrying."""
    if exc.args and isinstance(exc.args[0], str):
        code = exc.args[0]
        return any(code.startswith(prefix) for prefix in _TRANSIENT_SQLSTATES)
    return False


def load_config() -> dict:
    """Load environment variables and return validated config settings.

    Raises:
        ValueError: If required environment variables are missing.
    """
    load_dotenv()
    config = {
        "server": os.getenv("DB_SERVER"),
        "database": os.getenv("DB_DATABASE"),
        "schema": os.getenv("DB_SCHEMA"),
        "driver": os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server"),
        "output_dir": os.getenv("OUTPUT_DIR", "output"),
        "login_timeout": _get_int_env("DB_LOGIN_TIMEOUT", 30),
        "tables_config": os.getenv("TABLES_CONFIG", "tables.yaml"),
        "username": os.getenv("DB_USERNAME"),
        "password": os.getenv("DB_PASSWORD"),
        "encrypt": _get_bool_env("DB_ENCRYPT", None),
        "trust_server_certificate": _get_bool_env("DB_TRUST_SERVER_CERTIFICATE", False),
        "strict_row_count": _get_bool_env("STRICT_ROW_COUNT", True),
        "connect_retries": max(0, _get_int_env("DB_CONNECT_RETRIES", 3)),
        "retry_backoff": max(1, _get_int_env("DB_RETRY_BACKOFF", 5)),
        "fetch_batch_size": max(1, _get_int_env("FETCH_BATCH_SIZE", DEFAULT_FETCH_BATCH_SIZE)),
        "manifest_dir": os.getenv("MANIFEST_DIR", "manifests"),
        "s3_enabled": _get_bool_env("S3_ENABLED", False),
        "s3_bucket": os.getenv("S3_BUCKET"),
        "s3_prefix": os.getenv("S3_PREFIX", ""),
        "dump_period": os.getenv("DUMP_PERIOD", ""),
        "aws_region": os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "aws_session_token": os.getenv("AWS_SESSION_TOKEN"),
    }

    missing = [k for k in ("server", "database") if not config[k]]
    if missing:
        env_names = {"server": "DB_SERVER", "database": "DB_DATABASE"}
        raise ValueError(
            f"Missing required environment variables: "
            f"{', '.join(env_names[k] for k in missing)}"
        )

    if config["username"] and not config["password"]:
        raise ValueError("DB_USERNAME is set but DB_PASSWORD is missing.")

    if config["s3_enabled"] and not config["s3_bucket"]:
        raise ValueError("S3_ENABLED is set but S3_BUCKET is missing.")

    return config


def qualify_name(name: str, default_schema: Optional[str]) -> str:
    """Qualify a bare table name with the default schema.

    Names that already contain a schema part (a ".") are returned unchanged.
    e.g. "customer" + default "dbo" -> "dbo.customer"; "sales.customer" stays.
    """
    stripped = name.strip()
    if "." in stripped or not default_schema:
        return stripped
    return f"{default_schema}.{stripped}"


def load_table_rules(
    path: str = "tables.yaml", default_schema: Optional[str] = None
) -> Tuple[Set[str], Set[str]]:
    """Load include/exclude table rules from a YAML file.

    Entries may be fully-qualified "schema.table" names, or bare table names
    which are qualified with ``default_schema`` (from DB_SCHEMA). Missing file
    or empty rules are treated as 'export everything'.

    Raises:
        ValueError: If the YAML is malformed or has an unexpected structure.
    """
    rules_path = Path(path)
    if not rules_path.exists():
        logger.warning(f"'{path}' not found; exporting all tables.")
        return set(), set()

    try:
        with open(rules_path, "r", encoding="utf-8") as f:
            table_config = yaml.safe_load(f) or {}
    except OSError as exc:
        raise ValueError(f"Could not read '{path}': {exc}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in '{path}': {exc}") from exc

    if not isinstance(table_config, dict):
        raise ValueError(
            f"'{path}' must contain a mapping with 'include'/'exclude' keys."
        )

    include_tables = {
        qualify_name(n, default_schema) for n in (table_config.get("include") or [])
    }
    exclude_tables = {
        qualify_name(n, default_schema) for n in (table_config.get("exclude") or [])
    }
    logger.info(
        f"Loaded table rules: {len(include_tables)} include, "
        f"{len(exclude_tables)} exclude."
    )
    return include_tables, exclude_tables


def build_connection_string(config: dict) -> str:
    """Build the ODBC connection string.

    Uses SQL authentication when a username is supplied, otherwise falls back
    to Windows/Trusted authentication. The returned string may contain the
    password and must never be logged.
    """
    parts = [
        f"DRIVER={{{config['driver']}}}",
        f"SERVER={config['server']}",
        f"DATABASE={config['database']}",
    ]
    if config.get("username"):
        parts.append(f"UID={config['username']}")
        parts.append(f"PWD={config['password']}")
    else:
        parts.append("Trusted_Connection=yes")

    if config.get("encrypt") is True:
        parts.append("Encrypt=yes")
    elif config.get("encrypt") is False:
        parts.append("Encrypt=no")
    if config.get("trust_server_certificate"):
        parts.append("TrustServerCertificate=yes")

    return f"{';'.join(parts)};"


def connect(config: dict) -> pyodbc.Connection:
    """Connect to SQL Server, retrying on transient errors.

    Raises:
        pyodbc.Error: If the connection cannot be established after retries.
    """
    conn_str = build_connection_string(config)
    retries = config.get("connect_retries", 0)
    backoff = config.get("retry_backoff", 5)
    auth = "SQL auth" if config.get("username") else "Windows auth"

    attempt = 0
    while True:
        attempt += 1
        logger.info(
            f"Connecting to {config['server']}/{config['database']} "
            f"({auth}, attempt {attempt}/{retries + 1})..."
        )
        try:
            conn = pyodbc.connect(conn_str, timeout=config.get("login_timeout", 30))
            # Read-only export against a static database; autocommit avoids
            # leaving idle transactions open between statements.
            conn.autocommit = True
            logger.info("Connected successfully.")
            return conn
        except pyodbc.Error as exc:
            if attempt > retries or not _is_transient(exc):
                logger.error(
                    f"Failed to connect to {config['server']}/{config['database']}."
                )
                raise
            logger.warning(
                f"Connection attempt {attempt} failed ({exc}); "
                f"retrying in {backoff}s..."
            )
            time.sleep(backoff)


def full_table_name(schema: str, table: str) -> str:
    """Return the fully-qualified "schema.table" name."""
    return f"{schema}.{table}"


def get_all_tables(
    cursor: pyodbc.Cursor, schema: Optional[str] = None
) -> List[Tuple[str, str]]:
    """Return a list of (schema, table) tuples for all base tables.

    If ``schema`` is provided, only tables within that schema are returned.
    """
    schema_filter = " AND TABLE_SCHEMA = ?" if schema else ""
    query = (
        f"SELECT TABLE_SCHEMA, TABLE_NAME "
        f"FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_TYPE = 'BASE TABLE'"
        f"{schema_filter}"
        f" ORDER BY TABLE_SCHEMA, TABLE_NAME"
    )
    params: Tuple[Any, ...] = (schema,) if schema else ()

    cursor.execute(query, params)
    return [(row.TABLE_SCHEMA, row.TABLE_NAME) for row in cursor.fetchall()]


def warn_missing_tables(
    include_tables: Set[str], all_tables: List[Tuple[str, str]]
) -> None:
    """Warn about included tables not found in INFORMATION_SCHEMA."""
    all_tables_set = {full_table_name(s, t) for s, t in all_tables}
    for entry in include_tables:
        if entry not in all_tables_set:
            logger.warning(
                f"Table '{entry}' listed in tables.yaml (include) was not "
                f"found in INFORMATION_SCHEMA. Skipping."
            )


def should_export(
    schema: str, table: str, include_tables: Set[str], exclude_tables: Set[str]
) -> bool:
    """Decide whether a table should be exported based on include/exclude rules.

    Matching is done exclusively on the fully-qualified "schema.table" name.
    """
    full_name = full_table_name(schema, table)

    if full_name in exclude_tables:
        return False

    if not include_tables:
        return True

    return full_name in include_tables


def filter_tables(
    all_tables: List[Tuple[str, str]],
    include_tables: Set[str],
    exclude_tables: Set[str],
) -> List[Tuple[str, str]]:
    """Return the list of tables that pass the include/exclude rules."""
    return [
        (schema, table)
        for schema, table in all_tables
        if should_export(schema, table, include_tables, exclude_tables)
    ]


def quote_identifier(name: str) -> str:
    """Safely bracket-quote a SQL Server identifier, escaping ']' as ']]'."""
    escaped = name.replace("]", "]]")
    return f"[{escaped}]"


def serialize_special(value: Any) -> Any:
    """JSON serialiser for types pyodbc returns that json can't handle natively.

    NULL comes back as None (-> null) automatically, so it isn't handled here.
    """
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        # str preserves exact numeric precision; float() may round
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, uuid.UUID):
        return str(value)
    raise TypeError(
        f"Object of type {type(value).__name__} is not JSON serialisable"
    )


def _iter_rows(cursor: pyodbc.Cursor, batch_size: int) -> Iterable[Any]:
    """Yield rows from a cursor in batches to limit memory usage."""
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield row


def table_output_file(output_dir: str, table: str) -> Path:
    """Return the local JSON output path for a table."""
    return Path(output_dir) / f"{table}.json"


def build_s3_key(prefix: Optional[str], period: Optional[str], filename: str) -> str:
    """Join optional prefix and period segments with a filename into an S3 key.

    e.g. prefix="exports", period="2026-07", filename="customer.json"
    -> "exports/2026-07/customer.json".
    """
    segments = [
        segment.strip("/")
        for segment in (prefix, period)
        if segment and segment.strip("/")
    ]
    segments.append(filename)
    return "/".join(segments)


def create_s3_client(config: dict) -> Any:
    """Create a boto3 S3 client.

    Explicit AWS credentials from the config are used when present; otherwise
    boto3's standard credential chain (env vars, shared config, IAM role, SSO)
    is used. boto3 is imported lazily so the dependency is only required when
    S3 upload is enabled.

    Raises:
        RuntimeError: If boto3 is not installed.
    """
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "S3_ENABLED is set but boto3 is not installed. "
            "Install it with 'pip install boto3'."
        ) from exc

    client_kwargs: Dict[str, Any] = {}
    if config.get("aws_region"):
        client_kwargs["region_name"] = config["aws_region"]
    if config.get("aws_access_key_id") and config.get("aws_secret_access_key"):
        client_kwargs["aws_access_key_id"] = config["aws_access_key_id"]
        client_kwargs["aws_secret_access_key"] = config["aws_secret_access_key"]
        if config.get("aws_session_token"):
            client_kwargs["aws_session_token"] = config["aws_session_token"]

    return boto3.client("s3", **client_kwargs)


def upload_file_to_s3(s3_client: Any, bucket: str, key: str, file_path: Path) -> None:
    """Upload a local file to S3.

    boto3's ``upload_file`` transparently switches to a multipart, retrying
    transfer for large files, so arbitrarily large table exports upload safely.
    """
    s3_client.upload_file(str(file_path), bucket, key)


def _export_table_once(
    conn: pyodbc.Connection,
    schema: str,
    table: str,
    output_dir: str,
    strict: bool,
    batch_size: int,
) -> int:
    """Perform a single export attempt for one table. See export_table."""
    table_fqn = full_table_name(schema, table)
    logger.info(f"Exporting {table_fqn}...")
    out_file = table_output_file(output_dir, table)
    qualified = f"{quote_identifier(schema)}.{quote_identifier(table)}"

    cursor = conn.cursor()
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=output_dir, prefix=f"{table}.", suffix=".tmp"
    )
    count = 0
    try:
        # COUNT_BIG avoids overflow on tables with > 2^31 rows.
        cursor.execute(f"SELECT COUNT_BIG(*) FROM {qualified}")
        expected = cursor.fetchone()[0]

        cursor.execute(f"SELECT * FROM {qualified}")
        columns = [col[0] for col in cursor.description]

        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            f.write("[")
            for row in _iter_rows(cursor, batch_size):
                record = dict(zip(columns, row))
                line = json.dumps(
                    record, ensure_ascii=False, default=serialize_special
                )
                separator = "," if count else ""
                f.write(f"{separator}\n  {line}")
                count += 1
            f.write("\n]\n")

        if expected is not None and count != expected:
            msg = (
                f"Row count mismatch for {table_fqn}: source reported "
                f"{expected} row(s) but {count} were written."
            )
            if strict:
                raise DataIntegrityError(msg)
            logger.warning(msg)

        os.replace(tmp_path, out_file)  # atomic on same filesystem
    except BaseException:
        # Clean up the partial temp file on any failure/interrupt.
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise
    finally:
        cursor.close()

    logger.info(f"Export file {out_file} ({count} rows)")
    return count


def export_table(
    conn: pyodbc.Connection,
    schema: str,
    table: str,
    output_dir: str,
    strict: bool = True,
    retries: int = 1,
    backoff: int = 5,
    batch_size: int = DEFAULT_FETCH_BATCH_SIZE,
) -> int:
    """Stream a single table to a JSON array file, verifying the row count.

    Writes to a temp file and atomically renames on success so a failure
    never leaves a truncated/corrupt output file behind. Transient database
    errors are retried up to ``retries`` times.

    Returns:
        The number of rows exported.
    """
    table_fqn = full_table_name(schema, table)
    attempt = 0
    while True:
        attempt += 1
        try:
            return _export_table_once(
                conn, schema, table, output_dir, strict, batch_size
            )
        except pyodbc.Error as exc:
            if attempt > retries or not _is_transient(exc):
                raise
            logger.warning(
                f"Transient error exporting {table_fqn} ({exc}); "
                f"retry {attempt}/{retries} in {backoff}s..."
            )
            time.sleep(backoff)


def write_manifest(manifest_dir: str, manifest: dict) -> None:
    """Write the run manifest to a temp file and atomically rename it.

    The manifest is always written locally (never uploaded to S3).
    """
    manifest_path = Path(manifest_dir) / timestamped_filename("_manifest.json")
    try:
        Path(manifest_dir).mkdir(parents=True, exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=manifest_dir, prefix="_manifest.", suffix=".tmp"
        )
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, manifest_path)
        logger.info(f"Wrote run manifest file {manifest_path}")
    except OSError as exc:
        logger.warning(f"Could not write manifest '{manifest_path}': {exc}")


def main() -> int:
    """Execute the export and return a process exit code."""
    log_dir = os.getenv("LOG_DIR", "logs")
    log_file = timestamped_filename(os.getenv("LOG_FILE", "export.log"))
    setup_logging(
        log_file=str(Path(log_dir) / log_file) if log_dir else log_file,
        level=os.getenv("LOG_LEVEL", "INFO"),
    )

    try:
        config = load_config()
    except ValueError as exc:
        logger.error(f"Configuration error: {exc}")
        return 1

    try:
        include_tables, exclude_tables = load_table_rules(
            config["tables_config"], config.get("schema")
        )
    except ValueError as exc:
        logger.error(f"Table rules error: {exc}")
        return 1

    conn = None
    failures = 0
    exported = 0
    total_rows = 0
    table_results: List[Dict[str, Any]] = []
    run_id = str(uuid.uuid4())
    started_at = datetime.datetime.now(datetime.timezone.utc)
    try:
        conn = connect(config)

        with conn.cursor() as cursor:
            all_tables = get_all_tables(cursor, config.get("schema"))

        if config.get("schema"):
            logger.info(f"Restricting export to schema '{config['schema']}'.")

        warn_missing_tables(include_tables, all_tables)

        tables_to_export = filter_tables(all_tables, include_tables, exclude_tables)
        logger.info(f"Tables to export: {len(tables_to_export)}")
        logger.info(f"Strict row count: {config['strict_row_count']}")

        try:
            Path(config["output_dir"]).mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.error(
                f"Could not create output directory '{config['output_dir']}': {exc}"
            )
            return 1

        s3_client = None
        if config["s3_enabled"]:
            try:
                s3_client = create_s3_client(config)
            except RuntimeError as exc:
                logger.error(str(exc))
                return 1
            logger.info(
                f"S3 upload enabled: bucket '{config['s3_bucket']}', "
                f"prefix '{config['s3_prefix']}'."
            )

        for schema, table in tables_to_export:
            table_fqn = full_table_name(schema, table)
            out_file = table_output_file(config["output_dir"], table)
            try:
                rows = export_table(
                    conn,
                    schema,
                    table,
                    config["output_dir"],
                    strict=config["strict_row_count"],
                    backoff=config["retry_backoff"],
                    batch_size=config["fetch_batch_size"],
                )
            except DataIntegrityError as exc:
                failures += 1
                logger.error(f"Integrity check failed for {table_fqn}: {exc}")
                table_results.append(
                    {"table": table_fqn, "rows": None, "status": "integrity_error",
                     "error": str(exc)}
                )
                continue
            except pyodbc.Error as exc:
                failures += 1
                logger.error(f"Database error exporting {table_fqn}: {exc}")
                table_results.append(
                    {"table": table_fqn, "rows": None, "status": "db_error",
                     "error": str(exc)}
                )
                continue
            except Exception as exc:
                failures += 1
                logger.exception(f"Failed to export {table_fqn}")
                table_results.append(
                    {"table": table_fqn, "rows": None, "status": "error",
                     "error": str(exc)}
                )
                continue

            result: Dict[str, Any] = {
                "table": table_fqn,
                "file": out_file.name,
                "rows": rows,
                "status": "ok",
            }

            if s3_client is not None:
                key = build_s3_key(
                    config["s3_prefix"], config["dump_period"], out_file.name
                )
                try:
                    upload_file_to_s3(
                        s3_client, config["s3_bucket"], key, out_file
                    )
                except Exception as exc:
                    failures += 1
                    logger.error(f"Failed to upload {table_fqn} to S3: {exc}")
                    result.update(
                        status="s3_error", s3_uploaded=False, error=str(exc)
                    )
                    table_results.append(result)
                    continue
                result["s3_key"] = key
                result["s3_uploaded"] = True
                logger.info(
                    f"Uploaded {out_file.name} to "
                    f"s3://{config['s3_bucket']}/{key}"
                )

            exported += 1
            total_rows += rows
            table_results.append(result)
    except pyodbc.Error as exc:
        logger.error(f"Database error: {exc}")
        return 1
    except KeyboardInterrupt:
        logger.warning("Interrupted by user; shutting down.")
        return 130
    except Exception:
        logger.exception("Unexpected error during export.")
        return 1
    finally:
        if conn is not None:
            try:
                conn.close()
            except pyodbc.Error as exc:
                logger.warning(f"Error closing connection: {exc}")

    finished_at = datetime.datetime.now(datetime.timezone.utc)
    if table_results:
        write_manifest(
            config["manifest_dir"],
            {
                "run_id": run_id,
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "server": config["server"],
                "database": config["database"],
                "schema_filter": config.get("schema"),
                "strict_row_count": config["strict_row_count"],
                "s3_enabled": config["s3_enabled"],
                "s3_bucket": config["s3_bucket"] if config["s3_enabled"] else None,
                "s3_prefix": config["s3_prefix"] if config["s3_enabled"] else None,
                "dump_period": config["dump_period"] if config["s3_enabled"] else None,
                "tables_exported": exported,
                "tables_failed": failures,
                "total_rows": total_rows,
                "tables": table_results,
            },
        )

    logger.info(
        f"Exported {exported} table(s), {total_rows} row(s), "
        f"{failures} failure(s)."
    )
    if failures:
        logger.error(f"Done with {failures} table(s) failed.")
        return 1

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
