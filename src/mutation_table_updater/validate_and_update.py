#!/usr/bin/env python3
"""
Validate tables against YAML schemas, then archive+replace atomically,
and append entries to updates.log.

Usage examples:
  python scripts/validate_and_update.py --input uploads/ --user "nellaby"
  python scripts/validate_and_update.py --input data/mutations.csv --user "nellaby"
"""

import argparse
import datetime as dt
import hashlib
import json
import logging
import os
import pathlib
import re
import shutil
import sys
from pathlib import Path

import pandas as pd
import yaml

# ---------- Config defaults ----------
TABLES_DIR_DEFAULT = "tables"
ARCHIVE_DIR_DEFAULT = "archive"
SCHEMAS_DIR_DEFAULT = "schemas"
LOG_FILE_DEFAULT = "updates.log"

# ---------- Logging Setup -------------


def setup_logging(log_filename, logging_level):
    handlers = [
        logging.StreamHandler(),  # stdout
        logging.FileHandler(log_filename, mode="w"),  # file
    ]

    logging.basicConfig(
        level=logging_level,
        format="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
        handlers=handlers,
    )


# ---------- Helpers ----------
def dir_path(path):
    if Path(path).is_dir():
        return path
    else:
        raise argparse.ArgumentTypeError(f"'{path}' is not a valid directory")


def utc_now_iso() -> str:
    """
    Returns an ISO 8601 UTC timestamp with seconds precision and a trailing 'Z'.
    Example: '2026-01-02T10:47:00Z'
    """
    now = dt.datetime.now(dt.UTC)  # tz-aware UTC datetime
    # .isoformat(timespec="seconds") yields 'YYYY-MM-DDTHH:MM:SS+00:00' for tz-aware
    # Convert '+00:00' to 'Z' for canonical UTC representation
    return now.isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with Path.open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def read_table(path: str) -> pd.DataFrame:
    ext = Path(path).suffix
    if ext in [".csv"]:
        return pd.read_csv(path)
    elif ext in [".tsv", ".tab"]:
        return pd.read_csv(path, sep="\t")
    elif ext in [".xlsx", ".xls"]:
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        return pd.read_excel(path, engine=engine)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def list_candidate_files(input_path: str) -> list[str]:
    if Path(input_path).is_dir():
        files = []
        for root, _, fnames in os.walk(input_path):
            for fn in fnames:
                if fn.lower().endswith((".csv", ".tsv", ".tab", ".xlsx", ".xls")):
                    files.append(str(Path(root) / fn))
        return files
    elif Path(input_path).is_file():
        return [input_path]
    else:
        raise FileNotFoundError(f"Input not found: {input_path}")


def load_schemas(schemas_dir: str) -> dict[str, dict]:
    schemas = {}
    for fn in Path(schemas_dir).iterdir():
        if str(fn).lower().endswith((".yml", ".yaml")):
            with Path.open(Path(schemas_dir) / fn, "r", encoding="utf-8") as f:
                schema = yaml.safe_load(f)
            # Index by canonical filename (preferred) or table name
            key = schema.get("name")
            if not key:
                raise ValueError(f"Schema {fn} missing 'name'")
            schemas[key] = schema
    return schemas


def find_schema_for_file(schemas: dict[str, dict], file_path: str) -> dict | None:
    base = Path(file_path).name
    seg = str(base.split("_")[0])

    for key, sch in schemas.items():
        if key.startswith(seg):
            return sch
    return None


def validate_schemas(schemas: dict[str, dict]) -> None:
    """Check that Schemas loaded are correctly formatted"""
    # Load in schemas
    # Check the approriate keys are loaded
    expected_yaml_main_keys = {"name", "filename", "strict_columns", "primary_key", "columns"}

    logging.info("Checking Schema formats are correct.")
    for segment in schemas:
        seg_dict = schemas[segment]
        missing = expected_yaml_main_keys - set(seg_dict.keys())
        if len(missing) > 0:
            logging.critical(
                "Segment schema file %s was missing the following essential "
                'keys "%s". YAML should contain these essential keys: %s',
                segment,
                ", ".join(missing),
                ", ".join(expected_yaml_main_keys),
            )
            return False
        # Check primary keys are all present in 'columns'
        else:
            primary_keys_list = seg_dict["primary_key"]
            column_names = [item["name"] for item in seg_dict["columns"]]

            primary_not_in_columns = set(primary_keys_list) - set(column_names)
            columns_not_in_primary = set(column_names) - set(primary_keys_list)

            if len(columns_not_in_primary) > 0:
                logging.critical(
                    'Segment schema file %s has column names "%s" that are not '
                    'included in primary keys "%s". Please check schema formatting.',
                    segment,
                    ", ".join(columns_not_in_primary),
                    ", ".join(primary_keys_list),
                )
                return False
            elif len(primary_not_in_columns) > 0:
                logging.critical(
                    'Segment schema file %s has primary keys "%s" that are not '
                    'included in column names "%s". Please check schema formatting.',
                    segment,
                    ", ".join(primary_not_in_columns),
                    ", ".join(column_names),
                )
                return False

            logging.info("Schema formatting check passed.")
            return True


def check_filename(fn: str, seg_names: list) -> None:
    """Check if filename starts with segment, else fail gracefully."""
    filename = Path(fn).name
    if str(filename).startswith(tuple(seg_names)):
        return True
    else:
        logging.warning("Input File %s did not start with a segment ID (%s). Skipped.", filename, ", ".join(seg_names))
        return False


def map_schema_to_file(
    files: list[str], schemas: dict[str, dict], segment_names: list
) -> tuple[dict[str, dict], list[str]]:
    """Attach schema to each file based on filename matching."""

    schema_file_map: dict[str, dict] = {}
    skipped_files = []

    for f in files:
        # check filename stats with segment ID
        if check_filename(f, segment_names) is True:
            schema = find_schema_for_file(schemas, f)
            # If schema found, map it
            if schema:
                schema_file_map[f] = schema
                logging.info("Mapped schema '%s' to file '%s'.", schema["name"], Path(f).name)
            else:
                logging.warning("No matching schema found for file '%s'.", Path(f).name)
                skipped_files.append(f)
        else:
            logging.warning("File %s skipped due to filename check failure.", Path(f).name)
            skipped_files.append(f)

    if len(schema_file_map) > 0:
        logging.info("Schema mapping completed for %d files.", len(schema_file_map))
        return schema_file_map, skipped_files
    else:
        logging.critical("No files were mapped to schemas. Please check input files and schema directory.")
        sys.exit(1)


# ---------- Validation primitives ----------
def _type_check(series: pd.Series, typ: str) -> list[int]:
    """Return indices of rows failing the type check."""
    failures = []
    if typ == "str":
        # allow NaN if not required; actual non-str detected by not being instance of str after conversion
        series.astype(str)
        # Always passes; combine with required rule separately
    elif typ == "int":
        for idx, val in series.items():
            try:
                if pd.isna(val):  # allow NaN; 'required' will catch if needed
                    continue
                _ = int(val)
            except Exception:
                failures.append(idx)
    elif typ == "float":
        for idx, val in series.items():
            try:
                if pd.isna(val):
                    continue
                _ = float(val)
            except Exception:
                failures.append(idx)
    elif typ == "date":
        for idx, val in series.items():
            if pd.isna(val):
                continue
            try:
                pd.to_datetime(val, utc=True, errors="raise")
            except Exception:
                failures.append(idx)
    else:
        raise ValueError(f"Unknown type: {typ}")
    return failures


# column rules
def required_columns(df: pd.DataFrame, schema: dict) -> str:
    required_cols = [c["name"] for c in schema.get("columns", []) if c.get("required")]
    for col in required_cols:
        if col not in df.columns:
            error = f"Missing required column: {col}"
            return error


def validate_dataframe(df: pd.DataFrame, schema: dict) -> list[str]:
    """
    Return list of human-readable validation error messages.
    """
    errors: list[str] = []
    # Required columns
    required_column_error = required_columns(df, schema)
    errors.append(required_column_error)

    # Unexpected columns (optional strict mode)
    allowed_cols = [c["name"] for c in schema.get("columns", [])]
    if schema.get("strict_columns", True):
        extra = sorted(set(df.columns) - set(allowed_cols))
        if extra:
            errors.append(f"Unexpected columns present: {extra}")

    # Per-column checks
    for col_rule in schema.get("columns", []):
        col = col_rule["name"]
        if col not in df.columns:
            # Already flagged if required; skip otherwise
            continue
        series = df[col]

        # Required non-null
        if col_rule.get("required"):
            null_idx = list(series[series.isna()].index)
            if null_idx:
                errors.append(f"{col}: {len(null_idx)} required values are null")

        # Type checks
        if "type" in col_rule:
            bad_idx = _type_check(series, col_rule["type"])
            if bad_idx:
                errors.append(f"{col}: {len(bad_idx)} rows fail type '{col_rule['type']}'")

        # Regex
        if "pattern" in col_rule:
            reg = re.compile(col_rule["pattern"])
            bad_rows = [i for i, v in series.items() if not (pd.isna(v) or reg.match(str(v)))]
            if bad_rows:
                errors.append(f"{col}: {len(bad_rows)} rows fail regex '{col_rule['pattern']}'")

        # Allowed values (inline)
        if "allowed_values" in col_rule:
            allowed = set(col_rule["allowed_values"])
            bad_rows = [i for i, v in series.items() if not (pd.isna(v) or str(v) in allowed)]
            if bad_rows:
                errors.append(f"{col}: {len(bad_rows)} rows not in allowed_values")

        # Allowed values (external file)
        if "allowed_values_file" in col_rule:
            fpath = col_rule["allowed_values_file"]
            with Path.open(fpath, "r", encoding="utf-8") as f:
                allowed = {line.strip() for line in f if line.strip()}
            bad_rows = [i for i, v in series.items() if not (pd.isna(v) or str(v) in allowed)]
            if bad_rows:
                errors.append(f"{col}: {len(bad_rows)} rows not in allowed_values_file={fpath}")

        # Numeric range
        if col_rule.get("type") in ("int", "float"):
            lo = col_rule.get("min")
            hi = col_rule.get("max")
            if lo is not None:
                bad_rows = list(df.index[pd.to_numeric(series, errors="coerce") < lo])
                if bad_rows:
                    errors.append(f"{col}: {len(bad_rows)} values < {lo}")
            if hi is not None:
                bad_rows = list(df.index[pd.to_numeric(series, errors="coerce") > hi])
                if bad_rows:
                    errors.append(f"{col}: {len(bad_rows)} values > {hi}")

    # Primary key uniqueness
    pk = schema.get("primary_key")
    if pk:
        dup_mask = df.duplicated(subset=pk, keep=False)
        dup_count = int(dup_mask.sum())
        if dup_count:
            errors.append(f"Primary key {pk} has {dup_count} duplicate rows")

    # Foreign keys (referential integrity)
    # Format example:
    # foreign_keys:
    #   - column: gene
    #     ref_table: genes.csv
    #     ref_column: gene
    for fk in schema.get("foreign_keys", []):
        col = fk["column"]
        ref_table = fk["ref_table"]
        ref_col = fk["ref_column"]
        # Load reference quickly from tables/
        ref_path = str(Path(TABLES_DIR_DEFAULT) / ref_table)
        if not Path.exists(ref_path):
            errors.append(f"Foreign key reference table not found: {ref_table}")
        else:
            ref_df = read_table(ref_path)
            allowed = set(ref_df[ref_col].astype(str))
            bad = df[~df[col].astype(str).isin(allowed)][col]
            if len(bad) > 0:
                errors.append(f"Foreign key violation on {col}: {len(bad)} values not in {ref_table}.{ref_col}")

    return errors


# ---------- Archiving & Replacement ----------
def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)


def archive_and_replace(
    validated_files: list[tuple[str, dict]], tables_dir: str, archive_dir: str, user: str, log_file: str
) -> None:
    """validated_files: list of (source_path, schema) tuples."""
    # Compute date folder
    date_str = dt.date.today().isoformat()
    archive_today = str(Path(archive_dir) / date_str)
    ensure_dir(archive_today)
    ensure_dir(tables_dir)

    for src_path, schema in validated_files:
        base = Path(src_path).name
        target_name = schema.get("filename") or base
        target_path = str(Path(tables_dir) / target_name)

        # Hash counts old/new
        old_exists = Path(target_path).exists()
        old_hash = sha256_file(target_path) if old_exists else None
        old_rows = None
        if old_exists:
            try:
                old_rows = len(read_table(target_path))
            except Exception:
                old_rows = None

        new_hash = sha256_file(src_path)
        new_rows = len(read_table(src_path))

        # Archive old (if exists)
        if old_exists:
            shutil.move(target_path, str(Path(archive_today) / Path(target_path).name))

        # Replace with new
        shutil.copy2(src_path, target_path)

        # Log
        entry = {
            "timestamp_utc": utc_now_iso(),
            "user": user,
            "table": target_name,
            "source": str(Path(src_path).resolve()),
            "action": "update",
            "old_sha256": old_hash,
            "new_sha256": new_hash,
            "old_rows": old_rows,
            "new_rows": new_rows,
            "archive_path": str(Path(archive_today) / Path(target_path).name) if old_exists else None,
        }
        with Path.open(log_file, "a", encoding="utf-8") as lf:
            lf.write(json.dumps(entry) + "\n")


# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Validate tables, then archive+replace if all pass.")
    ap.add_argument("--input", required=True, help="Path to a file OR a directory containing tables.")
    ap.add_argument("--tables-dir", type=dir_path, default=TABLES_DIR_DEFAULT)
    ap.add_argument("--archive-dir", type=dir_path, default=ARCHIVE_DIR_DEFAULT)
    ap.add_argument("--schemas-dir", type=dir_path, default=SCHEMAS_DIR_DEFAULT)
    ap.add_argument("--log-file", default=LOG_FILE_DEFAULT)
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    ap.add_argument("--user", default=os.getenv("USER", "unknown"))
    args = ap.parse_args()

    # Set up Logging
    setup_logging(args.log_file, args.log_level)

    # Collect files and schemas
    files = list_candidate_files(args.input)
    if not files:
        print("No candidate table files found in input.", file=sys.stderr)
        sys.exit(2)

    schemas_map = load_schemas(args.schemas_dir)

    if validate_schemas(schemas_map) is False:
        sys.exit()

    segment_names = ["pb2", "pb1", "ha", "m", "na", "np", "ns", "pa"]

    # Map schema to input files
    schema_file_map, skipped_files = map_schema_to_file(files, schemas_map, segment_names)

    # # All good → archive + replace
    # archive_and_replace(validated, args.tables_dir, args.archive_dir, args.user, args.log_file)
    # print(f"Success. {len(validated)} table(s) validated and updated.\nLog: {args.log_file}")


if __name__ == "__main__":
    main()
