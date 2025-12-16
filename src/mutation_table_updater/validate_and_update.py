
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
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yaml

# ---------- Config defaults ----------
TABLES_DIR_DEFAULT = "tables"
ARCHIVE_DIR_DEFAULT = "archive"
SCHEMAS_DIR_DEFAULT = "schemas"
LOG_FILE_DEFAULT = "updates.log"

# ---------- Helpers ----------
def utc_now_iso() -> str:
    return dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def read_table(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".csv"]:
        return pd.read_csv(path)
    elif ext in [".tsv", ".tab"]:
        return pd.read_csv(path, sep="\t")
    elif ext in [".xlsx", ".xls"]:
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        return pd.read_excel(path, engine=engine)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

def list_candidate_files(input_path: str) -> List[str]:
    if os.path.isdir(input_path):
        files = []
        for root, _, fnames in os.walk(input_path):
            for fn in fnames:
                if fn.lower().endswith((".csv", ".tsv", ".tab", ".xlsx", ".xls")):
                    files.append(os.path.join(root, fn))
        return files
    elif os.path.isfile(input_path):
        return [input_path]
    else:
        raise FileNotFoundError(f"Input not found: {input_path}")

def load_schemas(schemas_dir: str) -> Dict[str, dict]:
    schemas = {}
    for fn in os.listdir(schemas_dir):
        if fn.lower().endswith((".yml", ".yaml")):
            with open(os.path.join(schemas_dir, fn), "r", encoding="utf-8") as f:
                schema = yaml.safe_load(f)
            # Index by canonical filename (preferred) or table name
            key = schema.get("filename") or schema.get("name")
            if not key:
                raise ValueError(f"Schema {fn} missing 'filename' or 'name'")
            schemas[key] = schema
    return schemas

def find_schema_for_file(schemas: Dict[str, dict], file_path: str) -> Optional[dict]:
    base = Path.name(file_path)
    seg = str(base.split('_')[0])+'_'
    print(f"Segment: {seg}")
    
    for key, sch in schemas.items():
        print(f"Key {key}")
        print(f"Schema {sch}")
        if key.startswith(seg):
            print("match")
            return sch
    return None

# ---------- Validation primitives ----------
def _type_check(series: pd.Series, typ: str) -> List[int]:
    """Return indices of rows failing the type check."""
    failures = []
    if typ == "str":
        # allow NaN if not required; actual non-str detected by not being instance of str after conversion
        coerced = series.astype(str)
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

def validate_dataframe(df: pd.DataFrame, schema: dict) -> List[str]:
    """
    Return list of human-readable validation error messages.
    """
    errors: List[str] = []

    # Required columns
    required_cols = [c["name"] for c in schema.get("columns", []) if c.get("required")]
    for col in required_cols:
        if col not in df.columns:
            errors.append(f"Missing required column: {col}")

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
            with open(fpath, "r", encoding="utf-8") as f:
                allowed = set([line.strip() for line in f if line.strip()])
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
        ref_path = os.path.join(TABLES_DIR_DEFAULT, ref_table)
        if not os.path.exists(ref_path):
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
    os.makedirs(p, exist_ok=True)

def archive_and_replace(validated_files: List[Tuple[str, dict]], tables_dir: str, archive_dir: str,
                        user: str, log_file: str) -> None:
    """validated_files: list of (source_path, schema) tuples."""
    # Compute date folder
    date_str = dt.date.today().isoformat()
    archive_today = os.path.join(archive_dir, date_str)
    ensure_dir(archive_today)
    ensure_dir(tables_dir)

    for src_path, schema in validated_files:
        base = os.path.basename(src_path)
        target_name = schema.get("filename") or base
        target_path = os.path.join(tables_dir, target_name)

        # Hash counts old/new
        old_exists = os.path.exists(target_path)
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
            shutil.move(target_path, os.path.join(archive_today, os.path.basename(target_path)))

        # Replace with new
        shutil.copy2(src_path, target_path)

        # Log
        entry = {
            "timestamp_utc": utc_now_iso(),
            "user": user,
            "table": target_name,
            "source": os.path.abspath(src_path),
            "action": "update",
            "old_sha256": old_hash,
            "new_sha256": new_hash,
            "old_rows": old_rows,
            "new_rows": new_rows,
            "archive_path": os.path.join(archive_today, os.path.basename(target_path)) if old_exists else None,
        }
        with open(log_file, "a", encoding="utf-8") as lf:
            lf.write(json.dumps(entry) + "\n")

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Validate tables, then archive+replace if all pass.")
    ap.add_argument("--input", required=True, help="Path to a file OR a directory containing tables.")
    ap.add_argument("--tables-dir", default=TABLES_DIR_DEFAULT)
    ap.add_argument("--archive-dir", default=ARCHIVE_DIR_DEFAULT)
    ap.add_argument("--schemas-dir", default=SCHEMAS_DIR_DEFAULT)
    ap.add_argument("--log-file", default=LOG_FILE_DEFAULT)
    ap.add_argument("--user", default=os.getenv("USER", "unknown"))
    args = ap.parse_args()

    # Collect files and schemas
    files = list_candidate_files(args.input)
    if not files:
        print("No candidate table files found in input.", file=sys.stderr)
        sys.exit(2)

    schemas_map = load_schemas(args.schemas_dir)

    # Validate all first
    validated: List[Tuple[str, dict]] = []
    all_errors: Dict[str, List[str]] = {}
    for f in files:
        schema = find_schema_for_file(schemas_map, f)
        breakpoint()
        if not schema:
            all_errors[f] = [f"No matching schema found in {args.schemas_dir} for file {os.path.basename(f)}"]
            continue
        try:
            df = read_table(f)
        except Exception as e:
            all_errors[f] = [f"Failed to read table: {e}"]
            continue

        errs = validate_dataframe(df, schema)
        if errs:
            all_errors[f] = errs
        else:
            validated.append((f, schema))

    # If any file failed, print a grouped report and exit non-zero
    if len(validated) != len(files):
        print("Validation failed for one or more files:\n", file=sys.stderr)
        for f, errs in all_errors.items():
            if errs:
                print(f"--- {f} ---", file=sys.stderr)
                for e in errs:
                    print(f"  * {e}", file=sys.stderr)
        sys.exit(1)

    # All good → archive + replace
    archive_and_replace(validated, args.tables_dir, args.archive_dir, args.user, args.log_file)
    print(f"Success. {len(validated)} table(s) validated and updated.\nLog: {args.log_file}")

if __name__ == "__main__":
    main()
