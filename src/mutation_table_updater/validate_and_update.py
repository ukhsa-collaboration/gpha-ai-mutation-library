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
import logging
import os
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
            with Path.open(fn, "r", encoding="utf-8") as f:
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


def validate_single_dataframe(df, schema) -> list[str]:
    errors: list[str] = []

    # column rules
    def required_columns(df: pd.DataFrame, schema: dict) -> str:
        required_cols = [c["name"] for c in schema.get("columns", []) if c.get("required")]
        missing_cols = []
        for col in required_cols:
            if col not in df.columns:
                missing_cols.append(col)
        if len(missing_cols) > 0:
            error = f"Missing required column: {', '.join(missing_cols)}"
            logging.warning(error)
            return error

    def unexpected_columns(df: pd.DataFrame, schema: dict) -> str:
        allowed_cols = [c["name"] for c in schema.get("columns", [])]
        if schema.get("strict_columns", True):
            extra = sorted(set(df.columns) - set(allowed_cols))
            if extra:
                error = f"Unexpected columns present: {', '.join(extra)}"
                logging.warning(error)
                return error

    # Columns that require non-null values
    def required_values(df, col) -> str:
        series = df[col]
        null_idx = list(series[series.isna()].index)
        if null_idx:
            error = f"{col}: {len(null_idx)} required values are null"
            logging.critical(error)
            return error

    # Required columns
    required_column_error = required_columns(df, schema)
    errors.append(required_column_error)

    # Unexpected columns (optional strict mode)
    unexpected_column_error = unexpected_columns(df, schema)
    errors.append(unexpected_column_error)

    # Per-column checks
    for col_rule in schema.get("columns", []):
        col = col_rule["name"]
        series = df[col]
        if col not in df.columns:
            # Already flagged if required; skip otherwise
            continue
        # Type checks
        # Ensure columns which expect values are not null/empty
        if col_rule.get("required"):
            required_value_error = required_values(df, col)
            errors.append(required_value_error)

        if "type" in col_rule:
            bad_idx = _type_check(series, col_rule["type"])
            if bad_idx:
                error = f"{col}: {len(bad_idx)} rows fail type '{col_rule['type']}'"
                logging.critical(error)
                errors.append(error)

        # Regex
        if "pattern" in col_rule:
            reg = re.compile(col_rule["pattern"])
            logging.info("Checking regex for column %s with pattern %s", col, col_rule["pattern"])
            bad_rows = [i for i, v in series.items() if not (pd.isna(v) or reg.match(str(v)))]
            if bad_rows:
                error = f"{col}: {len(bad_rows)} rows fail regex '{col_rule['pattern']}'"
                logging.critical(error)
                errors.append(error)

        # Allowed values (inline)
        if "allowed_values_file" in col_rule:
            fpath = Path.cwd() / col_rule["allowed_values_file"]
            with Path.open(fpath, "r", encoding="utf-8") as f:
                allowed = {line.strip().lower() for line in f if line.strip()}
            col_lower = df[col].astype("string").str.lower()
            not_allowed = df[~col_lower.isin(allowed)]
            if not not_allowed.empty:
                incorrect_values = col_lower.tolist()
                error = (
                    f"{col}: {len(not_allowed)} row(s) contain values not "
                    "present in the reference file (check schemas/reference_lists/)."
                )
                logging.critical(error)
                logging.critical("Incorrect value(s): %s", ", ".join(map(str, incorrect_values)))
                errors.append(error)

        # Numeric range
        if (col_rule.get("type") in ("int", "float")) and (col_rule.get("min") and col_rule.get("max")):
            lo = col_rule.get("min")
            hi = col_rule.get("max")
            values = df[col_rule["name"]].tolist()
            if all(isinstance(x, (int, float)) for x in values):
                # convert all values to float for simplicity
                if (lo is not None) & (hi is not None):
                    less_than = [x for x in values if x < lo]
                    greater_than = [x for x in values if x > hi]
                    if len(greater_than) > 0 or len(less_than) > 0:
                        error = f"Value outside bounds ({lo} - {hi}) for column '{col}'. check values: {values}"
                        logging.critical(error)
                        errors.append(error)
            else:
                error = f"Non-numeric value found in numeric range check for column '{col}'."
                logging.critical(error)
                errors.append(error)

    errors = [x for x in errors if x is not None]
    return errors


def validate_dataframes(schema_file_map: dict) -> list[dict]:
    """
    Return list of human-readable validation error messages.
    """
    dataframes_status_dict_list = []
    for mut_table_fp, schema in schema_file_map.items():
        logging.info("Validating table %s against schema %s.", Path(mut_table_fp).name, schema["name"])
        df = read_table(mut_table_fp)

        table_errors = validate_single_dataframe(df, schema)
        if table_errors:
            for err in table_errors:
                logging.error("Validation error in %s: %s", Path(mut_table_fp).name, err)
                dataframes_status_dict_list.append(
                    {
                        "mutation_df": df,
                        "segment": schema["name"],
                        "errors": table_errors,
                        "validation_status": "Failed",
                    }
                )
        else:
            logging.info("Table %s passed validation.", Path(mut_table_fp).name)
            dataframes_status_dict_list.append(
                {
                    "mutation_df": df,
                    "mutation_table_fp": mut_table_fp,
                    "segment": schema["name"],
                    "errors": table_errors,
                    "validation_status": "Passed",
                }
            )
    return dataframes_status_dict_list


# ---------- Archiving & Replacement ----------
def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)


def copy_table(original_table_path: str, new_table_dir: str, new_filename: str | None = None) -> None:
    original_path = Path(original_table_path)
    new_table_dir = Path(new_table_dir)

    # Determine destination path (directory + optional new filename)
    dst_path = new_table_dir / new_filename if new_filename else new_table_dir / original_path.name

    # Perform copy
    dst = shutil.copy2(original_path, dst_path)

    # Compare sizes
    src_size = original_path.stat().st_size
    dst_size = dst.stat().st_size

    if dst_path.exists() and src_size == dst_size:
        logging.info("Copied %s to %s", original_table_path, dst_path)
    else:
        logging.critical("Failed to copy %s to %s", original_table_path, dst_path)


def update_tables(
    dataframes_status_dict_list: list[dict],
    tables_dir: str,
    archive_dir: str,
    user: str,
    log_file: str,
) -> None:
    """Update tables that have passed validation."""
    # Check if validation passed
    validated_tables = []
    for dataframes_status_dict in dataframes_status_dict_list:
        if dataframes_status_dict["validation_status"] == "Passed":
            validated_tables.append((dataframes_status_dict["mutation_table_fp"], dataframes_status_dict["segment"]))
        else:
            logging.warning("No tables passed validation. No updates made.")
            return False  # Exit function if no tables passed validation
    if validated_tables:
        # Check if directories exists
        for dir in [tables_dir, archive_dir]:
            if not Path(dir).exists():
                ensure_dir(dir)
                logging.info("Created directory: %s", dir)
                # Copy new file to tables directory
                for validated_table in validated_tables:
                    copy_table(validated_table[0], tables_dir)
                    logging.info("Copied %s to %s", validated_table[0], tables_dir)
                    return True
            else:
                logging.info("Directory exists: %s", dir)
                # Identify if segment table exists in tables_dir
                for validated_table in validated_tables:
                    segment_name = validated_table[1]
                    new_table_path = validated_table[0]
                    existing_seg_tables = list(Path(tables_dir).glob(f"{segment_name}*"))
                    if not existing_seg_tables:
                        copy_table(new_table_path, tables_dir)
                        logging.info("Copied %s to %s", new_table_path, tables_dir)
                        return True
                    else:
                        if len(existing_seg_tables) == 1:
                            # Move existing file to archive_dir/
                            logging.info("Archiving existing table(s) for segment: %s", segment_name)
                            date_str = dt.date.today().isoformat()
                            archive_file_name = "_".join([date_str, existing_seg_tables[0].name])
                            copy_table(existing_seg_tables[0], archive_dir, archive_file_name)
                            logging.info(
                                "Archived existing table %s to %s",
                                existing_seg_tables[0],
                                str(Path(archive_dir) / archive_file_name),
                            )
                            # Save new file to tables directory
                            copy_table(new_table_path, tables_dir)
                            logging.info("Copied %s to %s", new_table_path, tables_dir)
                            return True
                        else:
                            logging.critical(
                                "Multiple existing tables found for segment %s in %s. Please resolve manually.",
                                segment_name,
                                tables_dir,
                            )
                            return False


def archive_cleanup(archive_dir: str) -> None:
    """validated_files: list of (source_path, schema) tuples."""
    # Compute date folder
    archive_path = Path(archive_dir)

    # Check to see if there are more than 3 files with the same name
    files = list(archive_path.glob("*"))

    # Extract out unique file names, ignoring date prefixes
    # for each file remove the first YYYY-MM-DD_ part
    file_name_map: dict[str, list[Path]] = {}
    for f in files:
        fname = f.name
        # Remove date prefix
        match = re.match(r"^\d{4}-\d{2}-\d{2}_(.+)$", fname)
        core_name = match.group(1) if match else fname
        if core_name not in file_name_map:
            file_name_map[core_name] = []
        file_name_map[core_name].append(f)

    # For each unique file name, check if more than 3 exist
    for _, file_list in file_name_map.items():
        if len(file_list) > 3:
            # sort files by date prefix (oldest first)
            file_list.sort(key=lambda x: x.name)
            files_to_delete = file_list[:-3]
            # Delete oldest file
            for f in files_to_delete:
                try:
                    f.unlink()
                    logging.info("Deleted old archive file: %s", f)
                except Exception as e:
                    logging.error("Failed to delete archive file %s: %s", f, str(e))


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

    # Validate tables with schemas
    dataframes_status_dict_list = validate_dataframes(schema_file_map)

    # Save/Archive tables that passed validation
    update_status = update_tables(
        dataframes_status_dict_list, args.tables_dir, args.archive_dir, args.user, args.log_file
    )
    if update_status is True:
        print(f"Success. Tables validated and updated.\nLog: {args.log_file}")
    else:
        print(f"No tables were updated. Check log for details: {args.log_file}")

    # Clean up archive directory, if more than 3 files of the same segment name, deleted oldest (based on prefix)
    archive_cleanup(args.archive_dir)


if __name__ == "__main__":
    main()
