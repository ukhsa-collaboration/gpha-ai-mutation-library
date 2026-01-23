import logging
from pathlib import Path

import pytest

from mutation_table_updater import validate_and_update as vau


########################### Fixtures ###########################
@pytest.fixture
def correct_ha_tsv():
    SCRIPT_DIR = Path(__file__).resolve().parent
    ha_tsv_fp = SCRIPT_DIR / "tables/correct_tables/ha_avian_influenza_mutation_table_gpha.tsv"
    return ha_tsv_fp


@pytest.fixture
def failed_fn_tsv():
    SCRIPT_DIR = Path(__file__).resolve().parent
    failed_tsv_fp = SCRIPT_DIR / "tables/incorrect_tables/incorrect_fn_test.tsv"
    return failed_tsv_fp


@pytest.fixture
def correct_ha_df(correct_ha_tsv):
    df = vau.read_table(correct_ha_tsv)
    return df


@pytest.fixture
def correct_ha_schema(load_schemas, correct_ha_tsv):
    schema = vau.find_schema_for_file(load_schemas, correct_ha_tsv)
    return schema


@pytest.fixture
def load_schemas():
    SCRIPT_DIR = Path(__file__).resolve().parent
    schemas_dir = SCRIPT_DIR / "../schemas/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map


@pytest.fixture
def load_bad_schemas():
    SCRIPT_DIR = Path(__file__).resolve().parent
    schemas_dir = SCRIPT_DIR / "schemas/bad_schemas/bad_top_key_schema/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map


@pytest.fixture
def load_extra_colname_schemas():
    SCRIPT_DIR = Path(__file__).resolve().parent
    schemas_dir = SCRIPT_DIR / "schemas/bad_schemas/extra_columns_key_schema/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map


@pytest.fixture
def load_missing_colname_schemas():
    SCRIPT_DIR = Path(__file__).resolve().parent
    schemas_dir = SCRIPT_DIR / "schemas/bad_schemas/missing_columns_key_schema/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map


@pytest.fixture
def load_good_schemas():
    SCRIPT_DIR = Path(__file__).resolve().parent
    schemas_dir = SCRIPT_DIR / "schemas/good_schemas/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map


@pytest.fixture
def load_good_tables():
    SCRIPT_DIR = Path(__file__).resolve().parent
    schemas_dir = SCRIPT_DIR / "tables/correct_tables/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map


########################### Tests ###########################


## Schema tests
def test_find_schema_for_file(load_schemas, correct_ha_tsv):
    """Test that the schema file is retrieved."""
    schema = vau.find_schema_for_file(load_schemas, correct_ha_tsv)
    assert schema.get("name")


def test_find_correct_schema_for_file(load_schemas, correct_ha_tsv):
    """Test the correct schema is found for a file"""
    sch = vau.find_schema_for_file(load_schemas, correct_ha_tsv)

    assert sch["name"] == "ha"


def test_validate_dataframe(correct_ha_df, correct_ha_schema):
    errs = vau.validate_dataframe(correct_ha_df, correct_ha_schema)
    assert errs == []


def test_incorrect_main_key_schema_file(load_bad_schemas, caplog):
    """Test schema contains the right information"""
    schema_val_status = vau.validate_schemas(load_bad_schemas)

    assert any(rec.levelno == logging.CRITICAL for rec in caplog.records)

    assert schema_val_status is False


def test_extra_column_name_in_schema(load_extra_colname_schemas, caplog):
    """Test schema primary key contains all column names"""
    schema_val_status = vau.validate_schemas(load_extra_colname_schemas)

    assert any(rec.levelno == logging.CRITICAL for rec in caplog.records)

    assert schema_val_status is False


def test_missing_column_name_in_schema(load_missing_colname_schemas, caplog):
    """Test schema primary key contains all column names"""
    schema_val_status = vau.validate_schemas(load_missing_colname_schemas)

    assert any(rec.levelno == logging.CRITICAL for rec in caplog.records)

    assert schema_val_status is False


def test_correct_main_key_schema_file(load_good_schemas):
    """Test schema contains the right information"""
    schema_val_status = vau.validate_schemas(load_good_schemas)
    assert schema_val_status is True


def test_incorrect_find_schema_for_file(load_schemas, correct_ha_tsv):
    """Test the correct schema is found for a file"""
    sch = vau.find_schema_for_file(load_schemas, correct_ha_tsv)

    assert sch["name"] not in ["pb2", "pb1", "m", "na", "np", "ns", "pa"]


def test_correct_map_schame_file(files, schemas):
    """Test mapping of schema to file"""
    # All segment files
    # All segment schemas
    # Result is dict with all files and schemas mapped and an empty skipped_files list
    file_schema_map = vau.map_schemas_to_files(files, schemas)

    for f in files:
        assert f in file_schema_map
        assert file_schema_map[f]["name"] in ["pb2", "pb1", "ha", "m", "na", "np", "ns", "pa"]


## Test names of files
def test_check_incorrect_filename(failed_fn_tsv, caplog):
    """Test to check incorrect filename are handled correctly"""
    # Capture warnings

    caplog.set_level(logging.WARNING, logger=__name__)

    segs = ["pb2", "pb1", "ha", "m", "na", "np", "ns", "pa"]

    check_fn_return = vau.check_filename(failed_fn_tsv, segs)

    # Expected warning message
    warning_message = "Input File {} did not start with a segment ID ({}). Skipped.".format(
        Path(failed_fn_tsv).name,
        ", ".join(segs),
    )

    # # Assert warning was raised for inappropriate filename
    assert any(rec.levelno == logging.WARNING and str(warning_message) in rec.message for rec in caplog.records)

    assert check_fn_return is False


def test_check_correct_filename(correct_ha_tsv):
    """Test to check correct filename are handled correctly"""
    # Capture warnings

    segs = ["pb2", "pb1", "ha", "m", "na", "np", "ns", "pa"]

    check_fn_return = vau.check_filename(correct_ha_tsv, segs)

    assert check_fn_return is True


# Test dataframe validation
"""
what am i trying to test?
- the correct schemas are read in per column?
- incorrect data is identfied as expected
- error messages are reported appropriately
    - if you provide a file that is inappropriately formatted provide appropriate feedback
- things are archived appropriately
- Logs are appropriate

"""
