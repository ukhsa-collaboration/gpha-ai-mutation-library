import datetime as dt
import logging
from pathlib import Path

import pytest

from mutation_table_updater import validate_and_update as vau

# If you want to split up the tests between the schema and the tables neater, you could have some test classes and
# have the associated fixtures with it.

# If you wanted to do away with having so many files, just make them on the fly.
# Say you wanted to drop a col, just do that in the unit test, write it to a temp dir (use tmp_path builtin) and then
# give that to the function. The test is then very explicit about what it's doing in one place. There are examples of
# this in claspar - tests/test_virus.py and tests/test_setup.py where break dataframes by dropping columns or changing
# col names to check an exception catch works as expected.

########################### Fixtures ###########################
# You can just have constants:
SCRIPT_DIR = Path(__file__).resolve().parent


@pytest.fixture
def correct_ha_tsv():
    ha_tsv_fp = SCRIPT_DIR / "tables/correct_tables/ha_avian_influenza_mutation_table_gpha.tsv"
    return ha_tsv_fp


@pytest.fixture
def failed_fn_tsv():
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
    schemas_dir = SCRIPT_DIR / "schemas/good_schemas/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map


@pytest.fixture
def load_bad_schemas():
    schemas_dir = SCRIPT_DIR / "schemas/bad_schemas/bad_top_key_schema/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map


@pytest.fixture
def load_extra_colname_schemas():
    schemas_dir = SCRIPT_DIR / "schemas/bad_schemas/extra_columns_key_schema/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map


@pytest.fixture
def load_missing_colname_schemas():
    schemas_dir = SCRIPT_DIR / "schemas/bad_schemas/missing_columns_key_schema/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map


@pytest.fixture
def load_good_schemas():
    schemas_dir = SCRIPT_DIR / "schemas/good_schemas/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map


@pytest.fixture
def load_good_tables():
    tables_dir = SCRIPT_DIR / "tables/correct_tables/"
    tables = vau.list_candidate_files(tables_dir)
    return tables


@pytest.fixture
def table_missing_req_col():
    tsv_fp = SCRIPT_DIR / "tables/incorrect_tables/ha_avian_influenza_mutation_table_gpha_missing_required_column.tsv"
    df = vau.read_table(tsv_fp)
    return df


@pytest.fixture
def table_unexpected_col():
    tsv_fp = SCRIPT_DIR / "tables/incorrect_tables/ha_avian_influenza_mutation_table_gpha_unexp_column.tsv"
    df = vau.read_table(tsv_fp)
    return df


@pytest.fixture
def table_required_values_error():
    tsv_fp = SCRIPT_DIR / "tables/incorrect_tables/ha_avian_influenza_mutation_table_gpha_not_null_error.tsv"
    df = vau.read_table(tsv_fp)
    return df


@pytest.fixture
def int_type_check_error():
    tsv_fp = SCRIPT_DIR / "tables/incorrect_tables/ha_avian_influenza_mutation_table_gpha_int_error.tsv"
    df = vau.read_table(tsv_fp)
    return df


@pytest.fixture
def subtype_pattern_error():
    tsv_fp = SCRIPT_DIR / "tables/incorrect_tables/ha_avian_influenza_mutation_table_gpha_subtype_regex_check.tsv"
    df = vau.read_table(tsv_fp)
    return df


@pytest.fixture
def allowed_aa_values_error():
    tsv_fp = SCRIPT_DIR / "tables/incorrect_tables/ha_avian_influenza_mutation_table_gpha_incorrect_aa_value.tsv"
    df = vau.read_table(tsv_fp)
    return df


@pytest.fixture
def allowed_phenotypes_values_error():
    tsv_fp = SCRIPT_DIR / "tables/incorrect_tables/ha_avian_influenza_mutation_table_gpha_incorrect_pheno_value.tsv"
    df = vau.read_table(tsv_fp)
    return df


@pytest.fixture
def oob_met1_position_error():
    tsv_fp = SCRIPT_DIR / "tables/incorrect_tables/ha_avian_influenza_mutation_table_gpha_oob_met1_position.tsv"
    df = vau.read_table(tsv_fp)
    return df


@pytest.fixture
def oob_met1_min_position_error():
    tsv_fp = SCRIPT_DIR / "tables/incorrect_tables/ha_avian_influenza_mutation_table_gpha_oob_met1_low_position.tsv"
    df = vau.read_table(tsv_fp)
    return df


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


def test_check_correct_filename(correct_ha_tsv):
    """Test to check correct filename are handled correctly"""
    # Capture warnings

    segs = ["pb2", "pb1", "ha", "m", "na", "np", "ns", "pa"]

    check_fn_return = vau.check_filename(correct_ha_tsv, segs)

    assert check_fn_return is True


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


def test_correct_map_schema_file(load_good_tables, load_good_schemas):
    """Test mapping of schema to file"""
    # Result is dict with all files and schemas mapped and an empty skipped_files list
    segs = ["pb2", "pb1", "ha", "m", "na", "np", "ns", "pa"]
    file_schema_map, skipped_files = vau.map_schema_to_file(load_good_tables, load_good_schemas, segs)

    # check file in keys
    for f in load_good_tables:
        assert f in file_schema_map

    # Check no missing files
    assert skipped_files == []


# Looks like many of the tests below have the same structure - call the fixture, put it and the HA schema into the
# function to test, assert that the output message is in the errors list. These lend themselves to parametrisation -
# essentially a for loop built into Pytest that iterates over tuples of arguments you set which get passed to the test
# function. Because some of these arguments are fixtures, you have to request the fixture (first line in the test).
# This means that there is less duplicated code, it's easier to read (theoretically) and adding new test cases is easy.


@pytest.mark.parametrize(
    "description,input,errormsg,schemadir",
    [
        (
            "Test unexpected column is identified",
            "table_unexpected_col",
            "Unexpected columns present: unexpected_column",
            SCRIPT_DIR / "schemas/good_schemas/",
        ),
        (
            "Test required non-null values are identified",
            "table_required_values_error",
            "feature_type: 1 required values are null",
            SCRIPT_DIR / "schemas/good_schemas/",
        ),
        (
            "Test int type violations are identified",
            "int_type_check_error",
            "position_met1: 1 rows fail type 'int'",
            SCRIPT_DIR / "schemas/good_schemas/",
        ),
        (
            "Test regex pattern violations are identified",
            "subtype_pattern_error",
            "subtype_tested: 1 rows fail regex '^H[0-9]+N[0-9]+$'",
            SCRIPT_DIR / "schemas/good_schemas/",
        ),
        (
            "Test allowed values are used in column",
            "allowed_aa_values_error",
            "Column ref_AA has incorrect value(s): 1",
            SCRIPT_DIR / "schemas/good_schemas/",
        ),
        (
            "Test allowed values are allowed phenotypes",
            "allowed_phenotypes_values_error",
            "Column phenotypic_category has incorrect value(s): incorrect_value",
            SCRIPT_DIR / "schemas/good_schemas/",
        ),
        (
            "Test high out-of-bounds values are identified",
            "oob_met1_position_error",
            "Value outside bounds (1 - 568) for column 'position_met1'. check values: [999]",
            SCRIPT_DIR / "schemas/good_schemas/",
        ),
        (
            "Test low out-of-bounds values are identified",
            "oob_met1_min_position_error",
            "Value outside bounds (1 - 568) for column 'position_met1'. check values: [0]",
            SCRIPT_DIR / "schemas/good_schemas/",
        ),
    ],
)
def test_table_integrity(description, input, errormsg, schemadir, correct_ha_schema, request):
    df = request.getfixturevalue(input)
    schema = correct_ha_schema

    errors = vau.validate_single_dataframe(df, schema, schemadir)
    print(errors)
    errors_cleaned = [x for x in errors if x is not None]
    print(errors_cleaned)
    error_messages = [msg for level, msg in errors_cleaned]
    # Check error message for unexpected column
    assert errormsg in error_messages, f"Expected {errormsg}, got {errors}"


def test_correct_ha_table_validation(correct_ha_df, correct_ha_schema):
    df = correct_ha_df
    schema = correct_ha_schema
    schemadir = SCRIPT_DIR / "schemas/good_schemas/"
    errors = vau.validate_single_dataframe(df, schema, schemadir)
    errors_cleaned = [x for x in errors if x is not None]
    assert not errors_cleaned


# To test saving archiving:
"""
1. No existing table or archive directories
2. Existing table directory, no archive directory
3. Existing archive directory, no table directory
4. Both existing table and archive directories:
    4.1 New table is different from existing tables
    4.2 New table is the same as existing tables
? Test if copy fails

- How do I create temporary dirs and files for these tests?
"""
# Create temporary dirs and files


@pytest.fixture
def setup_empty_dirs(tmp_path):
    tables_dir = tmp_path / "tables"
    archive_dir = tmp_path / "archive"
    return tables_dir, archive_dir


# Test copying new table when no existing table
def test_save_new_table_no_existing_table(setup_empty_dirs, correct_ha_tsv):
    tables_dir, archive_dir = setup_empty_dirs

    update_status = vau.update_tables(
        [{"validation_status": "Passed", "mutation_table_fp": correct_ha_tsv, "segment": "ha"}], tables_dir, archive_dir
    )

    assert update_status is True


@pytest.fixture
def setup_existing_table_dirs(tmp_path):
    tables_dir = tmp_path / "tables"
    archive_dir = tmp_path / "archive"

    tables_dir.mkdir()
    archive_dir.mkdir()

    ha_placeholder = tables_dir / "ha_avian_influenza_mutation_table_gpha.tsv"

    ha_placeholder.write_text("dummy content")

    return tables_dir, archive_dir


# Test when there is a file present
# Test copying new table when no existing table
def test_save_new_table_with_existing_table(setup_existing_table_dirs, correct_ha_tsv):
    tables_dir, archive_dir = setup_existing_table_dirs

    update_status = vau.update_tables(
        [{"validation_status": "Passed", "mutation_table_fp": correct_ha_tsv, "segment": "ha"}], tables_dir, archive_dir
    )

    # expected files
    expected_output_file = tables_dir / "ha_avian_influenza_mutation_table_gpha.tsv"
    expected_archive_file = archive_dir / f"{dt.date.today().isoformat()}_ha_avian_influenza_mutation_table_gpha.tsv"

    assert update_status is True
    assert expected_archive_file.exists()
    assert expected_output_file.exists()


# Test archive cleanup
@pytest.fixture
def setup_full_archive(tmp_path):
    tables_dir = tmp_path / "tables"
    archive_dir = tmp_path / "archive"

    tables_dir.mkdir()
    archive_dir.mkdir()

    ha_placeholder = tables_dir / "ha_avian_influenza_mutation_table_gpha.tsv"

    archive_placeholder_1 = tables_dir / "2026-01-01_ha_avian_influenza_mutation_table_gpha.tsv"
    archive_placeholder_2 = tables_dir / "2026-01-02_ha_avian_influenza_mutation_table_gpha.tsv"
    archive_placeholder_3 = tables_dir / "2026-01-03_ha_avian_influenza_mutation_table_gpha.tsv"

    ha_placeholder.write_text("dummy content")
    archive_placeholder_1.write_text("dummy content")
    archive_placeholder_2.write_text("dummy content")
    archive_placeholder_3.write_text("dummy content")

    return tables_dir, archive_dir


# Test archive cleanup
def test_archive_cleanup(setup_full_archive, correct_ha_tsv):
    tables_dir, archive_dir = setup_full_archive

    update_status = vau.update_tables(
        [{"validation_status": "Passed", "mutation_table_fp": correct_ha_tsv, "segment": "ha"}], tables_dir, archive_dir
    )

    # expected files
    expected_output_file = tables_dir / "ha_avian_influenza_mutation_table_gpha.tsv"
    expected_archive_file = archive_dir / f"{dt.date.today().isoformat()}_ha_avian_influenza_mutation_table_gpha.tsv"
    delected_archive_file = archive_dir / "2026-01-01_ha_avian_influenza_mutation_table_gpha.tsv"

    assert update_status is True
    assert expected_output_file.exists()
    assert expected_archive_file.exists()
    assert delected_archive_file.exists() is False
