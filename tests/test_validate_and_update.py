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
    "description,input,errormsg",
    [
        (
            "Test unexpected column is identified",
            "table_unexpected_col",
            "Unexpected columns present: unexpected_column",
        ),
        (
            "Test required non-null values are identified",
            "table_required_values_error",
            "feature_type: 1 required values are null",
        ),
        ("Test int type violations are identified", "int_type_check_error", "position_met1: 1 rows fail type 'int'"),
        (
            "Test regex pattern violations are identified",
            "subtype_pattern_error",
            "subtype_tested: 1 rows fail regex '^H[0-9]+N[0-9]+$'",
        ),
        (
            "Test allowed values are used in column",
            "allowed_aa_values_error",
            "ref_AA: 1 row(s) contain values not present in the reference file (check schemas/reference_lists/).",
        ),
        (
            "Test allowed values are allowed phenotypes",
            "allowed_phenotypes_values_error",
            (
                "phenotypic_category: 1 row(s) contain values not present in the reference file "
                "(check schemas/reference_lists/)."
            ),
        ),
        (
            "Test high out-of-bounds values are identified",
            "oob_met1_position_error",
            "Value outside bounds (1 - 568) for column 'position_met1'. check values: [999]",
        ),
        (
            "Test low out-of-bounds values are identified",
            "oob_met1_min_position_error",
            "Value outside bounds (1 - 568) for column 'position_met1'. check values: [0]",
        ),
    ],
)
def test_table_integrity(description, input, errormsg, correct_ha_schema, request):
    df = request.getfixturevalue(input)
    schema = correct_ha_schema

    errors = vau.validate_single_dataframe(df, schema)

    # Check error message for unexpected column
    assert errormsg in errors, f"Expected {errormsg}, got {errors}"


# --> Everything from here and below, to next arrow can be deleted, it's captured in the parametrised test above.
# Per ccolumn checks
def test_unexpected_column(table_unexpected_col, correct_ha_schema):
    """Test unexpected column is identified"""
    df = table_unexpected_col
    schema = correct_ha_schema

    errors = vau.validate_single_dataframe(df, schema)

    # Check error message for unexpected column
    expected_error_msg = "Unexpected columns present: unexpected_column"

    assert expected_error_msg in errors


# Required non-Null
def test_required_values_error(table_required_values_error, correct_ha_schema):
    """Test required non-null values are identified"""
    df = table_required_values_error

    schema = correct_ha_schema

    errors = vau.validate_single_dataframe(df, schema)

    # Check error message for required non-null violation
    expected_error_msg = "feature_type: 1 required values are null"

    assert expected_error_msg in errors


# Int Type Check
def test_int_type_check_error(int_type_check_error, correct_ha_schema):
    """Test int type violations are identified"""
    df = int_type_check_error

    schema = correct_ha_schema

    errors = vau.validate_single_dataframe(df, schema)

    # Check error message for int type violation
    expected_error_msg = "position_met1: 1 rows fail type 'int'"

    assert expected_error_msg in errors


# # Str Type Check
## TODO: Add test to ensure string type is used for column


# Regex Check
def test_regex_subtype_pattern_check(subtype_pattern_error, correct_ha_schema):
    """Test regex pattern violations are identified"""
    df = subtype_pattern_error

    schema = correct_ha_schema

    errors = vau.validate_single_dataframe(df, schema)

    # Check error message for regex pattern violation
    expected_error_msg = "subtype_tested: 1 rows fail regex '^H[0-9]+N[0-9]+$'"

    assert expected_error_msg in errors


# Allowed Values (external file)
def test_allowed_aa_values_check(allowed_aa_values_error, correct_ha_schema):
    """Test allowed values are used in column"""
    df = allowed_aa_values_error
    schema = correct_ha_schema

    errors = vau.validate_single_dataframe(df, schema)

    expected_error_msg = (
        "ref_AA: 1 row(s) contain values not present in the reference file (check schemas/reference_lists/)."
    )

    assert expected_error_msg in errors


def test_allowed_pheno_values_check(allowed_phenotypes_values_error, correct_ha_schema):
    """Test allowed values are used in column"""
    df = allowed_phenotypes_values_error
    schema = correct_ha_schema

    errors = vau.validate_single_dataframe(df, schema)

    expected_error_msg = "phenotypic_category: 1 row(s) contain values not present in the reference file (check schemas/reference_lists/)."

    assert expected_error_msg in errors


# Allowed Values (inline)
def test_oob_high_int_check(oob_met1_position_error, correct_ha_schema):
    """Test out-of-bounds values are identified"""
    df = oob_met1_position_error
    schema = correct_ha_schema

    errors = vau.validate_single_dataframe(df, schema)

    expected_error_msg = "Value outside bounds (1 - 568) for column 'position_met1'. check values: [999]"

    assert expected_error_msg in errors


def test_oob_low_int_check(oob_met1_min_position_error, correct_ha_schema):
    """Test out-of-bounds values are identified"""
    df = oob_met1_min_position_error
    schema = correct_ha_schema

    errors = vau.validate_single_dataframe(df, schema)

    expected_error_msg = "Value outside bounds (1 - 568) for column 'position_met1'. check values: [0]"

    assert expected_error_msg in errors


# --> Everything from here upwards to next arrow can be deleted, it's captured in the parametrised test above.


def test_correct_ha_table_validation(correct_ha_df, correct_ha_schema):
    df = correct_ha_df
    schema = correct_ha_schema
    errors = vau.validate_single_dataframe(df, schema)
    assert not errors


# Test dataframe validation
"""
what am i trying to test?
- things are archived appropriately
- Logs are appropriate

"""
