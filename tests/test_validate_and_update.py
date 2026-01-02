from _pytest.logging import caplog
import pytest
import pandas as pd
from pathlib import Path
from mutation_table_updater import validate_and_update as vau
import logging


## Fixtures
@pytest.fixture
def correct_ha_tsv():
    SCRIPT_DIR = Path(__file__).resolve().parent
    ha_tsv_fp = SCRIPT_DIR / "tables/ha_correct_test.tsv"
    return ha_tsv_fp

@pytest.fixture
def failed_fn_tsv():
    SCRIPT_DIR = Path(__file__).resolve().parent
    failed_tsv_fp = SCRIPT_DIR / "tables/incorrect_fn_test.tsv"
    return failed_tsv_fp

@pytest.fixture
def load_schemas():
    SCRIPT_DIR = Path(__file__).resolve().parent
    schemas_dir = SCRIPT_DIR / "../schemas/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map

@pytest.fixture
def correct_ha_df(correct_ha_tsv):
    df = vau.read_table(correct_ha_tsv)
    return df

@pytest.fixture
def correct_ha_schema(load_schemas, correct_ha_tsv):
    schema = vau.find_schema_for_file(load_schemas, correct_ha_tsv)
    return schema

@pytest.fixture
def load_bad_schemas():
    SCRIPT_DIR = Path(__file__).resolve().parent
    schemas_dir = SCRIPT_DIR / "bad_schemas/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map

@pytest.fixture
def load_good_schemas():
    SCRIPT_DIR = Path(__file__).resolve().parent
    schemas_dir = SCRIPT_DIR / "good_schemas/"
    schemas_map = vau.load_schemas(schemas_dir)
    return schemas_map

## Tests
def test_find_schema_for_file(load_schemas, correct_ha_tsv):
    """ Test that the schema file is retrieved. """
    schema = vau.find_schema_for_file(load_schemas, correct_ha_tsv)
    assert schema.get('name')

def test_validate_dataframe(correct_ha_df, correct_ha_schema):
    errs = vau.validate_dataframe(correct_ha_df, correct_ha_schema)
    assert errs == []

def test_check_incorrect_filename(failed_fn_tsv, caplog):
    """ Test to check incorrect filename are handled correctly"""
    # Capture warnings
    
    caplog.set_level(logging.WARNING, logger=__name__)

    segs = ['pb2','pb1','ha','m','na','np','ns','pa']

    check_fn_return = vau.check_filename(failed_fn_tsv, segs)

    # Expected warning message
    warning_message = "Input File %s did not start with a segment ID (%s). Skipped." % (
        Path(failed_fn_tsv).name, ", ".join(segs)
    )

    # # Assert warning was raised for inappropriate filename
    assert any(
            rec.levelno == logging.WARNING and str(warning_message) in rec.message
            for rec in caplog.records
        )

    assert check_fn_return is False

def test_check_correct_filename(correct_ha_tsv):
    """ Test to check correct filename are handled correctly"""
    # Capture warnings
    
    segs = ['pb2','pb1','ha','m','na','np','ns','pa']

    check_fn_return = vau.check_filename(correct_ha_tsv, segs)

    assert check_fn_return is True


# def test_validate_schemas_correct(load_schemas, correct_ha_tsv):
#     """ Test schema contains the right information """
#     schemas_map  = vau.validate_schemas(load_schemas)

#     # what fields are we expecting
#     # keys: name, filname, strict_columns, primary_key, columns
#     # Primary Key should container: "feature_type", "combination_name", "combination_id", "segment", "position_met1", "position_H5", "position_H3", "ref_AA", "alt_AA", "name", "subtype_tested", "subtype_notes", "phenotypic_consequences", "phenotypic_category", "concern_score", "confidence_score", "phenotypic_effect", "host_type", "host_taxon", "source", "references", "PMID", "additional_comments", "gpha_background", "gpha_known_muts"
#     # columns, each item should contain: 'name', 'type', 'required'

#     pass

def test_incorrect_main_key_schema_file(load_bad_schemas):
    """ Test schema contains the right information """
    schemas_map  = vau.validate_schemas(load_bad_schemas)
    assert vau.validate_schemas(schemas_map) is False

def test_correct_main_key_schema_file(load_good_schemas):
    """ Test schema contains the right information """
    schemas_map  = vau.validate_schemas(load_bad_schemas)
    assert vau.validate_schemas(schemas_map) is True


def test_find_schema_for_file(load_schemas, correct_ha_tsv):
    """ Test the correct schema is found for a file """
    sch  = vau.find_schema_for_file(load_schemas, correct_ha_tsv)
    
    assert sch['name'] == 'ha'

def test_incorrect_find_schema_for_file(load_schemas, correct_ha_tsv):
    """ Test the correct schema is found for a file """
    sch  = vau.find_schema_for_file(load_schemas, correct_ha_tsv)
    
    assert sch['name'] not in ['pb2','pb1', 'm','na','np','ns','pa']


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