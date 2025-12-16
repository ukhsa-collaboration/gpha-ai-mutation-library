import pytest
import pandas as pd
from pathlib import Path
from mutation_table_updater import validate_and_update as vau


## Fixtures
@pytest.fixture
def correct_ha_tsv():
    SCRIPT_DIR = Path(__file__).resolve().parent
    ha_tsv_fp = SCRIPT_DIR / "tables/ha_correct_test.tsv"
    return ha_tsv_fp

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

## Tests
def test_find_schema_for_file(load_schemas, correct_ha_tsv):
    """ Test that the schema file is retrieved. """
    schema = vau.find_schema_for_file(load_schemas, correct_ha_tsv)
    assert schema.get('name')

def test_validate_dataframe(correct_ha_df, correct_ha_schema):
    errs = vau.validate_dataframe(correct_ha_df, correct_ha_schema)
    assert errs == []

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