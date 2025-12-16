import pytest
from pathlib import Path
from mutation_table_updater import validate_and_update as vau


## Fixtures
@pytest.fixture
def correct_ha_tsv():
    SCRIPT_DIR = Path(__file__).resolve().parent
    ha_tsv_fp = SCRIPT_DIR / "tables/ha_correct_test.tsv"
    return str(ha_tsv_fp)

@pytest.fixture
def schemas():
    SCRIPT_DIR = Path(__file__).resolve().parent
    schemas_fp = SCRIPT_DIR / "../schemas/"
    schemas = vau.load_schemas(schemas_fp)
    return schemas

## Tests
def test_find_schema_for_file(schemas, correct_ha_tsv):
    ''' Test reading of tsv files '''
    schema_match = vau.find_schema_for_file(schemas, correct_ha_tsv)
    assert schema_match['name'] == 'ha'