import pytest
from pathlib import Path
from mutation_table_updater import validate_and_update as vau

test_ha_correct_path = 'tests/tables/ha_correct_test.tsv'


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

def test_find_schema_for_file(schemas):
    ''' Test reading of tsv files '''
    schema = vau.find_schema_for_file(str(test_ha_correct_path), schemas)
    print(schema)