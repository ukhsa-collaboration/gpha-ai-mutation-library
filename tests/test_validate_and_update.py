import pytest
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

## Tests
def test_find_schema_for_file(load_schemas, correct_ha_tsv):
    print("OUTPUTS")
    schema = vau.find_schema_for_file(load_schemas(), correct_ha_tsv())
    print(schema)
    