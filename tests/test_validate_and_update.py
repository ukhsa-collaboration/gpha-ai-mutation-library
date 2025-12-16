import pytest
from pathlib import Path
from mutation_table_updater import validate_and_update as vau
@pytest.fixture
def correct_ha_tsv():
    SCRIPT_DIR = Path(__file__).resolve().parent
    ha_tsv_fp = SCRIPT_DIR / "tables/ha_correct_test.tsv"
    return ha_tsv_fp

@pytest.fixture
def ha_schema_yaml():
    SCRIPT_DIR = Path(__file__).resolve().parent
    ha_yml = SCRIPT_DIR / "../schemas/ha_avian_influenza_mutation_table_gpha.yml"
    schema = vau.load_schemas(ha_yml)
    return schema

def test_find_schema_for_file(correct_ha_tsv, schema):
    ''' Test reading of tsv files '''
    schema = vau.find_schema_for_file(correct_ha_tsv, schema)
    print(schema)