import datetime as dt
import subprocess
from pathlib import Path

import pytest

# Define location of folders
test_dir = Path(__file__).resolve().parent
repo_dir = test_dir / ".."
vau_script_dir = repo_dir / "src" / "mutation_table_updater"


# Test archive cleanup
@pytest.fixture
def setup_temp_dir(tmp_path):
    tables_dir = tmp_path / "tables"
    archive_dir = tmp_path / "archive"

    tables_dir.mkdir()
    archive_dir.mkdir()

    return tables_dir, archive_dir, tmp_path


def test_end_to_end(setup_temp_dir):
    tables_dir, archive_dir, tmp_path = setup_temp_dir

    subprocess.run(
        [
            "python",
            f"{vau_script_dir}/validate_and_update.py",
            f"--input {test_dir}/tables/",
            f"--schemas-dir {repo_dir}/schemas",
            f"--tables-dir {tables_dir}",
            f"--archive-dir {archive_dir}",
            "--user",
            "test_user",
            "--log-file",
            f"{tmp_path}/TESTING.log",
        ]
    )

    assert (tables_dir) / "ha_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "pb1_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "pb2_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "m_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "np_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "na_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "ns_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "pa_avian_influenza_mutation_table_gpha.tsv"


def test_end_to_end_archive(setup_temp_dir):
    tables_dir, archive_dir, tmp_path = setup_temp_dir

    subprocess.run(
        [
            "python",
            f"{vau_script_dir}/validate_and_update.py",
            f"--input {test_dir}/tables/",
            f"--schemas-dir {repo_dir}/schemas",
            f"--tables-dir {tables_dir}",
            f"--archive-dir {archive_dir}",
            "--user",
            "test_user",
            "--log-file",
            f"{tmp_path}/TESTING.log",
        ]
    )

    assert (tables_dir) / "ha_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "pb1_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "pb2_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "m_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "np_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "na_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "ns_avian_influenza_mutation_table_gpha.tsv"
    assert (tables_dir) / "pa_avian_influenza_mutation_table_gpha.tsv"

    assert (archive_dir) / f"{dt.date.today().isoformat()}_ha_avian_influenza_mutation_table_gpha.tsv"
    assert (archive_dir) / f"{dt.date.today().isoformat()}_pb1_avian_influenza_mutation_table_gpha.tsv"
    assert (archive_dir) / f"{dt.date.today().isoformat()}_pb2_avian_influenza_mutation_table_gpha.tsv"
    assert (archive_dir) / f"{dt.date.today().isoformat()}_m_avian_influenza_mutation_table_gpha.tsv"
    assert (archive_dir) / f"{dt.date.today().isoformat()}_np_avian_influenza_mutation_table_gpha.tsv"
    assert (archive_dir) / f"{dt.date.today().isoformat()}_na_avian_influenza_mutation_table_gpha.tsv"
    assert (archive_dir) / f"{dt.date.today().isoformat()}_ns_avian_influenza_mutation_table_gpha.tsv"
    assert (archive_dir) / f"{dt.date.today().isoformat()}_pa_avian_influenza_mutation_table_gpha.tsv"
