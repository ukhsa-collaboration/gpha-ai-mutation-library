import pytest
import pandas as pd
import subprocess

def test_end_to_end():
    subprocess([
        'python',
        'scripts/validate_and_update.py',
        '--input tests/tables/ha_correct_test.csv',
        '--user', "<USERNAME>",
        '--log-file', "<TESTING.log>"
        ])

    expected = pd.read_csv('tests/tables/expected_ha_correct_test.csv')
    actual = pd.read_csv('whatever the output file is that the command made')

    assert actual == expected, f'end to end test failed, got {actual} but expected {expected}'
    print(f'End to end test - actual did match expected {actual}')
 