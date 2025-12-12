# gpha-ai-mutation-library
Library stores tables for avian influenza mutations of concern.

## Purpose
A utility that provides tables describing reviewed avian influenza mutations, in tsv format, for use in downstream applications.
This utility maintained current tables, as well as archived tables.
There is a validation script that takes a new table(s), checks that the new data meets data requirements, and creates an updating table, whilst moving the original table into an archive with an appropriate datestamp. This repository maintains three archived table sets.

## Repo Layout
|
|- README
|- LICENSE
|- table_validation.py
|- archived |- <DATE_1> |- ha_mutation_table.csv
                        |- pb1_mutation_table.csv
                        |- pb2_mutation_table.csv
                        |- na_mutation_table.csv
                        |- ns_mutation_table.csv
                        |- np_mutation_table.csv
                        |- m_mutation_table.csv
                        |- pa_mutation_table.csv
|- ha_mutation_table.csv
|- pb1_mutation_table.csv
|- pb2_mutation_table.csv
|- na_mutation_table.csv
|- ns_mutation_table.csv
|- np_mutation_table.csv
|- m_mutation_table.csv
|- pa_mutation_table.csv
|- updates.log

## Usage
### Accessing Tables
Tables should be read directly from GitHub with an appropriate URL i.e.
``
### Updating Tables
The validation script will:
    - Updates logs
    - Take in either a table or folder containing tables. 
    - Checks that the column headers are approriate
    - Checks expected data in columns meets requirements
    - If QC checks passed, archives original table(s)
    - Creates new table in main directory