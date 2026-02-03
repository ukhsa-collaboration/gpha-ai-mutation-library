# gpha-ai-mutation-library
Library stores tables for avian influenza mutations of concern.

## Purpose
A utility that provides tables describing reviewed avian influenza mutations, in tsv format, for use in downstream applications.
This utility maintained current tables, as well as archived tables.
There is a validation script that takes a new table(s), checks that the new data meets data requirements, and creates an updating table, whilst moving the original table into an archive with an appropriate datestamp. This repository maintains three archived table sets.

## Repo Layout
```
├── LICENSE
├── pyproject.toml
├── README.md
├── schemas
│   ├── ha_avian_influenza_mutation_table_gpha.yml
│   ├── m_avian_influenza_mutation_table_gpha.yml
│   ├── na_avian_influenza_mutation_table_gpha.yml
│   ├── np_avian_influenza_mutation_table_gpha.yml
│   ├── ns_avian_influenza_mutation_table_gpha.yml
│   ├── pa_avian_influenza_mutation_table_gpha.yml
│   ├── pb1_avian_influenza_mutation_table_gpha.yml
│   ├── pb2_avian_influenza_mutation_table_gpha.yml
│   └── reference_lists
│       ├── aa_list.txt
│       ├── feature_type_list.txt
│       ├── host_type_list.txt
│       ├── phenotypic_categories_list.txt
│       └── segment_list.txt
├── src
│   └── mutation_table_updater
│       ├── __init__.py
│       └── validate_and_update.py
├── tables
│   ├── ha_avian_influenza_mutation_table_gpha.tsv
│   ├── m_avian_influenza_mutation_table_gpha.tsv
│   ├── na_avian_influenza_mutation_table_gpha.tsv
│   ├── np_avian_influenza_mutation_table_gpha.tsv
│   ├── ns_avian_influenza_mutation_table_gpha.tsv
│   ├── pa_avian_influenza_mutation_table_gpha.tsv
│   ├── pb1_avian_influenza_mutation_table_gpha.tsv
│   └── pb2_avian_influenza_mutation_table_gpha.tsv
├── tests[...]
├── archive[...]
└── updates.log

[...] - Not shown for berevity
```
## Usage
### Accessing Tables
Tables should be read directly from GitHub with an appropriate URL i.e
```
wget https://github.com/ukhsa-collaboration/gpha-ai-mutation-library/tree/main/tables/
```
### Updating Tables
The validation script will:
    - Updates logs
    - Take in either a table or folder containing tables. 
    - Checks that the column headers are approriate
    - Checks expected data in columns meets requirements
    - If QC checks passed, archives original table(s)
    - Creates new table in main directory

#### Validate & update from a folder containing multiple tables
```
python \  
    src/mutation_table_updater/validate_and_update.py \  
    --input <TABLE_DIR>_avian_influenza_mutation_table.tsv \  
    --user <USERNAME>  
```

#### Validate & update a single file
```
python \  
    src/mutation_table_updater/validate_and_update.py \  
    --input <SEGMENT_ID>_avian_influenza_mutation_table.tsv \  
    --user <USERNAME>  
```