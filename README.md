# gpha-ai-mutation-library
Library stores and updates tables for avian influenza mutations of interest.

## Purpose
A utility that provides tables describing avian influenza mutations, in tsv format, for use in downstream applications.

There is a validation script that takes a new table(s), checks that the new data meets data requirements specified in the schema, and creates saves new tables, whilst moving the original table into an archive with an appropriate datestamp. This repository maintains three archived table sets.

## Installation
Clone repo and create environment:
`git clone git@github.com:ukhsa-collaboration/gpha-ai-mutation-library.git`

`conda env create -n gpha-ai-mutation-library`  
`conda activate gpha-ai-mutation-library`


Installation for users:
```
cd gpha-ai-mutation-library
pip install .
```

Installation for developers (installs code in editable mode):
```
cd gpha-ai-mutation-library
pip install --editable '.[dev]'
```

## Usage
### Accessing Tables
Tables should be read directly from GitHub with an appropriate URL i.e
```
wget https://github.com/ukhsa-collaboration/gpha-ai-mutation-library/tree/main/tables/
```

The original tables can be found in the GPHA SharePoint folder: /Projects/Avian_Flu/mutations/

### Updating Tables
The validation script will:
    - Updates logs
    - Take in either a table or folder containing tables. 
    - Checks that the column headers are approriate
    - Checks expected data in columns meets requirements
    - If QC checks passed, archives original table(s)
    - Creates new table in main directory

#### Usage
To get usage instructions: `mutation_table_updater --help`

```
usage: mutation_table_updater [-h] --input INPUT [--tables-dir TABLES_DIR] [--archive-dir ARCHIVE_DIR] [--schemas-dir SCHEMAS_DIR] [--log-file LOG_FILE]
                              [--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}] [--user USER]
```

#### Validate & update from a folder containing multiple tables
```
mutation_table_updater --input <TABLE_DIR> --user <USERNAME>  
```

#### Validate & update a single file
```
mutation_table_updater --input <SEGMENT_ID>_avian_influenza_mutation_table.tsv --user <USERNAME>  
```


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

[...] - Not shown for brevity
```