# Kraft Heinz Solution Assesment
This repository contains the solution audit review artifacts for the Kraft Heinz team.

## Installation and Setup

1. Create a `.env` file and fill-in the environment variables as shown in the `example.env`

2. Install the libraries listed in `requirements.txt`.
```bash
pip install -r requirements.txt
```

## Example python notebooks
The `hierarchy_queries` directory contains the notebook that executes and retrieves parent/child hierarchies:
* [query_test.ipynb](hierarchy_queries/query_test.ipynb)
    * Executes the cypher queries for extracting parent/child hierarchies
* [config.yaml](hierarchy_queries/config.yaml)
    * Cypher queries used in the notebook are extracted from this file, it contains 3 example queries:
        * **original_query** - original cypher provided by the kraft team.
        * **spanning_tree_only** - does the hierarchy expansion using apoc.path.spanningTree.
        * **qpp_query_only_nodes** - does the hierarchy expansion using qpp and only returning nodes as a list.

The `json_export` directory contains the python scripts that execute the json export using apoc:
* [export_many_files.py](json_export/export_many_files.py)
    * Iterates through each node label and relationship type and exports each label or type to a json lines file.
    * Data is now incrementally exported as opposed to bulk exported.
    * We take advantage of the multicore CPU of the AuraDB instance and run the export job in four parallel threads (This may be increased to six according to Kraft’s AuraDB instance specifications).
    * We batch each export transaction in chunks of 200 rows.
* [export_2_files.py](json_export/export_2_files.py)
    * Perform same process as above, however it will export all nodes into a file and all relationships into a file.
    * Process runs in under 10 minutes.

* [validate_data_export.ipynb](json_export/validate_data_export.ipynb)

    * The data exports are validated by retrieving the node label and relationship type counts from the AuraDB instance and comparing them to the line counts of their respective files. This is automatically completed by the validate_data_export.ipynb Python notebook. 

The exported data can be found in the directory `exports/*.`
