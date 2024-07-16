import os
from typing import Any, Dict, List
import json

from neo4j import GraphDatabase

uri = os.environ.get("NEO4J_URI")
user = os.environ.get("NEO4J_USERNAME")
password = os.environ.get("NEO4J_PASSWORD")
dbname = "neo4j"


driver = GraphDatabase.driver(
    uri,
    auth=(user, password),
    max_transaction_retry_time=180,
)

node_query = """
match (n:{node_label})
call {{
  with n
  with  collect(n) as nodeList
  call apoc.export.json.data(nodeList, [], null, {{stream:true}})
  yield data
  return data
}} in 4 concurrent transactions of 200 rows
return data
"""

rel_query = """
match ()-[r:{rel_type}]->()
call {{
  with r
  with  collect(r) as relList
  call apoc.export.json.data([], relList, null, {{stream:true}})
  yield data
  return data
}} in 4 concurrent transactions of 200 rows
return data
"""


def get_node_data(node_label: str) -> List[Dict[str, Any]]:
    print(f"Exporting Node {node_label}")
    with driver.session(database=dbname) as session:
        return session.run(node_query.format(node_label=node_label)).values()


def get_rel_data(rel_type: str) -> List[Dict[str, Any]]:
    print(f"Exporting Relationship {rel_type}")
    with driver.session(database=dbname) as session:
        return session.run(rel_query.format(rel_type=rel_type)).values()


def save_data_as_json_lines_file(
    data: List[Dict[str, Any]], file_name: str = "data.json"
) -> None:
    with open(file_name, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item[0], indent=4) + "\n")


labels = [
    "source",
    "ingested_table",
    "dbt_table_or_consumption_view",
    "powerbi_workspace",
    "powerbi_report",
    "powerbi_dashboard",
    "powerbi_dashboard_tile",
    "powerbi_dataset",
    "powerbi_table",
    "powerbi_datamart",
    "tableau_table",
    "tableau_dashboard",
    "tableau_workbook",
    "tableau_sheet",
]

rels = [
    "ingested_as",
    "has_view",
    "referenced_by",
    "used_in_powerbi_table",
    "present_in",
    "in_dashboard",
    "used_in_tile",
    "used_in",
    "in_workspace",
    "contained_in",
    "visualized_as",
]

if __name__ == "__main__":

    os.makedirs("exports/nodes", exist_ok=True)
    os.makedirs("exports/relationships", exist_ok=True)

    for label in labels:
        data = get_node_data(node_label=label)
        save_data_as_json_lines_file(data, "exports/nodes/"+label+".jsonl")
    
    for rel in rels:
        data = get_rel_data(rel_type=rel)
        save_data_as_json_lines_file(data, "exports/relationships/"+rel+".jsonl")
