import os
from typing import Any, Dict, List
import json

from neo4j import GraphDatabase
import timeit

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
match (n)
call {
  with n
  with  collect(n) as nodeList
  call apoc.export.json.data(nodeList, [], null, {stream:true})
  yield data
  return data
} in 4 concurrent transactions of 200 rows
return data
"""

rel_query = """
match ()-[r]->()
call {
  with r
  with  collect(r) as relList
  call apoc.export.json.data([], relList, null, {stream:true})
  yield data
  return data
} in 4 concurrent transactions of 200 rows
return data
"""


def get_node_data() -> List[Dict[str, Any]]:
    print(f"Exporting Nodes")
    with driver.session(database=dbname) as session:
        return session.run(node_query).values()


def get_rel_data() -> List[Dict[str, Any]]:
    print(f"Exporting Relationships")
    with driver.session(database=dbname) as session:
        return session.run(rel_query).values()


def save_data_as_json_lines_file(
    data: List[Dict[str, Any]], file_name: str = "data.json"
) -> None:
    with open(file_name, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item[0], indent=4) + "\n")

if __name__ == "__main__":

    start = timeit.default_timer()
    print(f"The start time is : {start} seconds.")
    
    os.makedirs("exports/", exist_ok=True)

    data = get_node_data()
    save_data_as_json_lines_file(data, "exports/nodes.jsonl")

    data = get_rel_data()
    save_data_as_json_lines_file(data, "exports/relationships.jsonl")

    print(f"The difference of time is : {timeit.default_timer() - start} seconds.")
