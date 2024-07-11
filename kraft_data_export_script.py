"""
This is the data export script used by Kraft. It errors out and is unable to export data in JSON format.
"""

import os
import sys
from neo4j.debug import watch

# uncomment below line(s) to get debug logging
watch("neo4j", out=sys.stdout) #Output debug to stdout
watch("neo4j", out=open('debugLogs.txt', 'w')) #Output debug to logfile


from neo4j import GraphDatabase

uri = os.environ.get("NEO4J_URI")
user = os.environ.get("NEO4J_USERNAME")
password = os.environ.get("NEO4J_PASSWORD")
dbname = 'neo4j'


driver = GraphDatabase.driver(
uri,
auth=(user, password),
max_transaction_retry_time=180,
)



ex_query = (
'CALL apoc.export.json.all(null, {stream:true}) '
'YIELD file, nodes, relationships, properties, data '
'RETURN file, nodes, relationships, properties, data'
)


with driver.session(database=dbname) as session:
    response = list(session.run(ex_query))
    # Loop through results and do something with them
    for p in response:
        print(p)