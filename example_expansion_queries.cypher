//Ravi's query
PROFILE 
MATCH (n:dbt_table_or_consumption_view)
    WHERE n.name = $name
WITH n
MATCH p=()-[*0..]->(n)
WITH n, nodes(p) AS nodes, relationships(p) AS rels
WITH n, { node: head(nodes) , rel: head(rels) } AS root
WITH n, COLLECT(DISTINCT root) AS roots
MATCH p=(n)-[*0..]->()
WITH n, roots, nodes(p) AS nodes, relationships(p) AS rels
WITH n, roots, { node: tail(nodes) , rel: CASE WHEN size(rels) > 1
THEN tail(rels) ELSE head(rels) END } AS leaf
WITH {node: n, rel: []} as n1, roots, COLLECT(DISTINCT leaf)
AS leaves
RETURN n1 + roots + leaves as data;

// Cypher using QPP expansion to get roots & apoc.path.spanningTree to get leaves
PROFILE
MATCH (n:dbt_table_or_consumption_view)
    WHERE n.name = $name

//Get roots
CALL {
    WITH n
    MATCH (m)-[r]->*(n)
    WITH * WHERE isEmpty(r)=false
    WITH n, {node: m, rels: r} AS root
    WITH n, COLLECT(root) AS roots
    RETURN {start: n, roots: tail(roots)} AS roots
  }
WITH n, roots

//Get Leaves
CALL{
    WITH n
    CALL apoc.path.spanningTree(n,{
      relationshipFilter: ">"
    }) yield path
    WITH n, tail(nodes(path)) AS nodes, relationships(path) AS rels
    WITH n, {leaf: nodes, rels: rels} AS leaf
    WITH n, COLLECT(leaf) AS leaves
    RETURN {start: n, leaves: tail(leaves)} AS leaves
  }  
RETURN roots, leaves;

// Cypher using apoc.path.spanningTree to get roots and leaves
PROFILE
MATCH (n:dbt_table_or_consumption_view)
    WHERE n.name = $name

//Get roots
CALL {
    WITH n
    CALL apoc.path.spanningTree(n,{
      relationshipFilter: "<"
    }) yield path
    WITH n, tail(nodes(path)) AS nodes, relationships(path) AS rels
    WITH n, {root: nodes, rels: rels} AS root
    WITH n, COLLECT(root) AS roots
    RETURN {start: n, roots: tail(roots)} AS roots
  }
WITH n, roots

//Get Leaves
  CALL{
    WITH n
    CALL apoc.path.spanningTree(n,{
      relationshipFilter: ">"
    }) yield path
    WITH n, tail(nodes(path)) AS nodes, relationships(path) AS rels
    WITH n, {leaf: nodes, rels: rels} AS leaf
    WITH n, COLLECT(leaf) AS leaves
    RETURN {start: n, leaves: tail(leaves)} AS leaves
  }  
RETURN roots, leaves;