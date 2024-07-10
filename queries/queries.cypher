// Original query in ticket
// Produces 28 rows with 829,857 total DB Hits
PROFILE 
MATCH p=(root:dbt_table_or_consumption_view{name: 'prod_khc_sales.iri_us_raw.iri_us_banner_total_category_product'})-[*0..9]->(n:dbt_table_or_consumption_view)-[*0..9]->(leaf) 
    WHERE n.name ='prod_khc_sales.ecommerce.omni_market_segment_fact' 
RETURN p;

// Same query modified with QPP
// Produces 28 rows with 829,857 total DB Hits
PROFILE 
MATCH p=(root:dbt_table_or_consumption_view{name: 'prod_khc_sales.iri_us_raw.iri_us_banner_total_category_product'})
//Start of QPP
(()-[r1]->()){,9}
(n:dbt_table_or_consumption_view WHERE n.name = 'prod_khc_sales.ecommerce.omni_market_segment_fact')
(()-[r2]->()){,9}
(leaf)
RETURN p;

// On the first hop, since you are going from dbt_table_or_consumption_view -> dbt_table_or_consumption_view and there is only one rel between them defining it will save you a lot of work
// Same result and it goes from 829,857 -> 66,578 total DB hits
PROFILE 
MATCH p=(root:dbt_table_or_consumption_view{name: 'prod_khc_sales.iri_us_raw.iri_us_banner_total_category_product'})-[:referenced_by*0..9]->(n:dbt_table_or_consumption_view)-[*0..9]->(leaf) 
    WHERE n.name ='prod_khc_sales.ecommerce.omni_market_segment_fact' 
RETURN p;

// Same as above with QPP
// Same result and it goes from 829,857 -> 66,578 total DB hits
PROFILE 
MATCH p=(root:dbt_table_or_consumption_view{name: 'prod_khc_sales.iri_us_raw.iri_us_banner_total_category_product'})
//Start of QPP
(()-[r1:referenced_by]->()){,9}
(n:dbt_table_or_consumption_view WHERE n.name = 'prod_khc_sales.ecommerce.omni_market_segment_fact')
(()-[r2]->()){,9}
(leaf)
RETURN p;

// The more paths you want to hop the more expensive it will get, below is an example for going from 0 to all
// 40 rows 24,480,944 total db hits in 2393 ms
PROFILE 
MATCH p=(root:dbt_table_or_consumption_view{name: 'prod_khc_sales.iri_us_raw.iri_us_banner_total_category_product'})-[*0..]->(n:dbt_table_or_consumption_view)-[*0..]->(leaf) 
    WHERE n.name ='prod_khc_sales.ecommerce.omni_market_segment_fact' 
RETURN p;

// Adding the filter to the first rel since it will always go to dbt_table_or_consumption_view -> dbt_table_or_consumption_view
// 40 rows 1,922,760 total db hits in 328 ms.
PROFILE 
MATCH p=(root:dbt_table_or_consumption_view{name: 'prod_khc_sales.iri_us_raw.iri_us_banner_total_category_product'})-[:referenced_by*0..]->(n:dbt_table_or_consumption_view)-[*0..]->(leaf) 
    WHERE n.name ='prod_khc_sales.ecommerce.omni_market_segment_fact' 
RETURN p;

// QPP Equivalent
// 40 rows 1922760 total db hits in 272 ms
PROFILE 
MATCH p=(root:dbt_table_or_consumption_view{name: 'prod_khc_sales.iri_us_raw.iri_us_banner_total_category_product'})
//Start of QPP
(()-[r1:referenced_by]->())*
(n:dbt_table_or_consumption_view WHERE n.name = 'prod_khc_sales.ecommerce.omni_market_segment_fact')
(()-[r2]->())*
(leaf)
RETURN p;

// Ravi's
MATCH (n:{node_type})
WHERE n.name ='{node_name}'
WITH n
MATCH p=()-[*0..]->(n)
WITH n, nodes(p) AS nodes, relationships(p) AS rels
with n, { node: head(nodes) , rel: head(rels) } AS root
WITH n, COLLECT(DISTINCT root) AS roots
MATCH p=(n)-[*0..]->()
WITH n, roots, nodes(p) AS nodes, relationships(p) AS rels
with n, roots, { node: tail(nodes) , rel: CASE WHEN size(rels) > 1
THEN tail(rels) ELSE head(rels) END } AS leaf
WITH {node: n, rel: []} as n1, roots, COLLECT(DISTINCT leaf)
AS leaves
RETURN n1 + roots + leaves as data



// Export Query
CALL apoc.export.json.all(null, {stream:true}) 
YIELD file, nodes, relationships, properties, data 
RETURN file, nodes, relationships, properties, data


// https://support.neo4j.com/s/case/500UN000005Xs17YAC/export-from-neo4j-failing

MATCH (n:dbt_table_or_consumption_view{name: 'prod_khc_sales.kroger.kroger_daily_point_of_sale_fact'})
// Get Roots
CALL {
    WITH n
    CALL apoc.path.expandConfig(n, {
	    relationshipFilter: "<",
        uniqueness: "RELATIONSHIP_GLOBAL"
    })
    YIELD path AS p
    /*WITH nodes(p) AS nodes, 
         relationships(p) AS rels
    WITH { node: head(nodes) , rel: head(rels) } AS root
    WITH COLLECT(DISTINCT root) AS roots
    RETURN roots*/
    RETURN count(p) AS totalRootPaths
}
WITH n, totalRootPaths
// Get Leafs
CALL {
    WITH n
    CALL apoc.path.expandConfig(n, {
	    relationshipFilter: ">",
        uniqueness: "RELATIONSHIP_GLOBAL"
    })
    YIELD path AS p
    /*WITH nodes(p) AS nodes, 
         relationships(p) AS rels
    WITH { node: head(nodes) , rel: head(rels) } AS leaves
    WITH COLLECT(DISTINCT root) AS leaves
    RETURN leaves*/
    RETURN count(p) AS totalLeafPaths
}
RETURN n.name AS sourceName, totalRootPaths, totalLeafPaths

