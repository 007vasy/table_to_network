# table_to_network
toolbox to turn tabular data into nodes and edges using parquet and polars so the import into a graphdb is breeze


# TODOS
* [ ] cli to generate files which reduces import times by eleminating duplicates
* [ ] enforce link uniqueness by based on cols?
* [ ] generate Cypher queries to import data into Neo4j
* [ ] TODO write node id null test
* [ ] allow empty attributes
* [x] allow same node/edge type from the same file form multiple columns
