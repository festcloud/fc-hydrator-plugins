# Neo4j Batch Sink

Description
-----------
Writes records to a Neo4j graph. Each record will be written to a node in the graph.

Use Case
--------
This sink is used whenever you need to write to a Neo4j graph.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.
Typically, the name of the table/view.

**Use Connection** Whether to use a connection. If a connection is used, you do not need to provide the credentials.

**Connection** Name of the connection to use. Project and service account information will be provided by the connection.
You also can use the macro function ${conn(connection-name)}.

**Driver Name:** Name of the JDBC driver to use.

**Database:** Neo4j database name.

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.
