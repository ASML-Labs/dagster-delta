# Dagster Unity Catalog Polars

A IO Manager to read Unity Catalog Tables in Dagster orchestrated pipelines.

## Instantiating the IO Manager

To instantiate the IO Manager you need to pass a non-cached token generating function as a `Callable[..., str]` such that the load functions generate and refresh the token as needed. Is set to every 1000 queries by default, this should suffice most of the time.
You also need to pass the server hostname to your databricks workspace and the SQL Warehouse endpoint to which you'll be connecting.

