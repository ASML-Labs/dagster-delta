# dagster-delta
Dagster deltalake implementation for Pyarrow & Polars. Originally forked from dagster-deltalake with customizations.

The IO Managers support partition mapping, custom write modes, special metadata configuration for advanced use cases.

The supported write modes:

- **error**
- **append**
- **overwrite**
- **ignore**
- **merge**
- **create_or_replace**

## Merge

dagster-delta supports MERGE execution with a couple pre-defined MERGE types (dagster_delta.config.MergeType):

- **deduplicate_insert**  <- Deduplicates on write
- **update_only**  <- updates only the matches records
- **upsert**  <- updates existing matches and inserts non matched records
- **replace_and_delete_unmatched** <- updates existing matches and deletes unmatched
- **custom** <- custom Merge with MergeOperationsConfig

Example:
```python
from dagster_delta import DeltaLakePolarsIOManager, WriteMode, MergeConfig, MergeType
from dagster_delta_polars import DeltaLakePolarsIOManager


@asset(
    key_prefix=["my_schema"]  # will be used as the schema (parent folder) in Delta Lake
)
def my_table() -> pl.DataFrame:  # the name of the asset will be the table name
    ...


defs = Definitions(
    assets=[my_table],
    resources={
        "io_manager": DeltaLakePolarsIOManager(
            root_uri="s3://bucket",
            mode=WriteMode.merge,  # or just "merge"
            merge_config=MergeConfig(
                merge_type=MergeType.upsert,
                predicate="s.a = t.a",
                source_alias="s",
                target_alias="t",
            ),
        )
    },
)
```

Custom merge (gives full control)
```python
from dagster_delta import (
    DeltaLakePolarsIOManager,
    WriteMode,
    MergeConfig,
    MergeType,
    MergeOperationsConfig,
)
from dagster_delta_polars import DeltaLakePolarsIOManager


@asset(
    key_prefix=["my_schema"]  # will be used as the schema (parent folder) in Delta Lake
)
def my_table() -> pl.DataFrame:  # the name of the asset will be the table name
    ...


defs = Definitions(
    assets=[my_table],
    resources={
        "io_manager": DeltaLakePolarsIOManager(
            root_uri="s3://bucket",
            mode=WriteMode.merge,  # or just "merge"
            merge_config=MergeConfig(
                merge_type=MergeType.custom,
                predicate="s.a = t.a",
                source_alias="s",
                target_alias="t",
                merge_operations_config=MergeOperationsConfig(
                    when_not_matched_insert_all=[
                        WhenNotMatchedInsertAll(predicate="s.price > 600")
                    ],
                    when_matched_update_all=[WhenMatchedUpdateAll()],
                ),
            ),
        )
    },
)
```

## Special metadata configurations

### **Add** additional `table_configuration`
Specify additional table configurations for `configuration` in `write_deltalake`.

```python
@dg.asset(
    io_manager_key="deltalake_io_manager",
    metadata={"table_configuration": {"delta.enableChangeDataFeed": "true"}},
)
def my_asset() -> pl.DataFrame: ...
```

### **Overwrite** the write `mode`
Override the write `mode` to be used in `write_deltalake`.

```python
@dg.asset(
    io_manager_key="deltalake_io_manager",
    metadata={"mode": "append"},
)
def my_asset() -> pl.DataFrame: ...
```

### **Overwrite** the `custom_metadata`
Override the `custom_metadata` to be used in `write_deltalake`.

```python
@dg.asset(
    io_manager_key="deltalake_io_manager",
    metadata={"custom_metadata": {"owner": "John Doe"}},
)
def my_asset() -> pl.DataFrame: ...
```

### **Overwrite** the write `schema_mode`
Override the `schema_mode` to be used in `write_deltalake`.

```python
@dg.asset(
    io_manager_key="deltalake_io_manager",
    metadata={"schema_mode": "merge"},
)
def my_asset() -> pl.DataFrame: ...
```

### **Overwrite** the `writer_properties`
Override the `writer_properties` to be used in `write_deltalake`.

```python
@dg.asset(
    io_manager_key="deltalake_io_manager",
    metadata={
        "writer_properties": {
            "compression": "SNAPPY",
        }
    },
)
def my_asset() -> pl.DataFrame: ...
```

### **Overwrite** the `merge_predicate`
Override the `merge_predicate` to be used with `merge` execution.

```python
@dg.asset(
    io_manager_key="deltalake_io_manager",
    metadata={"merge_predicate": "s.foo = t.foo AND s.bar = t.bar"},
)
def my_asset() -> pl.DataFrame: ...
```

### **Overwrite** the `schema`
Override the `schema` of where the table will be saved

```python
@dg.asset(
    io_manager_key="deltalake_io_manager",
    metadata={"schema": "custom_db_schema"},
)
def my_asset() -> pl.DataFrame: ...
```

### **Set** the `columns` that need to be read
Override the `columns` to only load these columns in

```python
@dg.asset(
    io_manager_key="deltalake_io_manager",
    ins={"upstream_asset": dg.AssetIn(metadata={"columns": ["foo", "bar"]})},
)
def my_asset(upstream_asset) -> pl.DataFrame: ...
```

### **Override** table name using `root_name`

Instead of using the asset_name for the table name it's possible to set a custom table name using the `root_name` in the asset defintion metadata.

This is useful where you have two or multiple assets who have the same table structure, but each asset is a subset of the full table partition_definition, and it wasn't possible to combine this into a single asset due to requiring different underlying Op logic and/or upstream assets:

```python
import polars as pl
import dagster as dg


@dg.asset(
    io_manager_key="deltalake_io_manager",
    partitions_def=dg.StaticPartitionsDefinition(["a", "b"]),
    metadata={
        "partition_expr": "foo",
        "root_name": "asset_partitioned",
    },
)
def asset_partitioned_1(upstream_1: pl.DataFrame, upstream_2: pl.DataFrame) -> pl.DataFrame: ...


@dg.asset(
    partitions_def=dg.StaticPartitionsDefinition(["c", "d"]),
    metadata={
        "partition_expr": "foo",
        "root_name": "asset_partitioned",
    },
)
def asset_partitioned_2(upstream_3: pl.DataFrame, upstream_4: pl.DataFrame) -> pl.DataFrame: ...
```

Effectively this would be the flow:

```

                 {static_partition_def: [a,b]}
┌───────────┐
│upstream 1 ├─┐ ┌────────────────────────┐
└───────────┘ │ │                        │            write to storage on partition (a,b)
┌───────────┐ └─►   asset_partitioned_1  ├──────────────────────┐
│upstream 2 ├───►                        │                      │
└───────────┘   └────────────────────────┘       ┌──────────────▼──────────────────┐
                                                 │                     partitions  │
                                                 │  asset_partitioned:             │
                                                 │                     [a,b,c,d]   │
┌───────────┐   ┌────────────────────────┐       └──────────────▲──────────────────┘
│upstream 3 ├──┐│                        │                      │
└───────────┘  └►   asset_partitioned_2  │                      │
┌───────────┐ ┌─►                        ├──────────────────────┘
│upstream 4 ├─┘ └────────────────────────┘            write to storage on partition (c,d)
└───────────┘
                 {static_partition_def: [c,d]}

```
