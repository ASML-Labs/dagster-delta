import os
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from dagster import (
    Out,
    TimeWindow,
    graph,
    op,
)
from dagster._core.storage.db_io_manager import TablePartitionDimension
from deltalake import DeltaTable
from deltalake.schema import Field, PrimitiveType, Schema

from dagster_delta import DeltaLakePyarrowIOManager, LocalConfig
from dagster_delta.handler import partition_dimensions_to_dnf
from dagster_delta.io_manager import WriteMode

TablePartitionDimension(
    partitions=TimeWindow(datetime(2020, 1, 2), datetime(2020, 2, 3)),
    partition_expr="my_timestamp_col",
)


@pytest.fixture()
def test_schema() -> Schema:
    fields = [
        Field(name="string_col", type=PrimitiveType("string")),
        Field(name="date_col", type=PrimitiveType("date")),
        Field(name="timestamp_col", type=PrimitiveType("timestamp")),
    ]
    return Schema(fields=fields)


def test_partition_dimensions_to_dnf(test_schema) -> None:
    parts = [
        TablePartitionDimension(
            partitions=TimeWindow(datetime(2020, 1, 2), datetime(2020, 2, 3)),
            partition_expr="timestamp_col",
        ),
    ]
    dnf = partition_dimensions_to_dnf(parts, test_schema, True)
    assert dnf == [("timestamp_col", "=", "2020-01-02 00:00:00")]

    parts = [
        TablePartitionDimension(
            partitions=TimeWindow(datetime(2020, 1, 2), datetime(2020, 2, 3)),
            partition_expr="date_col",
        ),
    ]
    dnf = partition_dimensions_to_dnf(parts, test_schema, True)
    assert dnf == [("date_col", "=", "2020-01-02")]


@op(out=Out(metadata={"schema": "a_df"}))
def a_pa_table() -> pa.Table:
    return pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})


@op(out=Out(metadata={"schema": "add_one"}))
def add_one(table: pa.Table) -> pa.Table:
    # Add one to each column in the table
    updated_columns = [pc.add_checked(table[column], pa.scalar(1)) for column in table.column_names]  # type: ignore
    updated_table = pa.Table.from_arrays(updated_columns, names=table.column_names)
    return updated_table


@graph
def add_one_to_dataset():
    add_one(a_pa_table())


@pytest.fixture()
def io_manager_with_parquet_read_options(tmp_path) -> DeltaLakePyarrowIOManager:
    return DeltaLakePyarrowIOManager(
        root_uri=str(tmp_path),
        storage_options=LocalConfig(),
        mode=WriteMode.overwrite,
        parquet_read_options={"coerce_int96_timestamp_unit": "us"},
    )


def test_deltalake_io_manager_with_parquet_read_options(
    tmp_path,
    io_manager_with_parquet_read_options,
):
    resource_defs = {"io_manager": io_manager_with_parquet_read_options}

    job = add_one_to_dataset.to_job(resource_defs=resource_defs)

    # run the job twice to ensure that tables get properly deleted
    for _ in range(2):
        res = job.execute_in_process()

        assert res.success

        dt = DeltaTable(os.path.join(tmp_path, "a_df/result"))
        out_df = dt.to_pyarrow_table()
        assert out_df["a"].to_pylist() == [1, 2, 3]

        dt = DeltaTable(os.path.join(tmp_path, "add_one/result"))
        out_df = dt.to_pyarrow_table()
        assert out_df["a"].to_pylist() == [2, 3, 4]
