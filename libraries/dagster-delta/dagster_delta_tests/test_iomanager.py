import os

import polars as pl
import pytest
from dagster import (
    Out,
    graph,
    op,
)
from dagster_delta_polars import DeltaLakePolarsIOManager
from deltalake import DeltaTable

from dagster_delta import LocalConfig
from dagster_delta.io_manager import WriteMode


@op(out=Out(metadata={"schema": "a_df"}))
def a_df() -> pl.DataFrame:
    return pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


@op(out=Out(metadata={"schema": "add_one"}))
def add_one(df: pl.DataFrame) -> pl.DataFrame:
    return df + 1


@graph
def add_one_to_dataframe():
    add_one(a_df())


@pytest.fixture()
def io_manager_with_parquet_read_options(tmp_path) -> DeltaLakePolarsIOManager:
    return DeltaLakePolarsIOManager(
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

    job = add_one_to_dataframe.to_job(resource_defs=resource_defs)

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
