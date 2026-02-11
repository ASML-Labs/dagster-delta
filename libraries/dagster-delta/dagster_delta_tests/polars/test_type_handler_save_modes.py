import os

import polars as pl
import pytest
from dagster import (
    Out,
    graph,
    op,
)
from deltalake import DeltaTable

from dagster_delta import DeltaLakePolarsIOManager, LocalConfig, WriteMode


@pytest.fixture
def io_manager(tmp_path) -> DeltaLakePolarsIOManager:
    return DeltaLakePolarsIOManager(
        root_uri=str(tmp_path),
        storage_options=LocalConfig(),
        mode=WriteMode.overwrite,
    )


@pytest.fixture
def io_manager_append(tmp_path) -> DeltaLakePolarsIOManager:
    return DeltaLakePolarsIOManager(
        root_uri=str(tmp_path),
        storage_options=LocalConfig(),
        mode=WriteMode.append,
    )


@pytest.fixture
def io_manager_ignore(tmp_path) -> DeltaLakePolarsIOManager:
    return DeltaLakePolarsIOManager(
        root_uri=str(tmp_path),
        storage_options=LocalConfig(),
        mode=WriteMode.ignore,
    )


@op(out=Out(metadata={"schema": "a_df"}))
def a_df() -> pl.DataFrame:
    return pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


@op(out=Out(metadata={"schema": "add_one"}))
def add_one(df: pl.DataFrame):  # noqa: ANN201
    return df + 1


@graph
def add_one_to_dataframe():
    add_one(a_df())


@graph
def just_a_df():
    a_df()


def test_deltalake_io_manager_with_ops_appended(tmp_path, io_manager_append):
    resource_defs = {"io_manager": io_manager_append}

    job = just_a_df.to_job(resource_defs=resource_defs)

    # run the job twice to ensure tables get appended
    expected_result1 = [1, 2, 3]

    for _ in range(2):
        res = job.execute_in_process()

        assert res.success

        dt = DeltaTable(os.path.join(tmp_path, "a_df/result"))
        out_df = dt.to_pyarrow_table()
        assert sorted(out_df["a"].to_pylist()) == sorted(expected_result1)

        expected_result1.extend(expected_result1)


def test_deltalake_io_manager_with_ops_ignored(tmp_path, io_manager_ignore):
    resource_defs = {"io_manager": io_manager_ignore}

    job = just_a_df.to_job(resource_defs=resource_defs)

    # run the job 5 times to ensure tables gets ignored on each write
    for _ in range(5):
        res = job.execute_in_process()

        assert res.success

        dt = DeltaTable(os.path.join(tmp_path, "a_df/result"))
        out_df = dt.to_pyarrow_table()
        assert sorted(out_df["a"].to_pylist()) == [1, 2, 3]

    dt = DeltaTable(os.path.join(tmp_path, "a_df/result"))
    assert dt.version() == 0


@op(out=Out(metadata={"schema": "a_df", "mode": "append"}))
def a_df_custom() -> pl.DataFrame:
    return pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


@graph
def add_one_to_dataframe_custom():
    add_one(a_df_custom())


def test_deltalake_io_manager_with_ops_mode_overridden(tmp_path, io_manager):
    resource_defs = {"io_manager": io_manager}

    job = add_one_to_dataframe_custom.to_job(resource_defs=resource_defs)

    # run the job twice to ensure that tables get properly deleted

    a_df_result = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    add_one_result = pl.DataFrame({"a": [2, 3, 4], "b": [5, 6, 7]})
    from polars.testing import assert_frame_equal

    for _ in range(2):
        res = job.execute_in_process()

        assert res.success

        dt = DeltaTable(os.path.join(tmp_path, "a_df/result"))
        out_df = pl.DataFrame(dt.to_pyarrow_table())
        assert_frame_equal(out_df, a_df_result, check_row_order=False)

        dt = DeltaTable(os.path.join(tmp_path, "add_one/result"))
        out_df = pl.DataFrame(dt.to_pyarrow_table())
        assert_frame_equal(out_df, add_one_result, check_row_order=False)

        a_df_result = pl.concat([a_df_result, a_df_result])
        add_one_result = pl.concat([add_one_result, add_one_result])
