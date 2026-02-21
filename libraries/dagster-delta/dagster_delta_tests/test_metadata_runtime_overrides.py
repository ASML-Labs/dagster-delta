import os
from typing import Optional

import pyarrow as pa
import pytest
from dagster import (
    Config,
    OpExecutionContext,
    Out,
    RunConfig,
    graph,
    op,
)
from deltalake import DeltaTable

from dagster_delta import DeltaLakePyarrowIOManager, LocalConfig


@pytest.fixture
def io_manager(tmp_path) -> DeltaLakePyarrowIOManager:
    return DeltaLakePyarrowIOManager(
        root_uri=str(tmp_path),
        storage_options=LocalConfig(),
    )


class RuntimeConfig(Config):  # noqa: D101
    mode: Optional[str] = None
    schema_mode: Optional[str] = None


@op(
    out=Out(
        metadata={
            "schema": "a_df",
            "mode": "append",
            "schema_mode": "merge",
            "commit_properties": {"custom_metadata": {"userName": "John Doe"}},
        },
    ),
)
def a_df(context: OpExecutionContext, config: RuntimeConfig) -> pa.Table:
    if config.mode:
        context.add_output_metadata({"mode": config.mode})
    if config.schema_mode:
        context.add_output_metadata({"schema_mode": config.schema_mode})
    return pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})


@op(
    out=Out(
        metadata={
            "schema": "a_df",
            "mode": "append",
            "schema_mode": "merge",
            "commit_properties": {"custom_metadata": {"userName": "Jane Doe"}},
        },
    ),
)
def b_df(context: OpExecutionContext, config: RuntimeConfig) -> pa.Table:
    if config.mode:
        context.add_output_metadata({"mode": config.mode})
    if config.schema_mode:
        context.add_output_metadata({"schema_mode": config.schema_mode})
    return pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})


@graph
def to_one_df():
    a_df()


@graph
def to_same_df():
    b_df()


def test_runtime_mode_override(tmp_path, io_manager):
    resource_defs = {"io_manager": io_manager}

    # Execute op job
    job = to_one_df.to_job(resource_defs=resource_defs)
    res = job.execute_in_process()
    assert res.success

    dt = DeltaTable(os.path.join(tmp_path, "a_df/result"))
    assert dt.count() == 3

    # Append more rows
    res = job.execute_in_process()
    dt = DeltaTable(os.path.join(tmp_path, "a_df/result"))
    assert dt.count() == 6

    # Overwrite Delta Table using Run Config
    res = job.execute_in_process(
        run_config=RunConfig(
            ops={
                "a_df": RuntimeConfig(mode="overwrite"),
            },
        ),
    )
    dt = DeltaTable(os.path.join(tmp_path, "a_df/result"))
    assert dt.history(1)[0].get("operationParameters", {})["mode"] == "Overwrite"
    assert dt.count() == 3


def test_runtime_schema_mode_override(tmp_path, io_manager):
    resource_defs = {"io_manager": io_manager}

    job = to_one_df.to_job(resource_defs=resource_defs)
    job2 = to_same_df.to_job(resource_defs=resource_defs)

    res = job.execute_in_process()
    assert res.success
    dt = DeltaTable(os.path.join(tmp_path, "a_df/result"))
    assert dt.count() == 3
    assert len(dt.schema().fields) == 2

    res = job2.execute_in_process()
    assert res.success
    dt = DeltaTable(os.path.join(tmp_path, "a_df/result"))
    assert dt.count() == 6
    assert len(dt.schema().fields) == 3

    # Overwrite Delta Table using Run Config with schema overwrite
    res = job.execute_in_process(
        run_config=RunConfig(
            ops={
                "a_df": RuntimeConfig(
                    mode="overwrite",
                    schema_mode="overwrite",
                ),
            },
        ),
    )
    assert res.success
    dt = DeltaTable(os.path.join(tmp_path, "a_df/result"))
    assert dt.count() == 3
    assert len(dt.schema().fields) == 2
