import datetime as dt
import os
import warnings
from datetime import datetime

import pyarrow as pa
from dagster import (
    AssetExecutionContext,
    AssetIn,
    DailyPartitionsDefinition,
    DimensionPartitionMapping,
    MultiPartitionMapping,
    MultiPartitionsDefinition,
    MultiToSingleDimensionPartitionMapping,
    SpecificPartitionsPartitionMapping,
    StaticPartitionMapping,
    StaticPartitionsDefinition,
    asset,
    materialize,
)
from deltalake import DeltaTable

from dagster_delta import DeltaLakePyarrowIOManager

warnings.filterwarnings("ignore")

daily_partitions_def = DailyPartitionsDefinition(
    start_date="2022-01-01",
    end_date="2022-01-10",
)

letter_partitions_def = StaticPartitionsDefinition(["a", "b", "c"])

color_partitions_def = StaticPartitionsDefinition(["red", "blue", "yellow"])

multi_partition_with_letter = MultiPartitionsDefinition(
    partitions_defs={
        "date": daily_partitions_def,
        "letter": letter_partitions_def,
    },
)

multi_partition_with_color = MultiPartitionsDefinition(
    partitions_defs={
        "date": daily_partitions_def,
        "color": color_partitions_def,
    },
)


@asset(
    key_prefix=["my_schema"],
)
def asset_1() -> pa.Table:
    return pa.Table.from_pydict(
        {
            "value": [1],
            "b": [1],
        },
    )


@asset(
    key_prefix=["my_schema"],
)
def asset_2(asset_1: pa.Table) -> pa.Table:
    return asset_1


# case: we have multiple partitions
@asset(
    key_prefix=["my_schema"],
    partitions_def=multi_partition_with_color,
    metadata={
        "partition_expr": {
            "date": "date_column",
            "color": "color_column",
        },
    },
)
def multi_partitioned_asset_1(context: AssetExecutionContext) -> pa.Table:
    color, date = context.partition_key.split("|")
    date_parsed = dt.datetime.strptime(date, "%Y-%m-%d").date()

    return pa.Table.from_pydict(
        {
            "date_column": [date_parsed],
            "value": [1],
            "b": [1],
            "color_column": [color],
        },
    )


# Multi-to-multi asset is supported
@asset(
    key_prefix=["my_schema"],
    partitions_def=multi_partition_with_color,
    metadata={
        "partition_expr": {
            "date": "date_column",
            "color": "color_column",
        },
    },
)
def multi_partitioned_asset_2(multi_partitioned_asset_1: pa.Table) -> pa.Table:
    return multi_partitioned_asset_1


@asset(
    key_prefix=["my_schema"],
)
def non_partitioned_asset(multi_partitioned_asset_1: pa.Table) -> pa.Table:
    return multi_partitioned_asset_1


# Multi-to-single asset is supported through MultiToSingleDimensionPartitionMapping
@asset(
    key_prefix=["my_schema"],
    partitions_def=daily_partitions_def,
    ins={
        "multi_partitioned_asset": AssetIn(
            ["my_schema", "multi_partitioned_asset_1"],
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date",
            ),
        ),
    },
    metadata={
        "partition_expr": "date_column",
    },
)
def single_partitioned_asset_date(multi_partitioned_asset: pa.Table) -> pa.Table:
    return multi_partitioned_asset


@asset(
    key_prefix=["my_schema"],
    partitions_def=color_partitions_def,
    ins={
        "multi_partitioned_asset": AssetIn(
            ["my_schema", "multi_partitioned_asset_1"],
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="color",
            ),
        ),
    },
    metadata={
        "partition_expr": "color_column",
    },
)
def single_partitioned_asset_color(multi_partitioned_asset: pa.Table) -> pa.Table:
    return multi_partitioned_asset


@asset(
    partitions_def=multi_partition_with_letter,
    key_prefix=["my_schema"],
    metadata={"partition_expr": {"date": "date_column", "letter": "letter"}},
    ins={
        "multi_partitioned_asset": AssetIn(
            ["my_schema", "multi_partitioned_asset_1"],
            partition_mapping=MultiPartitionMapping(
                {
                    "color": DimensionPartitionMapping(
                        dimension_name="letter",
                        partition_mapping=StaticPartitionMapping(
                            {"blue": "a", "red": "b", "yellow": "c"},
                        ),
                    ),
                    "date": DimensionPartitionMapping(
                        dimension_name="date",
                        partition_mapping=SpecificPartitionsPartitionMapping(
                            ["2022-01-01", "2024-01-01"],
                        ),
                    ),
                },
            ),
        ),
    },
)
def mapped_multi_partition(
    context: AssetExecutionContext,
    multi_partitioned_asset: pa.Table,
) -> pa.Table:
    _, letter = context.partition_key.split("|")

    table_ = multi_partitioned_asset.append_column("letter", pa.array([letter]))
    return table_


def test_unpartitioned_asset_to_unpartitioned_asset(
    io_manager: DeltaLakePyarrowIOManager,
):
    resource_defs = {"io_manager": io_manager}

    res = materialize([asset_1, asset_2], resources=resource_defs)
    assert res.success

    asset_1_data = res.asset_value(asset_1.key)
    asset_2_data = res.asset_value(asset_2.key)
    assert asset_1_data == asset_2_data


def test_multi_partitioned_to_multi_partitioned_asset(
    tmp_path,
    io_manager: DeltaLakePyarrowIOManager,
):
    resource_defs = {"io_manager": io_manager}

    multi_partitioned_asset_1_data_all = []
    multi_partitioned_asset_2_data_all = []

    for partition_key in ["red|2022-01-01", "red|2022-01-02", "red|2022-01-03"]:
        res = materialize(
            [multi_partitioned_asset_1, multi_partitioned_asset_2],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success

        multi_partitioned_asset_1_data = res.asset_value(multi_partitioned_asset_1.key)
        multi_partitioned_asset_2_data = res.asset_value(multi_partitioned_asset_2.key)
        cols = multi_partitioned_asset_1_data.column_names
        assert pa.table(
            multi_partitioned_asset_1_data.select(cols).to_pydict(),
        ) == pa.table(
            multi_partitioned_asset_2_data.select(cols).to_pydict(),
        )

        multi_partitioned_asset_1_data_all.append(multi_partitioned_asset_1_data)
        multi_partitioned_asset_2_data_all.append(multi_partitioned_asset_2_data)

    dt = DeltaTable(os.path.join(str(tmp_path), "/".join(multi_partitioned_asset_1.key.path)))

    assert pa.table(dt.to_pyarrow_table().to_pydict()).sort_by("date_column") == pa.table(
        pa.concat_tables(multi_partitioned_asset_1_data_all).to_pydict(),
    ).sort_by("date_column")

    dt = DeltaTable(os.path.join(str(tmp_path), "/".join(multi_partitioned_asset_2.key.path)))
    assert dt.metadata().partition_columns == ["color_column", "date_column"]
    assert pa.table(dt.to_pyarrow_table().to_pydict()).sort_by("date_column") == pa.table(
        pa.concat_tables(multi_partitioned_asset_2_data_all).to_pydict(),
    ).sort_by("date_column")


def test_multi_partitioned_to_single_partitioned_asset_colors(
    io_manager: DeltaLakePyarrowIOManager,
):
    resource_defs = {"io_manager": io_manager}

    multi_partitioned_asset_1_data_all = []

    for partition_key in ["red|2022-01-01", "blue|2022-01-01", "yellow|2022-01-01"]:
        res = materialize(
            [multi_partitioned_asset_1],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success

        multi_partitioned_asset_1_data = res.asset_value(multi_partitioned_asset_1.key)
        multi_partitioned_asset_1_data_all.append(multi_partitioned_asset_1_data)


def test_multi_partitioned_to_single_partitioned_asset_dates(
    tmp_path,
    io_manager: DeltaLakePyarrowIOManager,
):
    resource_defs = {"io_manager": io_manager}

    multi_partitioned_asset_1_data_all = []

    for partition_key in ["red|2022-01-01", "red|2022-01-02", "red|2022-01-03"]:
        res = materialize(
            [multi_partitioned_asset_1],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success

        multi_partitioned_asset_1_data = res.asset_value(multi_partitioned_asset_1.key)
        multi_partitioned_asset_1_data_all.append(multi_partitioned_asset_1_data)

    res = materialize(
        [multi_partitioned_asset_1, single_partitioned_asset_color],
        partition_key="red",
        resources=resource_defs,
        selection=[single_partitioned_asset_color],
    )
    assert res.success

    single_partitioned_asset_color_data = res.asset_value(single_partitioned_asset_color.key)
    cols = single_partitioned_asset_color_data.column_names
    assert pa.table(
        single_partitioned_asset_color_data.select(cols).to_pydict(),
    ).sort_by("date_column") == pa.table(
        pa.concat_tables(multi_partitioned_asset_1_data_all).select(cols).to_pydict(),
    ).sort_by("date_column")

    dt = DeltaTable(os.path.join(str(tmp_path), "/".join(single_partitioned_asset_color.key.path)))
    assert dt.metadata().partition_columns == ["color_column"]

    assert pa.table(
        single_partitioned_asset_color_data.to_pydict(),
    ).sort_by("date_column") == pa.table(dt.to_pyarrow_table().to_pydict()).sort_by("date_column")


def test_multi_partitioned_to_non_partitioned_asset(
    tmp_path,
    io_manager: DeltaLakePyarrowIOManager,
):
    resource_defs = {"io_manager": io_manager}

    multi_partitioned_asset_1_data_all = []

    for partition_key in ["red|2022-01-01", "red|2022-01-02", "red|2022-01-03"]:
        res = materialize(
            [multi_partitioned_asset_1],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success

        multi_partitioned_asset_1_data = res.asset_value(multi_partitioned_asset_1.key)
        multi_partitioned_asset_1_data_all.append(multi_partitioned_asset_1_data)

    res = materialize(
        [multi_partitioned_asset_1, non_partitioned_asset],
        resources=resource_defs,
        selection=[non_partitioned_asset],
    )
    assert res.success

    non_partitioned_asset_data = res.asset_value(non_partitioned_asset.key)
    cols = non_partitioned_asset_data.column_names

    assert pa.table(
        non_partitioned_asset_data.select(cols).to_pydict(),
    ).sort_by("date_column") == pa.table(
        pa.concat_tables(multi_partitioned_asset_1_data_all).select(cols).to_pydict(),
    ).sort_by("date_column")

    dt = DeltaTable(os.path.join(str(tmp_path), "/".join(non_partitioned_asset.key.path)))
    assert dt.metadata().partition_columns == []

    assert pa.table(
        non_partitioned_asset_data.to_pydict(),
    ).sort_by("date_column") == pa.table(dt.to_pyarrow_table().to_pydict()).sort_by("date_column")


def test_multi_partitioned_to_multi_partitioned_with_different_dimensions(
    tmp_path,
    io_manager: DeltaLakePyarrowIOManager,
):
    resource_defs = {"io_manager": io_manager}

    for partition_key in ["red|2022-01-01", "blue|2022-01-01", "yellow|2022-01-01"]:
        res = materialize(
            [multi_partitioned_asset_1],
            partition_key=partition_key,
            resources=resource_defs,
        )
        assert res.success

    res = materialize(
        [multi_partitioned_asset_1, mapped_multi_partition],
        partition_key="2022-01-01|a",
        resources=resource_defs,
        selection=[mapped_multi_partition],
    )

    date_parsed = datetime.strptime("2022-01-01", "%Y-%m-%d").date()

    expected = pa.Table.from_pydict(
        {
            "value": [1],
            "b": [1],
            "color_column": ["blue"],
            "date_column": [date_parsed],
            "letter": ["a"],
        },
    )

    dt = DeltaTable(os.path.join(str(tmp_path), "/".join(mapped_multi_partition.key.path)))
    assert dt.metadata().partition_columns == ["date_column", "letter"]
    actual = pa.table(dt.to_pyarrow_table().to_pydict()).select(expected.column_names)
    assert expected == actual
