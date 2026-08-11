import datetime as dt

import pytest
from dagster import AssetKey, MultiPartitionKey, MultiPartitionsDefinition, TimeWindow
from dagster._core.storage.db_io_manager import TablePartitionDimension

from dagster_delta._db_io_manager import utils


# NB: dagster uses dt.datetime even for dates
@pytest.fixture
def daily_partitions_time_window_consecutive() -> list[TimeWindow]:
    return [
        TimeWindow(
            start=dt.datetime(2022, 1, 1, 0),
            end=dt.datetime(2022, 1, 2, 0),
        ),
        TimeWindow(
            start=dt.datetime(2022, 1, 2, 0),
            end=dt.datetime(2022, 1, 3, 0),
        ),
        TimeWindow(
            start=dt.datetime(2022, 1, 3, 0),
            end=dt.datetime(2022, 1, 4, 0),
        ),
    ]


@pytest.fixture
def daily_partitions_time_window_not_consecutive() -> list[TimeWindow]:
    return [
        TimeWindow(
            start=dt.datetime(2022, 1, 1, 0),
            end=dt.datetime(2022, 1, 2, 0),
        ),
        TimeWindow(
            start=dt.datetime(2022, 1, 2, 0),
            end=dt.datetime(2022, 1, 3, 0),
        ),
        TimeWindow(
            start=dt.datetime(2022, 1, 4, 0),
            end=dt.datetime(2022, 1, 5, 0),
        ),
    ]


@pytest.fixture
def hourly_partitions_time_window_consecutive() -> list[TimeWindow]:
    return [
        TimeWindow(
            start=dt.datetime(2022, 1, 1, 1),
            end=dt.datetime(2022, 1, 1, 2),
        ),
        TimeWindow(
            start=dt.datetime(2022, 1, 1, 2),
            end=dt.datetime(2022, 1, 1, 3),
        ),
        TimeWindow(
            start=dt.datetime(2022, 1, 1, 3),
            end=dt.datetime(2022, 1, 1, 4),
        ),
    ]


@pytest.fixture
def hourly_partitions_time_window_not_consecutive() -> list[TimeWindow]:
    return [
        TimeWindow(
            start=dt.datetime(2022, 1, 1, 1),
            end=dt.datetime(2022, 1, 1, 2),
        ),
        TimeWindow(
            start=dt.datetime(2022, 1, 1, 2),
            end=dt.datetime(2022, 1, 1, 3),
        ),
        TimeWindow(
            start=dt.datetime(2022, 1, 1, 4),
            end=dt.datetime(2022, 1, 1, 5),
        ),
    ]


# NB: The MonthlyPartitionDefinition in dagster will orchestrate the partitions into separate jobs to avoid issues with different month duration in hours so we only need to check a single month at a time. We need to check a single month doesn't collide witht the code like in the case of PR #20
@pytest.fixture
def monthly_partitions_time_window() -> list[TimeWindow]:
    return [
        TimeWindow(
            start=dt.datetime(2022, 1, 1, 0),
            end=dt.datetime(2022, 2, 1, 0),
        ),
    ]


def test_multi_time_partitions_monthly_checker(
    monthly_partitions_time_window: list[TimeWindow],
):
    checker = utils.MultiTimePartitionsChecker(
        partitions=monthly_partitions_time_window,
    )

    assert checker.hourly_delta == [744]
    assert checker.start == dt.datetime(2022, 1, 1, 0)
    assert checker.end == dt.datetime(2022, 2, 1, 0)
    assert checker.is_consecutive()


def test_multi_time_partitions_daily_checker_consecutive(
    daily_partitions_time_window_consecutive: list[TimeWindow],
):
    checker = utils.MultiTimePartitionsChecker(
        partitions=daily_partitions_time_window_consecutive,
    )

    assert checker.hourly_delta == [24, 24, 24]
    assert checker.start == dt.datetime(2022, 1, 1, 0)
    assert checker.end == dt.datetime(2022, 1, 4, 0)
    assert checker.is_consecutive()


def test_multi_time_partitions_daily_checker_non_consecutive(
    daily_partitions_time_window_not_consecutive: list[TimeWindow],
):
    checker = utils.MultiTimePartitionsChecker(
        partitions=daily_partitions_time_window_not_consecutive,
    )

    assert checker.hourly_delta == [24, 24, 24]
    assert checker.start == dt.datetime(2022, 1, 1, 0)
    assert checker.end == dt.datetime(2022, 1, 5, 0)
    assert not checker.is_consecutive()


def test_multi_time_partitions_hourly_checker_consecutive(
    hourly_partitions_time_window_consecutive: list[TimeWindow],
):
    checker = utils.MultiTimePartitionsChecker(
        partitions=hourly_partitions_time_window_consecutive,
    )

    assert checker.hourly_delta == [1, 1, 1]
    assert checker.start == dt.datetime(2022, 1, 1, 1)
    assert checker.end == dt.datetime(2022, 1, 1, 4)
    assert checker.is_consecutive()


def test_multi_time_partitions_hourly_checker_non_consecutive(
    hourly_partitions_time_window_not_consecutive: list[TimeWindow],
):
    checker = utils.MultiTimePartitionsChecker(
        partitions=hourly_partitions_time_window_not_consecutive,
    )

    assert checker.hourly_delta == [1, 1, 1]
    assert checker.start == dt.datetime(2022, 1, 1, 1)
    assert checker.end == dt.datetime(2022, 1, 1, 5)
    assert not checker.is_consecutive()


def test_generate_single_partition_dimension_static():
    partition_dimension = utils.generate_single_partition_dimension(
        partition_expr="color_column",
        asset_partition_keys=["red"],
        asset_partitions_time_window=None,
    )
    assert isinstance(partition_dimension, TablePartitionDimension)
    assert partition_dimension.partition_expr == "color_column"
    assert partition_dimension.partitions == ["red"]


def test_generate_single_partition_dimension_time_window():
    partition_dimension = utils.generate_single_partition_dimension(
        partition_expr="date_column",
        asset_partition_keys=["2022-01-01"],
        asset_partitions_time_window=TimeWindow(
            start=dt.datetime(2022, 1, 1, 0),
            end=dt.datetime(2022, 1, 2, 0),
        ),
    )
    assert isinstance(partition_dimension, TablePartitionDimension)
    assert isinstance(partition_dimension.partitions, TimeWindow)
    assert partition_dimension.partition_expr == "date_column"
    assert partition_dimension.partitions.start == dt.datetime(2022, 1, 1, 0)
    assert partition_dimension.partitions.end == dt.datetime(2022, 1, 2, 0)


def test_generate_partition_dimensions_color_varying(
    multi_partition_with_color: MultiPartitionsDefinition,
):
    partition_dimensions = utils.generate_multi_partitions_dimension(
        asset_key=AssetKey("my_asset"),
        # NB: these must be multi partition keys
        asset_partition_keys=[
            MultiPartitionKey(keys_by_dimension={"color": "red", "date": "2022-01-01"}),
            MultiPartitionKey(
                keys_by_dimension={"color": "blue", "date": "2022-01-01"},
            ),
            MultiPartitionKey(
                keys_by_dimension={"color": "yellow", "date": "2022-01-01"},
            ),
        ],
        asset_partitions_def=multi_partition_with_color,
        partition_expr={
            "date": "date_column",
            "color": "color_column",
        },
    )
    assert len(partition_dimensions) == 2
    assert partition_dimensions[0].partition_expr == "color_column"
    assert partition_dimensions[1].partition_expr == "date_column"
    assert partition_dimensions[1].partitions.start == dt.datetime(  # type: ignore
        2022,
        1,
        1,
        0,
        tzinfo=dt.timezone.utc,
    )
    assert partition_dimensions[1].partitions.end == dt.datetime(  # type: ignore
        2022,
        1,
        2,
        0,
        tzinfo=dt.timezone.utc,
    )
    assert sorted(partition_dimensions[0].partitions) == ["blue", "red", "yellow"]


def test_generate_partition_dimensions_date_varying(
    multi_partition_with_color: MultiPartitionsDefinition,
):
    partition_dimensions = utils.generate_multi_partitions_dimension(
        asset_key=AssetKey("my_asset"),
        # NB: these must be multi partition keys
        asset_partition_keys=[
            MultiPartitionKey(keys_by_dimension={"color": "red", "date": "2022-01-01"}),
            MultiPartitionKey(keys_by_dimension={"color": "red", "date": "2022-01-02"}),
            MultiPartitionKey(keys_by_dimension={"color": "red", "date": "2022-01-03"}),
        ],
        asset_partitions_def=multi_partition_with_color,
        partition_expr={
            "date": "date_column",
            "color": "color_column",
        },
    )
    assert len(partition_dimensions) == 2
    assert partition_dimensions[0].partition_expr == "color_column"
    assert partition_dimensions[1].partition_expr == "date_column"
    assert partition_dimensions[1].partitions.start == dt.datetime(  # type: ignore
        2022,
        1,
        1,
        0,
        tzinfo=dt.timezone.utc,
    )
    assert partition_dimensions[1].partitions.end == dt.datetime(  # type: ignore
        2022,
        1,
        4,
        0,
        tzinfo=dt.timezone.utc,
    )
    assert partition_dimensions[0].partitions == ["red"]
