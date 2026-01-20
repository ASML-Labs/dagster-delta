from __future__ import annotations

import datetime as dt
from typing import List, Mapping, Sequence, Union, cast  # noqa

import pendulum
from dagster import (
    AssetKey,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    PartitionsDefinition,
    TimeWindowPartitionsDefinition,
)

try:
    from dagster._core.definitions.partitions.utils import TimeWindow
except ModuleNotFoundError:
    from dagster._core.definitions.time_window_partitions import TimeWindow  # pyright: ignore[reportMissingImports]

from dagster._core.storage.db_io_manager import TablePartitionDimension
from pendulum import instance as pdi


def generate_multi_partitions_dimension(
    asset_partition_keys: Sequence[str],
    asset_partitions_def: MultiPartitionsDefinition | PartitionsDefinition,
    partition_expr: Mapping[str, str] | TimeWindow | None,
    asset_key: AssetKey,
) -> list[TablePartitionDimension]:
    """Generates multi partition dimensions."""
    partition_dimensions: list[TablePartitionDimension] = []
    multi_partition_key_mappings = [
        cast(MultiPartitionKey, partition_key).keys_by_dimension
        for partition_key in asset_partition_keys
    ]
    for part in asset_partitions_def.partitions_defs:  # type: ignore[attr-defined]
        partitions: list[TimeWindow | str] = []
        for multi_partition_key_mapping in multi_partition_key_mappings:
            partition_key = multi_partition_key_mapping[part.name]
            if isinstance(part.partitions_def, TimeWindowPartitionsDefinition):
                partitions.append(
                    part.partitions_def.time_window_for_partition_key(partition_key),
                )
            else:
                partitions.append(partition_key)

        partition_expr_str = partition_expr.get(part.name)  # type: ignore[attr-defined]
        if partition_expr_str is None:
            raise ValueError(
                f"Asset '{asset_key}' has partition {part.name}, but the"
                f" 'partition_expr' metadata does not contain a {part.name} entry,"
                " so we don't know what column to filter it on. Specify which"
                " column of the database contains data for the"
                f" {part.name} partition.",
            )
        partitions_: TimeWindow | Sequence[str]
        if all(isinstance(partition, TimeWindow) for partition in partitions):
            checker = MultiTimePartitionsChecker(
                partitions=cast(list[TimeWindow], partitions),
            )
            if not checker.is_consecutive():
                raise ValueError("Dates are not consecutive.")
            partitions_ = TimeWindow(
                start=checker.start,
                end=checker.end,
            )
        elif all(isinstance(partition, str) for partition in partitions):
            partitions_ = list(set(cast(list[str], partitions)))
        else:
            raise ValueError("Unknown partition type")
        partition_dimensions.append(
            TablePartitionDimension(
                partition_expr=cast(str, partition_expr_str),
                partitions=partitions_,
            ),
        )
    return partition_dimensions


def generate_single_partition_dimension(
    partition_expr: str,
    asset_partition_keys: Sequence[str],
    asset_partitions_time_window: TimeWindow | None,
) -> TablePartitionDimension:
    """Given a single partition, generate a TablePartitionDimension object that can be used to create a TableSlice object.

    Args:
        partition_expr (str): Partition expression for the asset partition
        asset_partition_keys (Sequence[str]): Partition keys for the asset
        asset_partitions_time_window (TimeWindow | None): TimeWindow object for the asset partition

    Returns:
        TablePartitionDimension: TablePartitionDimension object
    """
    partition_dimension: TablePartitionDimension
    if isinstance(asset_partitions_time_window, TimeWindow):
        partition_dimension = TablePartitionDimension(
            partition_expr=partition_expr,
            partitions=(asset_partitions_time_window if asset_partition_keys else []),  # type: ignore
        )
    else:
        partition_dimension = TablePartitionDimension(
            partition_expr=partition_expr,
            partitions=asset_partition_keys,
        )
    return partition_dimension


class MultiTimePartitionsChecker:
    def __init__(self, partitions: list[TimeWindow]):
        """Helper class that defines checks on a list of TimeWindow objects
        most importantly, partitions should be consecutive.

        Args:
            partitions (list[TimeWindow]): List of TimeWindow objects
        """
        self._partitions = partitions

        start_date = min([w.start for w in self._partitions])
        end_date = max([w.end for w in self._partitions])

        if not isinstance(start_date, dt.datetime):
            raise ValueError("Start date is not a datetime")
        if not isinstance(end_date, dt.datetime):
            raise ValueError("End date is not a datetime")

        self.start = start_date
        self.end = end_date

    @property
    def hourly_delta(self) -> int:
        deltas = [date_diff(w.start, w.end).in_hours() for w in self._partitions]
        if len(set(deltas)) != 1:
            raise ValueError(
                "TimeWindowPartitionsDefinition must have the same delta from start to end",
            )
        return int(deltas[0])

    def is_consecutive(self) -> bool:
        """Checks whether the provided start dates of each partition timewindow is consecutive"""
        expected_starts = {
            pdi(self.start).add(hours=self.hourly_delta * i)
            for i in range(len(set(self._partitions)))
        }

        actual_starts = {pdi(d.start) for d in self._partitions}

        return expected_starts == actual_starts


def date_diff(start: dt.datetime, end: dt.datetime) -> pendulum.Interval:
    """Compute an interval between two dates"""
    start_ = pendulum.instance(start)
    end_ = pendulum.instance(end)
    return end_ - start_
