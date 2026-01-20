from collections.abc import Iterable, Sequence
from typing import Optional, Union, cast

try:
    from dagster._core.definitions.partitions.utils import (
        TimeWindow,
    )
except ImportError:
    from dagster._core.definitions.time_window_partitions import (  # pyright: ignore[reportMissingImports]
        TimeWindow,
    )

from dagster._core.storage.db_io_manager import TablePartitionDimension
from deltalake.schema import Field as DeltaField
from deltalake.schema import PrimitiveType, Schema
from deltalake.table import FilterLiteralType


def partition_dimensions_to_dnf(
    partition_dimensions: Iterable[TablePartitionDimension],
    table_schema: Schema,
    date_format: Optional[dict[str, str]] = None,
) -> Optional[list[FilterLiteralType]]:
    """Converts partition dimensions to dnf filters"""
    parts = []
    for partition_dimension in partition_dimensions:
        field = _field_from_schema(partition_dimension.partition_expr, table_schema)
        if field is None:
            raise ValueError(
                f"Field {partition_dimension.partition_expr} is not part of table schema.",
                "Currently only column names are supported as partition expressions",
            )
        if isinstance(field.type, PrimitiveType):
            if field.type.type in ["timestamp", "date"]:
                filter_ = _time_window_partition_dnf(
                    partition_dimension,
                    field.type.type,
                )
                if isinstance(filter_, list):
                    parts.extend(filter_)
                else:
                    parts.append(filter_)
            elif field.type.type in ["string", "integer"]:
                field_date_format = date_format.get(field.name) if date_format is not None else None
                filter_ = _value_dnf(
                    partition_dimension,
                    field_date_format,
                    field.type.type,
                )
                if isinstance(filter_, list):
                    parts.extend(filter_)
                else:
                    parts.append(filter_)
            else:
                raise ValueError(f"Unsupported partition type {field.type.type}")
        else:
            raise ValueError(f"Unsupported partition type {field.type}")

    return parts if len(parts) > 0 else None


def _value_dnf(
    table_partition: TablePartitionDimension,
    date_format: Optional[str] = None,
    field_type: Optional[str] = None,
) -> Union[
    list[tuple[str, str, Union[int, str]]],
    tuple[str, str, Sequence[str]],
    tuple[str, str, str],
]:  # noqa: ANN202
    # ", ".join(f"'{partition}'" for partition in table_partition.partitions)  # noqa: ERA001
    if (
        isinstance(table_partition.partitions, list)
        and all(isinstance(p, TimeWindow) for p in table_partition.partitions)
    ) or isinstance(table_partition.partitions, TimeWindow):
        if date_format is None:
            raise Exception(
                "Date format not set on time based partition definition, even though field is (str, int). Set date fmt on the partition_def, or change column type to date/datetime.",
            )
        if isinstance(table_partition.partitions, list):
            start_dts = [partition.start for partition in table_partition.partitions]  # type: ignore
            end_dts = [partition.end for partition in table_partition.partitions]  # type: ignore
            start_dt = min(start_dts)
            end_dt = max(end_dts)
        else:
            start_dt = table_partition.partitions.start  # type: ignore[attr-defined]
            end_dt = table_partition.partitions.end  # type: ignore[attr-defined]

        start_dt = start_dt.strftime(date_format)
        end_dt = end_dt.strftime(date_format)

        if field_type == "integer":
            start_dt = int(start_dt)
            end_dt = int(end_dt)
        return [
            (table_partition.partition_expr, ">=", start_dt),
            (table_partition.partition_expr, "<", end_dt),
        ]

    else:
        partition = cast(Sequence[str], table_partition.partitions)
        partition = list(set(partition))
        if len(partition) > 1:
            return (table_partition.partition_expr, "in", partition)
        else:
            return (table_partition.partition_expr, "=", partition[0])


def _time_window_partition_dnf(
    table_partition: TablePartitionDimension,
    data_type: str,
) -> Union[FilterLiteralType, list[FilterLiteralType]]:
    if isinstance(table_partition.partitions, list):
        raise Exception(
            "For date primitive we shouldn't have received a sequence[str] but a TimeWindow",
        )
    else:
        partition = cast(TimeWindow, table_partition.partitions)
        start_dt, end_dt = partition
        start_dt, end_dt = start_dt.replace(tzinfo=None), end_dt.replace(tzinfo=None)

    if data_type == "date":
        start_dt, end_dt = (
            start_dt.date(),
            end_dt.date(),
        )

    if start_dt != end_dt:
        return [
            (table_partition.partition_expr, ">=", start_dt),
            (table_partition.partition_expr, "<", end_dt),
        ]
    else:
        return (table_partition.partition_expr, "=", start_dt)


def _field_from_schema(field_name: str, schema: Schema) -> Optional[DeltaField]:
    for field in schema.fields:
        if field.name == field_name:
            return field
    return None
