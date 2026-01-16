from datetime import date, datetime
from typing import Optional

from deltalake.table import FilterLiteralType

from dagster_delta.io_manager.base import (
    DELTA_DATE_FORMAT,
    DELTA_DATETIME_FORMAT,
)


def create_predicate(
    partition_filters: list[FilterLiteralType],
    target_alias: Optional[str] = None,
) -> str:
    partition_predicates = []

    def to_sql_tuple(values: list[str]) -> str:
        escaped = [v.replace("'", "''") for v in values]
        quoted = [f"'{v}'" for v in escaped]
        return f"({', '.join(quoted)})"

    for part_filter in partition_filters:
        column = f"{target_alias}.{part_filter[0]}" if target_alias is not None else part_filter[0]
        value = part_filter[2]
        if isinstance(value, (int, float, bool)):
            value = str(value)
        elif isinstance(value, str):
            value = value.replace("'", "''")  # convert to double single quotes for sql parser
            value = f"'{value}'"
        elif isinstance(value, list):
            if all(isinstance(v, str) for v in value):
                value = to_sql_tuple(value)
            else:
                value = str(tuple(v for v in value))
        elif isinstance(value, datetime):
            value = f"'{value.strftime(DELTA_DATETIME_FORMAT)}'"
        elif isinstance(value, date):
            value = f"'{value.strftime(DELTA_DATE_FORMAT)}'"
        partition_predicates.append(f"{column} {part_filter[1]} {value}")

    return " AND ".join(partition_predicates)
