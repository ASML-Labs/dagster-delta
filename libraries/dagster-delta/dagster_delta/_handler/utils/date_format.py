from typing import Optional, Union

from dagster import (
    InputContext,
    MultiPartitionsDefinition,
    OutputContext,
)

try:
    from dagster._core.definitions.partitions.definition.time_window import (
        TimeWindowPartitionsDefinition,
    )
except ModuleNotFoundError:
    from dagster._core.definitions.time_window_partitions import (  # pyright: ignore[reportMissingImports]
        TimeWindowPartitionsDefinition,
    )


def extract_date_format_from_partition_definition(
    context: Union[OutputContext, InputContext],
) -> Optional[dict[str, str]]:
    """Gets the date format from the partition definition if there is a TimeWindowPartitionsDefinition present (nested or not), to be used to properly compare with columns
    in the delta table which are not a datetime object. Returns None if no TimeWindowPartitionsDefinition were present.
    """
    if isinstance(context, InputContext):
        if context.has_asset_partitions:
            if context.upstream_output is not None:
                partition_expr = context.upstream_output.definition_metadata["partition_expr"]  # type: ignore
                partitions_definition = context.asset_partitions_def
            else:
                raise ValueError(
                    "'partition_expr' should have been set in the metadata of the incoming asset since it has a partition definition.",
                )
        else:
            return None
    elif isinstance(context, OutputContext):
        if context.has_asset_partitions:
            if (
                context.definition_metadata is not None
                and "partition_expr" in context.definition_metadata
            ):
                partition_expr = context.definition_metadata["partition_expr"]
            else:
                raise ValueError(
                    "'partition_expr' should have been set in the metadata of the incoming asset since it has a partition definition.",
                )
            partitions_definition = context.asset_partitions_def
        else:
            return None
    if partition_expr is None or partitions_definition is None:
        return None

    date_format: dict[str, str] = {}
    if isinstance(partitions_definition, TimeWindowPartitionsDefinition):
        if isinstance(partition_expr, str):
            date_format[partition_expr] = partitions_definition.fmt  # type: ignore
        else:
            raise ValueError(
                "Single partition definition provided, so partion_expr needs to be a string",
            )
    elif isinstance(partitions_definition, MultiPartitionsDefinition):
        if isinstance(partition_expr, dict):
            for partition_dims_definition in partitions_definition.partitions_defs:
                if isinstance(
                    partition_dims_definition.partitions_def,
                    TimeWindowPartitionsDefinition,
                ):
                    partition_expr_name = partition_expr.get(partition_dims_definition.name)
                    if partition_expr_name is None:
                        raise ValueError(
                            f"Partition_expr mapping is invalid. Partition_dimension :{partition_dims_definition.name} not found in partition_expr: {partition_expr}.",
                        )
                    date_format[partition_expr_name] = partition_dims_definition.partitions_def.fmt  # type: ignore[attr-defined]
        else:
            raise ValueError(
                "MultiPartitionsDefinition provided, so partion_expr needs to be a dictionary mapping of {dimension: column}",
            )

    return date_format if len(date_format) else None
