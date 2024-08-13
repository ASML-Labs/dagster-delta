from collections.abc import Mapping
from typing import (
    TypeVar,
    Union,
    cast,
)

from dagster._core.definitions.multi_dimensional_partitions import (
    MultiPartitionKey,
    MultiPartitionsDefinition,
)
from dagster._core.definitions.time_window_partitions import (
    TimeWindowPartitionsDefinition,
)
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from dagster._core.storage.db_io_manager import DbIOManager, TablePartitionDimension, TableSlice

T = TypeVar("T")


class DbIOManagerFixed(DbIOManager):  # noqa
    def _get_table_slice(
        self,
        context: Union[OutputContext, InputContext],  # noqa
        output_context: OutputContext,
    ) -> TableSlice:
        output_context_metadata = output_context.metadata or {}

        schema: str
        table: str
        partition_dimensions: list[TablePartitionDimension] = []
        if context.has_asset_key:
            asset_key_path = context.asset_key.path

            if output_context_metadata.get("root_name"):
                table = output_context_metadata["root_name"]
            else:
                table = asset_key_path[-1]
            # schema order of precedence: metadata, I/O manager 'schema' config, key_prefix
            if output_context_metadata.get("schema"):
                schema = cast(str, output_context_metadata["schema"])
            elif self._schema:
                schema = self._schema
            elif len(asset_key_path) > 1:
                schema = asset_key_path[-2]
            else:
                schema = "public"

            if context.has_asset_partitions:
                partition_expr = output_context_metadata.get("partition_expr")
                if partition_expr is None:
                    raise ValueError(
                        f"Asset '{context.asset_key}' has partitions, but no 'partition_expr'"
                        " metadata value, so we don't know what column it's partitioned on. To"
                        " specify a column, set this metadata value. E.g."
                        ' @asset(metadata={"partition_expr": "your_partition_column"}).',
                    )

                if isinstance(context.asset_partitions_def, MultiPartitionsDefinition):
                    multi_partition_key_mappings = [
                        cast(MultiPartitionKey, partition_key).keys_by_dimension
                        for partition_key in context.asset_partition_keys
                    ]
                    for part in context.asset_partitions_def.partitions_defs:
                        partitions = []
                        for multi_partition_key_mapping in multi_partition_key_mappings:
                            partition_key = multi_partition_key_mapping[part.name]
                            if isinstance(part.partitions_def, TimeWindowPartitionsDefinition):
                                partitions.append(
                                    part.partitions_def.time_window_for_partition_key(
                                        partition_key,  # type: ignore
                                    ),
                                )
                            else:
                                partitions.append(partition_key)

                        partition_expr_str = cast(Mapping[str, str], partition_expr).get(part.name)
                        if partition_expr is None:
                            raise ValueError(
                                f"Asset '{context.asset_key}' has partition {part.name}, but the"
                                f" 'partition_expr' metadata does not contain a {part.name} entry,"
                                " so we don't know what column to filter it on. Specify which"
                                " column of the database contains data for the"
                                f" {part.name} partition.",
                            )
                        partition_dimensions.append(
                            TablePartitionDimension(
                                partition_expr=cast(str, partition_expr_str),
                                partitions=partitions,
                            ),
                        )
                elif isinstance(context.asset_partitions_def, TimeWindowPartitionsDefinition):
                    partition_dimensions.append(
                        TablePartitionDimension(
                            partition_expr=cast(str, partition_expr),
                            partitions=(
                                context.asset_partitions_time_window
                                if context.asset_partition_keys
                                else []
                            ),
                        ),
                    )
                else:
                    partition_dimensions.append(
                        TablePartitionDimension(
                            partition_expr=cast(str, partition_expr),
                            partitions=context.asset_partition_keys,
                        ),
                    )
        else:
            table = output_context.name
            if output_context_metadata.get("schema"):
                schema = cast(str, output_context_metadata["schema"])
            elif self._schema:
                schema = self._schema
            else:
                schema = "public"

        return TableSlice(
            table=table,
            schema=schema,
            database=self._database,
            partition_dimensions=partition_dimensions,
            columns=(context.metadata or {}).get("columns"),
        )
