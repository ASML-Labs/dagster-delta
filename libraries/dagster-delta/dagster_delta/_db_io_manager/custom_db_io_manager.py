from collections.abc import Mapping
from typing import (
    TypeVar,
    Union,
    cast,
)

try:
    from dagster._core.definitions.partitions.definition.multi import (
        MultiPartitionsDefinition,
    )
    from dagster._core.definitions.partitions.definition.time_window import (
        TimeWindowPartitionsDefinition,
    )
except ModuleNotFoundError:
    from dagster._core.definitions.multi_dimensional_partitions import (  # pyright: ignore[reportMissingImports]
        MultiPartitionsDefinition,
    )
    from dagster._core.definitions.time_window_partitions import (  # pyright: ignore[reportMissingImports]
        TimeWindowPartitionsDefinition,
    )

from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from dagster._core.storage.db_io_manager import DbIOManager, TablePartitionDimension, TableSlice

from dagster_delta._db_io_manager.utils import (
    generate_multi_partitions_dimension,
    generate_single_partition_dimension,
)

T = TypeVar("T")


class CustomDbIOManager(DbIOManager):
    """Works exactly like the DbIOManager, but overrides the _get_table_slice method
    to provide support for partition mapping. e.g. a mapping from partition A to partition B,
    where A is partitioned on two dimensions and B is partitioned on only one dimension.

    Additionally, gives ability to override
    the table name using `root_name` in the metadata.

    Example:
    ```
    @dg.asset(
    partitions_def=dg.StaticPartitionsDefinition(["a", "b"]),
    metadata={
        "partition_expr": "foo",
        "root_name": "asset_partitioned",
        },
    )
    def asset_partitioned_1(upstream_1, upstream_2):
    ```
    """

    def _get_table_slice(
        self,
        context: Union[OutputContext, InputContext],
        output_context: OutputContext,
    ) -> TableSlice:
        output_context_definition_metadata = output_context.definition_metadata or {}

        schema: str
        table: str
        partition_dimensions: list[TablePartitionDimension] = []
        if context.has_asset_key:
            asset_key_path = context.asset_key.path

            ## Override the
            if output_context_definition_metadata.get("root_name"):
                table = output_context_definition_metadata["root_name"]
            else:
                table = asset_key_path[-1]
            # schema order of precedence: metadata, I/O manager 'schema' config, key_prefix
            if output_context_definition_metadata.get("schema"):
                schema = cast(str, output_context_definition_metadata["schema"])
            elif self._schema:
                schema = self._schema
            elif len(asset_key_path) > 1:
                schema = asset_key_path[-2]
            else:
                schema = "public"

            if context.has_asset_partitions:
                partition_expr = output_context_definition_metadata.get("partition_expr")
                if partition_expr is None:
                    raise ValueError(
                        f"Asset '{context.asset_key}' has partitions, but no 'partition_expr'"
                        " metadata value, so we don't know what column it's partitioned on. To"
                        " specify a column, set this metadata value. E.g."
                        ' @asset(metadata={"partition_expr": "your_partition_column"}).',
                    )

                if isinstance(context.asset_partitions_def, MultiPartitionsDefinition):
                    partition_dimensions.extend(
                        generate_multi_partitions_dimension(
                            asset_partition_keys=context.asset_partition_keys,
                            asset_partitions_def=context.asset_partitions_def,
                            partition_expr=cast(Mapping[str, str], partition_expr),
                            asset_key=context.asset_key,
                        ),
                    )
                else:
                    partition_dimensions.append(
                        generate_single_partition_dimension(
                            partition_expr=cast(str, partition_expr),
                            asset_partition_keys=context.asset_partition_keys,
                            asset_partitions_time_window=(
                                context.asset_partitions_time_window
                                if isinstance(
                                    context.asset_partitions_def,
                                    TimeWindowPartitionsDefinition,
                                )
                                else None
                            ),
                        ),
                    )
        else:
            table = output_context.name
            if output_context_definition_metadata.get("schema"):
                schema = cast(str, output_context_definition_metadata["schema"])
            elif self._schema:
                schema = self._schema
            else:
                schema = "public"

        return TableSlice(
            table=table,
            schema=schema,
            database=self._database,
            partition_dimensions=partition_dimensions,
            columns=(context.definition_metadata or {}).get("columns"),
        )
