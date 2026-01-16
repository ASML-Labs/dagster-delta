import logging
from abc import abstractmethod
from typing import Any, Generic, TypeVar, Union, cast

from arro3.core import RecordBatchReader, Table
from arro3.core.types import ArrowArrayExportable, ArrowStreamExportable
from dagster import (
    InputContext,
    MetadataValue,
    OutputContext,
    TableColumn,
    TableSchema,
)
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from deltalake import CommitProperties, DeltaTable, QueryBuilder, WriterProperties, write_deltalake
from deltalake.exceptions import TableNotFoundError
from deltalake.schema import Schema
from deltalake.writer._conversion import _convert_arro3_schema_to_delta

from dagster_delta._handler.merge import merge_execute
from dagster_delta._handler.utils import (
    create_predicate,
    extract_date_format_from_partition_definition,
    partition_dimensions_to_dnf,
)
from dagster_delta.config import MergeConfig
from dagster_delta.io_manager.base import (
    TableConnection,
    _DeltaTableIOManagerResourceConfig,
)

T = TypeVar("T")

ArrowTypes = Union[RecordBatchReader, Table]
try:
    import pyarrow as pa

    ArrowTypes = Union[RecordBatchReader, Table, pa.Table, pa.RecordBatchReader]
except ImportError:
    pass


class DeltalakeBaseArrowTypeHandler(DbTypeHandler[T], Generic[T]):
    """Base TypeHandler implementation for arrow supported libraries used to handle deltalake IO."""

    @abstractmethod
    def from_arrow(
        self,
        obj: Union[ArrowStreamExportable, ArrowArrayExportable],
        target_type: type,
    ) -> T:
        """Abstract method to convert arrow to target type"""
        pass

    @abstractmethod
    def to_arrow(self, obj: T) -> RecordBatchReader:  # type: ignore
        """Abstract method to convert type to arrow"""
        pass

    @abstractmethod
    def get_output_stats(self, obj: T) -> dict[str, MetadataValue]:
        """Abstract method to return output stats"""
        pass

    @staticmethod
    def _find_keys_in_metadata(
        context: OutputContext,
        keys: list[str] = ["merge_predicate", "merge_operations_config"],
    ) -> dict[str, Any]:
        """Finds the keys in the metadata in the following order:

        It will find the merge_predicate or merge_operations_config in this order:
        1. Runtime metadata
        2. Definition metadata
        3. IO Manager config

        E.g., `merge_predicate` and `merge_operations_config`

        Args:
            context (OutputContext): The output context
            keys (list[str], optional): The keys to find in the metadata. Defaults to ["merge_predicate", "merge_operations_config"].

        Returns:
            dict[str, Any]: The metadata with the keys found
        """
        metadata_definition = context.definition_metadata or {}
        metadata_output = context.output_metadata or {}

        # Find each of the key in the definition or output metadata
        result = {}

        for key in keys:
            if key in metadata_output or {}:
                result[key] = metadata_output[key]
            else:
                result[key] = metadata_definition.get(key)

            # If it's a TextMetadataValue, cast it to string
            if isinstance(result[key], MetadataValue):
                result[key] = result[key].value

        return result

    def handle_output(
        self,
        context: OutputContext,
        table_slice: TableSlice,
        obj: T,
        connection: TableConnection,
    ):
        """Stores pyarrow types in Delta table."""
        logger = logging.getLogger()
        logger.setLevel("DEBUG")

        keys_from_metadata = self._find_keys_in_metadata(
            context,
            ["merge_predicate", "merge_operations_config"],
        )

        merge_predicate_from_metadata = keys_from_metadata.get("merge_predicate", None)
        merge_operations_config_from_metadata = keys_from_metadata.get(
            "merge_operations_config",
            None,
        )

        definition_metadata = context.definition_metadata or {}
        additional_table_config = definition_metadata.get("table_configuration", {})
        if connection.table_config is not None:
            table_config = additional_table_config | connection.table_config
        else:
            table_config = additional_table_config
        resource_config = context.resource_config or {}
        object_stats = self.get_output_stats(obj)

        data = self.to_arrow(obj=obj)
        delta_schema = Schema.from_arrow(_convert_arro3_schema_to_delta(data.schema))
        resource_config = cast(_DeltaTableIOManagerResourceConfig, context.resource_config)
        save_mode = definition_metadata.get("mode")
        main_save_mode = resource_config.get("mode")
        schema_mode = definition_metadata.get("schema_mode") or resource_config.get(
            "schema_mode",
        )
        if schema_mode is not None:
            schema_mode = str(schema_mode)

        writer_properties = resource_config.get("writer_properties")
        writer_properties = (
            WriterProperties(**writer_properties) if writer_properties is not None else None  # type: ignore
        )

        commit_properties = definition_metadata.get("commit_properties") or resource_config.get(
            "commit_properties",
        )
        commit_properties = (
            CommitProperties(**commit_properties) if commit_properties is not None else None  # type: ignore
        )
        merge_config = resource_config.get("merge_config")

        date_format = extract_date_format_from_partition_definition(context)

        if save_mode is not None:
            logger.debug(
                "IO manager mode overridden with the asset metadata mode, %s -> %s",
                main_save_mode,
                save_mode,
            )
            main_save_mode = save_mode
        logger.debug("Writing with mode: `%s`", main_save_mode)

        merge_stats = None
        partition_columns = None
        predicate = None

        if table_slice.partition_dimensions is not None:
            partition_filters = partition_dimensions_to_dnf(
                partition_dimensions=table_slice.partition_dimensions,
                table_schema=delta_schema,
                date_format=date_format,
            )
            if partition_filters is not None:
                ## Convert partition_filter to predicate
                predicate = create_predicate(partition_filters)

            partition_columns = [dim.partition_expr for dim in table_slice.partition_dimensions]

        if main_save_mode not in ["merge", "create_or_replace"]:
            if predicate is not None:
                logger.debug("Using explicit partition predicate: \n%s", predicate)
            write_deltalake(
                table_or_uri=connection.table_uri,
                data=data,  # type: ignore[reportArgumentType]
                storage_options=connection.storage_options,
                mode=main_save_mode,  # type: ignore[reportArgumentType]
                predicate=predicate,
                partition_by=partition_columns,
                schema_mode=schema_mode,  # type: ignore[reportArgumentType]
                configuration=table_config,
                writer_properties=writer_properties,  # type: ignore[reportArgumentType]
                commit_properties=commit_properties,
            )
        elif main_save_mode == "create_or_replace":
            DeltaTable.create(
                table_uri=connection.table_uri,
                schema=delta_schema,
                mode="overwrite",
                partition_by=partition_columns,
                configuration=table_config,
                storage_options=connection.storage_options,
            )
        else:
            if merge_config is None:
                raise ValueError(
                    "Merge Configuration should be provided when `mode = WriterMode.merge`",
                )
            try:
                dt = DeltaTable(connection.table_uri, storage_options=connection.storage_options)
            except TableNotFoundError:
                logger.debug("Creating a DeltaTable first before merging.")
                dt = DeltaTable.create(
                    table_uri=connection.table_uri,
                    schema=delta_schema,
                    partition_by=partition_columns,
                    configuration=table_config,
                    storage_options=connection.storage_options,
                    commit_properties=commit_properties,
                )
            merge_stats = merge_execute(
                dt,
                data,  # type: ignore[reportArgumentType]
                MergeConfig.model_validate(merge_config),
                writer_properties=writer_properties,
                commit_properties=commit_properties,
                merge_predicate_from_metadata=merge_predicate_from_metadata,
                merge_operations_config=merge_operations_config_from_metadata,
                partition_filters=partition_filters,
            )

        dt = DeltaTable(connection.table_uri, storage_options=connection.storage_options)

        output_metadata = {
            # "dagster/table_name": table_slice.table,
            "table_uri": MetadataValue.path(connection.table_uri),
            # "dagster/uri": MetadataValue.path(connection.table_uri),
            "dagster/column_schema": MetadataValue.table_schema(
                TableSchema(
                    columns=[
                        TableColumn(name=name, type=str(dtype))
                        for name, dtype in zip(
                            delta_schema.to_arrow().names,
                            delta_schema.to_arrow().types,
                        )
                    ],
                ),
            ),
            "table_version": MetadataValue.int(dt.version()),
            # **stats,
            **object_stats,
        }
        if merge_stats is not None:
            output_metadata["num_output_rows"] = MetadataValue.int(
                merge_stats.get("num_output_rows", 0),
            )
            output_metadata["merge_stats"] = MetadataValue.json(merge_stats)

        context.add_output_metadata(output_metadata)

    def load_input(
        self,
        context: InputContext,
        table_slice: TableSlice,
        connection: TableConnection,
    ) -> T:
        """Loads the input as a arro3 Table or RecordBatchReader."""
        table = DeltaTable(
            table_uri=connection.table_uri,
            storage_options=connection.storage_options,
        )
        logger = logging.getLogger()
        logger.setLevel("DEBUG")
        logger.debug("Connection timeout duration %s", connection.storage_options.get("timeout"))

        predicate = None
        if table_slice.partition_dimensions is not None:
            partition_filters = partition_dimensions_to_dnf(
                partition_dimensions=table_slice.partition_dimensions,
                table_schema=table.schema(),
            )

            if partition_filters is not None:
                ## Convert partition_filter to predicate
                predicate = create_predicate(partition_filters)

                logger.debug("Dataset input predicate %s", predicate)

        col_select = table_slice.columns if table_slice.columns is not None else "*"
        query = f"SELECT {col_select} FROM tbl"
        if predicate is not None:
            query = f"{query} WHERE {predicate}"
        data = QueryBuilder().register("tbl", table).execute(query)

        return self.from_arrow(data, context.dagster_type.typing_type)
