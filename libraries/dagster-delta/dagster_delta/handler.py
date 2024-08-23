import logging
from abc import abstractmethod
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any, Generic, Optional, TypeVar, Union, cast

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from dagster import (
    InputContext,
    MetadataValue,
    MultiPartitionsDefinition,
    OutputContext,
    TableColumn,
    TableSchema,
)
from dagster._core.definitions.time_window_partitions import (
    TimeWindow,
    TimeWindowPartitionsDefinition,
)
from dagster._core.storage.db_io_manager import DbTypeHandler, TablePartitionDimension, TableSlice
from deltalake import DeltaTable, WriterProperties, write_deltalake
from deltalake.exceptions import TableNotFoundError
from deltalake.schema import Field as DeltaField
from deltalake.schema import PrimitiveType, Schema, _convert_pa_schema_to_delta
from deltalake.table import FilterLiteralType

try:
    from pyarrow.parquet import filters_to_expression  # pyarrow >= 10.0.0
except ImportError:
    from pyarrow.parquet import _filters_to_expression as filters_to_expression


from .config import MergeType
from .io_manager import (
    DELTA_DATE_FORMAT,
    DELTA_DATETIME_FORMAT,
    TableConnection,
    _DeltaTableIOManagerResourceConfig,
)

T = TypeVar("T")
ArrowTypes = Union[pa.Table, pa.RecordBatchReader]


def _create_predicate(
    partition_filters: list[FilterLiteralType],
    target_alias: Optional[str] = None,
) -> Optional[str]:
    partition_predicates = []
    for part_filter in partition_filters:
        column = f"{target_alias}.{part_filter[0]}" if target_alias is not None else part_filter[0]
        value = part_filter[2]
        if isinstance(value, (int, float, bool)):
            value = str(value)
        elif isinstance(value, str):
            value = f"'{value}'"
        elif isinstance(value, list):
            value = str(tuple(v for v in value))
        elif isinstance(value, datetime):
            value = str(
                int(value.timestamp() * 1000 * 1000),
            )  # convert to microseconds
        partition_predicates.append(f"{column} {part_filter[1]} {value}")

    return " AND ".join(partition_predicates)


def _merge_execute(
    dt: DeltaTable,
    reader: pa.RecordBatchReader,
    merge_config: dict[str, Any],
    writer_properties: Optional[WriterProperties],
    custom_metadata: Optional[dict[str, str]],
    delta_params: dict[str, Any],
    merge_predicate_from_metadata: Optional[str],
    partition_filters: Optional[list[FilterLiteralType]] = None,
) -> dict[str, Any]:
    merge_type = merge_config.get("merge_type")
    error_on_type_mismatch = merge_config.get("error_on_type_mismatch", True)

    if merge_predicate_from_metadata is not None:
        predicate = str(merge_predicate_from_metadata)
    elif merge_config.get("predicate") is not None:
        predicate = str(merge_config.get("predicate"))
    else:
        raise Exception("merge predicate was not provided")

    target_alias = merge_config.get("target_alias")

    if partition_filters is not None:
        partition_predicate = _create_predicate(partition_filters, target_alias=target_alias)

        predicate = f"{predicate} AND {partition_predicate}"
        logger = logging.getLogger()
        logger.setLevel("DEBUG")
        logger.debug("Using explicit MERGE partition predicate: \n%s", predicate)

    merger = dt.merge(
        source=reader,
        predicate=predicate,
        source_alias=merge_config.get("source_alias"),
        target_alias=target_alias,
        error_on_type_mismatch=error_on_type_mismatch,
        writer_properties=writer_properties,
        custom_metadata=custom_metadata,
        **delta_params,
    )

    if merge_type == MergeType.update_only:
        return merger.when_matched_update_all().execute()
    elif merge_type == MergeType.deduplicate_insert:
        return merger.when_not_matched_insert_all().execute()
    elif merge_type == MergeType.upsert:
        return merger.when_matched_update_all().when_not_matched_insert_all().execute()
    elif merge_type == MergeType.replace_delete_unmatched:
        return merger.when_matched_update_all().when_not_matched_by_source_delete().execute()
    else:
        raise NotImplementedError


class DeltalakeBaseArrowTypeHandler(DbTypeHandler[T], Generic[T]):  # noqa: D101
    @abstractmethod
    def from_arrow(self, obj: pa.RecordBatchReader, target_type: type) -> T:
        """Abstract method to convert arrow to target type"""
        pass

    @abstractmethod
    def to_arrow(self, obj: T) -> tuple[pa.RecordBatchReader, dict[str, Any]]:
        """Abstract method to convert type to arrow"""
        pass

    @abstractmethod
    def get_output_stats(self, obj: T) -> dict[str, MetadataValue]:
        """Abstract method to return output stats"""
        pass

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
        metadata = context.metadata or {}
        merge_predicate_from_metadata = metadata.get("merge_predicate")
        additional_table_config = metadata.get("table_configuration", {})
        if connection.table_config is not None:
            table_config = additional_table_config | connection.table_config
        else:
            table_config = additional_table_config
        resource_config = context.resource_config or {}
        object_stats = self.get_output_stats(obj)
        reader, delta_params = self.to_arrow(obj=obj)
        delta_schema = Schema.from_pyarrow(_convert_pa_schema_to_delta(reader.schema))
        resource_config = cast(_DeltaTableIOManagerResourceConfig, context.resource_config)
        engine = resource_config.get("writer_engine")
        save_mode = metadata.get("mode")
        main_save_mode = resource_config.get("mode")
        custom_metadata = metadata.get("custom_metadata") or resource_config.get("custom_metadata")
        schema_mode = metadata.get("schema_mode") or resource_config.get(
            "schema_mode",
        )
        writer_properties = resource_config.get("writer_properties")
        writer_properties = (
            WriterProperties(**writer_properties) if writer_properties is not None else None  # type: ignore
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
        partition_filters = None
        partition_columns = None
        predicate = None

        if table_slice.partition_dimensions is not None:
            partition_filters = partition_dimensions_to_dnf(
                partition_dimensions=table_slice.partition_dimensions,
                table_schema=delta_schema,
                str_values=True,
                date_format=date_format,
            )
            if partition_filters is not None and engine == "rust":
                ## Convert partition_filter to predicate
                predicate = _create_predicate(partition_filters)
                partition_filters = None
            else:
                predicate = None
            # TODO(): make robust and move to function
            partition_columns = [dim.partition_expr for dim in table_slice.partition_dimensions]

        if main_save_mode not in ["merge", "create_or_replace"]:
            if predicate is not None and engine == "rust":
                logger.debug("Using explicit partition predicate: \n%s", predicate)
            elif partition_filters is not None and engine == "pyarrow":
                logger.debug("Using explicit partition_filter: \n%s", partition_filters)
            write_deltalake(  # type: ignore
                table_or_uri=connection.table_uri,
                data=reader,
                storage_options=connection.storage_options,
                mode=main_save_mode,
                partition_filters=partition_filters,
                predicate=predicate,
                partition_by=partition_columns,
                engine=engine,
                schema_mode=schema_mode,
                configuration=table_config,
                custom_metadata=custom_metadata,
                writer_properties=writer_properties,
                **delta_params,
            )
        elif main_save_mode == "create_or_replace":
            DeltaTable.create(
                table_uri=connection.table_uri,
                schema=_convert_pa_schema_to_delta(reader.schema, **delta_params),
                mode="overwrite",
                partition_by=partition_columns,
                configuration=table_config,
                storage_options=connection.storage_options,
                custom_metadata=custom_metadata,
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
                    schema=_convert_pa_schema_to_delta(reader.schema, **delta_params),
                    partition_by=partition_columns,
                    configuration=table_config,
                    storage_options=connection.storage_options,
                    custom_metadata=custom_metadata,
                )
            merge_stats = _merge_execute(
                dt,
                reader,
                merge_config,
                writer_properties=writer_properties,
                custom_metadata=custom_metadata,
                delta_params=delta_params,
                merge_predicate_from_metadata=merge_predicate_from_metadata,
                partition_filters=partition_filters,
            )

        dt = DeltaTable(connection.table_uri, storage_options=connection.storage_options)
        try:
            stats = _get_partition_stats(dt=dt, partition_filters=partition_filters)
        except Exception as e:
            context.log.warn(f"error while computing table stats: {e}")
            stats = {}

        output_metadata = {
            "table_columns": MetadataValue.table_schema(
                TableSchema(
                    columns=[
                        TableColumn(name=name, type=str(dtype))
                        for name, dtype in zip(reader.schema.names, reader.schema.types)
                    ],
                ),
            ),
            "table_uri": MetadataValue.path(connection.table_uri),
            "table_version": MetadataValue.int(dt.version()),
            **stats,
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
        """Loads the input as a pyarrow Table or RecordBatchReader."""
        parquet_read_options = None
        if context.resource_config is not None:
            parquet_read_options = context.resource_config.get("parquet_read_options", None)
            parquet_read_options = (
                ds.ParquetReadOptions(**parquet_read_options)
                if parquet_read_options is not None
                else None
            )

        dataset = _table_reader(table_slice, connection, parquet_read_options=parquet_read_options)

        if context.dagster_type.typing_type == ds.Dataset:
            if table_slice.columns is not None:
                raise ValueError("Cannot select columns when loading as Dataset.")
            return dataset

        scanner = dataset.scanner(columns=table_slice.columns)
        return self.from_arrow(scanner.to_reader(), context.dagster_type.typing_type)


class DeltaLakePyArrowTypeHandler(DeltalakeBaseArrowTypeHandler[ArrowTypes]):  # noqa: D101
    def from_arrow(self, obj: pa.RecordBatchReader, target_type: type[ArrowTypes]) -> ArrowTypes:  # noqa: D102
        if target_type == pa.Table:
            return obj.read_all()
        return obj

    def to_arrow(self, obj: ArrowTypes) -> tuple[pa.RecordBatchReader, dict[str, Any]]:  # noqa: D102
        if isinstance(obj, pa.Table):
            return obj.to_reader(), {}
        if isinstance(obj, ds.Dataset):
            return obj.scanner().to_reader(), {}
        return obj, {}

    def get_output_stats(self, obj: ArrowTypes) -> dict[str, MetadataValue]:  # noqa: ARG002
        """Returns output stats to be attached to the the context.

        Args:
            obj (PolarsTypes): LazyFrame or DataFrame

        Returns:
            Mapping[str, MetadataValue]: metadata stats
        """
        return {}

    @property
    def supported_types(self) -> Sequence[type[object]]:
        """Returns the supported dtypes for this typeHandler"""
        return [pa.Table, pa.RecordBatchReader, ds.Dataset]


def partition_dimensions_to_dnf(
    partition_dimensions: Iterable[TablePartitionDimension],
    table_schema: Schema,
    str_values: bool = False,
    input_dnf: bool = False,  # during input we want to read a range when it's (un)-partitioned
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
                    str_values,
                    input_dnf,
                )
                if isinstance(filter_, list):
                    parts.extend(filter_)
                else:
                    parts.append(filter_)
            elif field.type.type in ["string", "integer"]:
                field_format = date_format.get(field.name) if date_format is not None else None
                filter_ = _value_dnf(
                    partition_dimension,
                    field_format,
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
            raise Exception("Date format was not provided")
        if isinstance(table_partition.partitions, list):
            start_dts = [partition.start for partition in table_partition.partitions]  # type: ignore
            end_dts = [partition.end for partition in table_partition.partitions]  # type: ignore
            start_dt = min(start_dts)
            end_dt = max(end_dts)
        else:
            start_dt = table_partition.partitions.start
            end_dt = table_partition.partitions.end

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
    str_values: bool,
    input_dnf: bool,
) -> Union[FilterLiteralType, list[FilterLiteralType]]:
    if isinstance(table_partition.partitions, list):
        start_dt = min(
            [cast(TimeWindow, partition).start for partition in table_partition.partitions],
        ).replace(tzinfo=None)
        end_dt = max(
            [cast(TimeWindow, partition).end for partition in table_partition.partitions],
        ).replace(tzinfo=None)
    else:
        partition = cast(TimeWindow, table_partition.partitions)
        start_dt, end_dt = partition
        start_dt, end_dt = start_dt.replace(tzinfo=None), end_dt.replace(tzinfo=None)

    if str_values:
        if data_type == "timestamp":
            start_dt, end_dt = (
                start_dt.strftime(DELTA_DATETIME_FORMAT),
                end_dt.strftime(DELTA_DATETIME_FORMAT),
            )
        elif data_type == "date":
            start_dt, end_dt = (
                start_dt.strftime(DELTA_DATE_FORMAT),
                end_dt.strftime(DELTA_DATETIME_FORMAT),
            )
        else:
            raise ValueError(f"Unknown primitive type: {data_type}")

    if input_dnf:
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


def _get_partition_stats(
    dt: DeltaTable,
    partition_filters: Optional[list[FilterLiteralType]] = None,
) -> dict[str, Any]:
    """_summary_

    Args:
        dt (DeltaTable): DeltaTable object
        partition_filters (list[FilterLiteralType] | None, optional): filters to grabs stats with. Defaults to None.

    Returns:
        dict[str, MetadataValue]: _description_
    """
    files = pa.array(dt.files(partition_filters=partition_filters))
    files_table = pa.Table.from_arrays([files], names=["path"])
    actions_table = pa.Table.from_batches([dt.get_add_actions(flatten=True)])
    actions_table = actions_table.select(["path", "size_bytes", "num_records"])
    table = files_table.join(actions_table, keys="path")

    stats = {
        "size_MB": MetadataValue.float(
            pc.sum(table.column("size_bytes")).as_py() * 0.00000095367432,  # type: ignore
        ),
        "dagster/row_count": MetadataValue.int(pc.sum(table.column("num_records")).as_py()),  # type: ignore
    }

    return stats


def _table_reader(
    table_slice: TableSlice,
    connection: TableConnection,
    version: Optional[int] = None,
    date_format: Optional[dict[str, str]] = None,
    parquet_read_options: Optional[ds.ParquetReadOptions] = None,
) -> ds.Dataset:
    table = DeltaTable(
        table_uri=connection.table_uri,
        version=version,
        storage_options=connection.storage_options,
    )
    logger = logging.getLogger()
    logger.setLevel("DEBUG")
    logger.debug("Connection timeout duration %s", connection.storage_options.get("timeout"))

    partition_expr = None
    if table_slice.partition_dimensions is not None:
        partition_filters = partition_dimensions_to_dnf(
            partition_dimensions=table_slice.partition_dimensions,
            table_schema=table.schema(),
            input_dnf=True,
            date_format=date_format,
        )
        if partition_filters is not None:
            partition_expr = filters_to_expression([partition_filters])

    logger.debug("Dataset input predicate %s", partition_expr)
    dataset = table.to_pyarrow_dataset(parquet_read_options=parquet_read_options)
    if partition_expr is not None:
        dataset = dataset.filter(expression=partition_expr)

    return dataset


def extract_date_format_from_partition_definition(
    context: Union[OutputContext, InputContext],
) -> Optional[dict[str, str]]:
    """Gets the date format from the partition definition if there is a TimeWindowPartitionsDefinition present (nested or not), to be used to properly compare with columns
    in the delta table which are not a datetime object. Returns None if no TimeWindowPartitionsDefinition were present.
    """
    if isinstance(context, InputContext):
        if context.has_asset_partitions:
            if context.upstream_output is not None:
                partition_expr = context.upstream_output.metadata["partition_expr"]  # type: ignore
                partitions_definition = context.asset_partitions_def
            else:
                raise ValueError(
                    "'partition_expr' should have been set in the metadata of the incoming asset since it has a partition definition.",
                )
        else:
            return None
    elif isinstance(context, OutputContext):
        if context.has_asset_partitions:
            if context.metadata is not None and "partition_expr" in context.metadata:
                partition_expr = context.metadata["partition_expr"]
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
        date_format[partition_expr] = partitions_definition.fmt  # type: ignore
    elif isinstance(partitions_definition, MultiPartitionsDefinition):
        for partition_dims_definition in partitions_definition.partitions_defs:
            if isinstance(
                partition_dims_definition.partitions_def,
                TimeWindowPartitionsDefinition,
            ):
                date_format[partition_expr[partition_dims_definition.name]] = (
                    partition_dims_definition.partitions_def.fmt
                )

    return date_format if len(date_format) else None
