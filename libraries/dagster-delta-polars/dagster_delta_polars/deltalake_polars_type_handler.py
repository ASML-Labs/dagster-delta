import logging
from collections.abc import Sequence
from typing import Any, Optional, Union

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from dagster import InputContext, MetadataValue
from dagster._core.storage.db_io_manager import (
    DbTypeHandler,
    TableSlice,
)
from dagster_delta.handler import (
    DeltalakeBaseArrowTypeHandler,
    DeltaLakePyArrowTypeHandler,
    _table_reader,
    extract_date_format_from_partition_definition,
)
from dagster_delta.io_manager import DeltaLakeIOManager, TableConnection

PolarsTypes = Union[pl.DataFrame, pl.LazyFrame]


class DeltaLakePolarsTypeHandler(DeltalakeBaseArrowTypeHandler[PolarsTypes]):  # noqa: D101
    def from_arrow(  # noqa: D102
        self,
        obj: Union[ds.Dataset, pa.RecordBatchReader],
        target_type: type[PolarsTypes],
    ) -> PolarsTypes:
        if isinstance(obj, pa.RecordBatchReader):
            return pl.DataFrame(obj.read_all())
        elif isinstance(obj, ds.Dataset):
            df = pl.scan_pyarrow_dataset(obj)
            if target_type == pl.DataFrame:
                return df.collect()
            else:
                return df
        else:
            raise NotImplementedError("Unsupported objected passed of type:  %s", type(obj))

    def to_arrow(self, obj: PolarsTypes) -> tuple[pa.RecordBatchReader, dict[str, Any]]:  # noqa: D102
        if isinstance(obj, pl.LazyFrame):
            obj = obj.collect()

        logger = logging.getLogger()
        logger.setLevel("DEBUG")
        logger.debug("shape of dataframe: %s", obj.shape)
        # TODO(ion): maybe move stats collection here

        return obj.to_arrow().to_reader(), {"large_dtypes": True}

    def load_input(
        self,
        context: InputContext,
        table_slice: TableSlice,
        connection: TableConnection,
    ) -> PolarsTypes:
        """Loads the input as a Polars DataFrame or LazyFrame."""
        metadata = context.metadata if context.metadata is not None else {}
        date_format = extract_date_format_from_partition_definition(context)

        parquet_read_options = None
        if context.resource_config is not None:
            parquet_read_options = context.resource_config.get("parquet_read_options", None)
            parquet_read_options = (
                ds.ParquetReadOptions(**parquet_read_options)
                if parquet_read_options is not None
                else None
            )

        dataset = _table_reader(
            table_slice,
            connection,
            version=metadata.get("table_version"),
            date_format=date_format,
            parquet_read_options=parquet_read_options,
        )

        if table_slice.columns is not None:
            if context.dagster_type.typing_type == pl.LazyFrame:
                return self.from_arrow(dataset, context.dagster_type.typing_type).select(
                    table_slice.columns,
                )
            else:
                scanner = dataset.scanner(columns=table_slice.columns)
                return self.from_arrow(scanner.to_reader(), context.dagster_type.typing_type)
        else:
            return self.from_arrow(dataset, context.dagster_type.typing_type)

    def get_output_stats(self, obj: PolarsTypes) -> dict[str, MetadataValue]:
        """Returns output stats to be attached to the the context.

        Args:
            obj (PolarsTypes): LazyFrame or DataFrame

        Returns:
            Mapping[str, MetadataValue]: metadata stats
        """
        stats = {}
        # TODO(ion): think of more meaningful stats to add from a dataframe
        if isinstance(obj, pl.DataFrame):
            stats["num_rows_in_source"] = MetadataValue.int(obj.shape[0])

        return stats

    @property
    def supported_types(self) -> Sequence[type[object]]:
        """Returns the supported dtypes for this typeHandler"""
        return [pl.DataFrame, pl.LazyFrame]


class DeltaLakePolarsIOManager(DeltaLakeIOManager):
    """Base class for an IO manager definition that reads inputs from and writes outputs to Delta Lake.

    Examples:
        .. code-block:: python

            from dagster_delta_polars import DeltaLakePolarsIOManager

            @asset(
                key_prefix=["my_schema"]  # will be used as the schema (parent folder) in Delta Lake
            )
            def my_table() -> pl.DataFrame:  # the name of the asset will be the table name
                ...

            defs = Definitions(
                assets=[my_table],
                resources={"io_manager": DeltaLakePolarsIOManager()}
            )

    If you do not provide a schema, Dagster will determine a schema based on the assets and ops using
    the I/O Manager. For assets, the schema will be determined from the asset key, as in the above example.
    For ops, the schema can be specified by including a "schema" entry in output metadata. If none
    of these is provided, the schema will default to "public".

    .. code-block:: python

        @op(
            out={"my_table": Out(metadata={"schema": "my_schema"})}
        )
        def make_my_table() -> pl.DataFrame:
            ...

    To only use specific columns of a table as input to a downstream op or asset, add the metadata "columns" to the
    In or AssetIn.

    .. code-block:: python

        @asset(
            ins={"my_table": AssetIn("my_table", metadata={"columns": ["a"]})}
        )
        def my_table_a(my_table: pl.DataFrame):
            # my_table will just contain the data from column "a"
            ...

    """

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        """Returns all available type handlers on this IO Manager."""
        return [DeltaLakePolarsTypeHandler(), DeltaLakePyArrowTypeHandler()]

    @staticmethod
    def default_load_type() -> Optional[type]:
        """Grabs the default load type if no type hint is passed."""
        return pl.DataFrame
