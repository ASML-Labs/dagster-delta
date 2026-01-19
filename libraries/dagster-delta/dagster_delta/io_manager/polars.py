import logging
import re
from collections.abc import Sequence
from typing import Optional, Union

from deltalake import DeltaTable
from polars.lazyframe.opt_flags import DEFAULT_QUERY_OPT_FLAGS

from dagster_delta._handler.utils import (
    create_predicate,
    extract_date_format_from_partition_definition,
    partition_dimensions_to_dnf,
)

try:
    import polars as pl
except ImportError as e:
    raise ImportError(
        "Please install dagster-delta[polars]",
    ) from e
from arro3.core import RecordBatchReader, Table
from dagster import InputContext, MetadataValue, OutputContext
from dagster._core.storage.db_io_manager import (
    DbTypeHandler,
    TableSlice,
)

from dagster_delta._handler.base import (
    DeltalakeBaseArrowTypeHandler,
)
from dagster_delta.io_manager.arrow import _DeltaLakePyArrowTypeHandler
from dagster_delta.io_manager.base import BaseDeltaLakeIOManager, TableConnection

PolarsTypes = Union[pl.DataFrame, pl.LazyFrame]


class _DeltaLakePolarsTypeHandler(DeltalakeBaseArrowTypeHandler[PolarsTypes]):  # noqa: D101
    def from_arrow(  # noqa: D102
        self,
        obj: Union[RecordBatchReader, Table],
        target_type: type[PolarsTypes],
    ) -> PolarsTypes:
        raise NotImplementedError

    def to_arrow(self, obj: PolarsTypes) -> RecordBatchReader:  # noqa: D102
        if isinstance(obj, pl.LazyFrame):
            obj = obj.collect(engine="streaming")

        logger = logging.getLogger()
        logger.setLevel("DEBUG")
        logger.debug("shape of dataframe: %s", obj.shape)
        # TODO(ion): maybe move stats collection here

        if isinstance(obj, pl.DataFrame):
            obj = obj.lazy()

        ldf = obj._ldf.with_optimizations(DEFAULT_QUERY_OPT_FLAGS._pyoptflags)  # type: ignore[reportAttributeAccessIssue]
        stream = ldf.collect_batches(
            engine="streaming",
            maintain_order=True,
            chunk_size=None,
            lazy=True,
        )

        return RecordBatchReader.from_arrow(stream)

    def load_input(
        self,
        context: InputContext,
        table_slice: TableSlice,
        connection: TableConnection,
    ) -> PolarsTypes:
        """Loads the input as a Polars DataFrame or LazyFrame."""
        definition_metadata = (
            context.definition_metadata if context.definition_metadata is not None else {}
        )
        version = definition_metadata.get("table_version")
        table = DeltaTable(
            table_uri=connection.table_uri,
            storage_options=connection.storage_options,
            version=version,
        )

        date_format = extract_date_format_from_partition_definition(context)
        logger = logging.getLogger()
        logger.setLevel("DEBUG")

        predicate = None
        if table_slice.partition_dimensions is not None:
            partition_filters = partition_dimensions_to_dnf(
                partition_dimensions=table_slice.partition_dimensions,
                table_schema=table.schema(),
                date_format=date_format,
            )
            if partition_filters is not None:
                ## Convert partition_filter to predicate
                predicate = create_predicate(partition_filters)

                logger.debug("Dataset input predicate %s", predicate)

        col_select = ",".join(table_slice.columns) if table_slice.columns is not None else "*"
        query = f"SELECT {col_select} FROM tbl"
        if predicate is not None:
            query = f"{query} WHERE {predicate}"

        logger.info(
            "using query: %s",
            re.sub(
                r"\btbl\b",
                context.asset_key.parts[-1] if context.has_asset_key else "tbl",
                query,
            ),
        )

        df = pl.scan_delta(table).sql(query=query, table_name="tbl")

        if context.dagster_type.typing_type == pl.LazyFrame:
            return df
        else:
            return df.collect()

    def handle_output(
        self,
        context: OutputContext,
        table_slice: TableSlice,
        obj: Union[pl.DataFrame, pl.LazyFrame],
        connection: TableConnection,
    ):
        """Writes polars frame as delta table"""
        super().handle_output(context, table_slice, obj, connection)  # type: ignore[reportArgumentType]
        metadata = {**context.consume_logged_metadata()}

        if connection.table_uri.startswith("lakefs://"):
            # We grab the lakefs endpoint from the object storage options
            for key, value in connection.storage_options.items():
                if key.lower() in ["endpoint", "aws_endpoint", "aws_endpoint_url", "endpoint_url"]:
                    metadata["lakefs_link"] = MetadataValue.url(
                        _convert_uri_to_lakefs_link(connection.table_uri, value),
                    )
                    break
        context.add_output_metadata(metadata)

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


class DeltaLakePolarsIOManager(BaseDeltaLakeIOManager):
    """Base class for an IO manager definition that reads inputs from and writes outputs to Delta Lake.

    Examples:
        .. code-block:: python

            from dagster_delta import DeltaLakePolarsIOManager

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
        return [_DeltaLakePolarsTypeHandler(), _DeltaLakePyArrowTypeHandler()]

    @staticmethod
    def default_load_type() -> Optional[type]:
        """Grabs the default load type if no type hint is passed."""
        return pl.DataFrame


def _convert_uri_to_lakefs_link(uri: str, lakefs_base_url: str) -> str:
    """Convert an S3 uri to a link to lakefs"""
    from urllib.parse import quote

    uri = uri[len("lakefs://") :]
    parts = uri.split("/", 2)
    if len(parts) < 3:
        return "https://error-invalid-s3-uri-format"
    repository = parts[0]
    ref = parts[1]
    path = parts[2]
    encoded_path = quote(path + "/")
    https_url = f"{lakefs_base_url.rstrip('/')}/repositories/{repository}/objects?ref={ref}&path={encoded_path}"
    return https_url
