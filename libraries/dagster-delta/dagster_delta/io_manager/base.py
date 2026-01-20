import sys
from abc import abstractmethod
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, TypedDict, Union, cast

from dagster import InputContext, OutputContext
from dagster._config.pythonic_config import ConfigurableIOManagerFactory

try:
    from dagster._core.definitions.partitions.utils import TimeWindow
except ModuleNotFoundError:
    from dagster._core.definitions.time_window_partitions import TimeWindow  # pyright: ignore[reportMissingImports]

from dagster._core.storage.db_io_manager import (
    DbClient,
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
)
from pydantic import Field

from dagster_delta._db_io_manager import CustomDbIOManager

if sys.version_info >= (3, 11):
    from typing import NotRequired
else:
    from typing_extensions import NotRequired

from dagster_delta.config import (
    AzureConfig,
    ClientConfig,
    GcsConfig,
    LocalConfig,
    MergeConfig,
    S3Config,
)

DELTA_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DELTA_DATE_FORMAT = "%Y-%m-%d"


@dataclass(frozen=True)
class TableConnection:  # noqa: D101
    table_uri: str
    storage_options: dict[str, str]
    table_config: Optional[dict[str, str]]


class _StorageOptionsConfig(TypedDict, total=False):
    local: dict[str, str]
    s3: dict[str, str]
    azure: dict[str, str]
    gcs: dict[str, str]


class WriteMode(str, Enum):
    """Deltalake Write mode"""

    error = "error"
    append = "append"
    overwrite = "overwrite"
    ignore = "ignore"
    merge = "merge"
    create_or_replace = "create_or_replace"


class SchemaMode(str, Enum):
    """Deltalake schema mode"""

    overwrite = "overwrite"
    merge = "merge"


class _DeltaTableIOManagerResourceConfig(TypedDict):
    root_uri: str
    mode: WriteMode
    schema_mode: NotRequired[SchemaMode]
    merge_config: NotRequired[dict[str, Any]]
    storage_options: _StorageOptionsConfig
    client_options: NotRequired[dict[str, str]]
    table_config: NotRequired[dict[str, str]]
    writer_properties: NotRequired[dict[str, str]]
    commit_properties: NotRequired[dict[str, str]]
    parquet_read_options: NotRequired[dict[str, Any]]


class BaseDeltaLakeIOManager(ConfigurableIOManagerFactory):
    """Base class for an IO manager definition that reads inputs from and writes outputs to Delta Lake.

    Examples:
    ```python
    from dagster_delta import BaseDeltaLakeIOManager
    from dagster_delta.io_manager.arrow import _DeltaLakePyArrowTypeHandler

    class MyDeltaLakeIOManager(BaseDeltaLakeIOManager):
        @staticmethod
        def type_handlers() -> Sequence[DbTypeHandler]:
            return [_DeltaLakePyArrowTypeHandler()]

    @asset(
        key_prefix=["my_schema"]  # will be used as the schema (parent folder) in Delta Lake
    )
    def my_table() -> pa.Table:  # the name of the asset will be the table name
        ...

    defs = Definitions(
        assets=[my_table],
        resources={"io_manager": MyDeltaLakeIOManager()}
    )
    ```

    If you do not provide a schema, Dagster will determine a schema based on the assets and ops using
    the I/O Manager. For assets, the schema will be determined from the asset key, as in the above example.
    For ops, the schema can be specified by including a "schema" entry in output metadata. If none
    of these is provided, the schema will default to "public".

    ```python

    @op(
        out={"my_table": Out(metadata={"schema": "my_schema"})}
    )
    def make_my_table() -> pa.Table:
        ...
    ```

    To only use specific columns of a table as input to a downstream op or asset, add the metadata "columns" to the
    In or AssetIn.

    ```python
        @asset(
            ins={"my_table": AssetIn("my_table", metadata={"columns": ["a"]})}
        )
        def my_table_a(my_table: pa.Table:
            # my_table will just contain the data from column "a"
            ...
    ```
    """

    root_uri: str = Field(description="Storage location where Delta tables are stored.")
    mode: WriteMode = Field(  # type: ignore
        default=WriteMode.overwrite.value,
        description="The write mode passed to save the output.",
    )
    schema_mode: Optional[SchemaMode] = Field(
        default=None,
        description="If set to 'overwrite', allows replacing the schema of the table. Set to 'merge' to merge with existing schema.",
    )

    merge_config: Optional[MergeConfig] = Field(
        default=None,
        description="Additional configuration for the MERGE operation.",
    )

    storage_options: Union[AzureConfig, S3Config, LocalConfig, GcsConfig] = Field(  # noqa
        discriminator="provider",
        description="Configuration for accessing storage location.",
    )

    client_options: Optional[ClientConfig] = Field(
        default=None,
        description="Additional configuration passed to http client.",
    )

    table_config: Optional[dict[str, str]] = Field(
        default=None,
        description="Additional config and metadata added to table on creation.",
    )

    schema_: Optional[str] = Field(
        default=None,
        alias="schema",
        description="Name of the schema to use.",
    )  # schema is a reserved word for pydantic
    writer_properties: Optional[dict[str, str]] = Field(
        default=None,
        description="Writer properties passed to the rust engine writer.",
    )
    commit_properties: Optional[dict[str, Any]] = Field(
        default=None,
        description="Commit properties, Controls the behaviour of the commit.",
    )
    parquet_read_options: Optional[dict[str, Any]] = Field(
        default=None,
        description="Parquet read options, to be passed to pyarrow.",
    )

    @staticmethod
    @abstractmethod
    def type_handlers() -> Sequence[DbTypeHandler]:  # noqa: D102
        ...

    @staticmethod
    def default_load_type() -> Optional[type]:  # noqa: D102
        return None

    def create_io_manager(self, context) -> CustomDbIOManager:  # noqa: D102, ANN001, ARG002
        return CustomDbIOManager(
            db_client=DeltaLakeDbClient(),
            database="deltalake",
            schema=self.schema_,
            type_handlers=self.type_handlers(),
            default_load_type=self.default_load_type(),
            io_manager_name="DeltaLakeIOManager",
        )


class DeltaLakeDbClient(DbClient):  # noqa: D101
    @staticmethod
    def delete_table_slice(  # noqa: D102
        context: OutputContext,
        table_slice: TableSlice,
        connection: TableConnection,
    ) -> None:
        # deleting the table slice here is a no-op, since we use deltalake's internal mechanism
        # to overwrite table partitions.
        pass

    @staticmethod
    def ensure_schema_exists(  # noqa: D102
        context: OutputContext,
        table_slice: TableSlice,
        connection: TableConnection,
    ) -> None:
        # schemas are just folders and automatically created on write.
        pass

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:  # noqa: D102
        # The select statement here is just for illustrative purposes,
        # and is never actually executed. It does however logically correspond
        # the the operation being executed.
        col_str = ", ".join(table_slice.columns) if table_slice.columns else "*"

        if table_slice.partition_dimensions and len(table_slice.partition_dimensions) > 0:
            query = f"SELECT {col_str} FROM {table_slice.schema}.{table_slice.table} WHERE\n"
            return query + _partition_where_clause(table_slice.partition_dimensions)
        else:
            return f"""SELECT {col_str} FROM {table_slice.schema}.{table_slice.table}"""

    @staticmethod
    @contextmanager
    def connect(  # noqa: D102
        context: Union[OutputContext, InputContext],
        table_slice: TableSlice,
    ) -> Iterator[TableConnection]:
        resource_config = cast(_DeltaTableIOManagerResourceConfig, context.resource_config)
        root_uri = resource_config["root_uri"].rstrip("/")
        storage_options = resource_config["storage_options"]

        # Values of a config are unpacked into a dict[str,Any], we convert it back to the Config
        # so that we can do str_dict()
        if "local" in storage_options:
            storage_options = LocalConfig(**storage_options["local"])  # type: ignore
        elif "s3" in storage_options:
            storage_options = S3Config(**storage_options["s3"])  # type: ignore
        elif "azure" in storage_options:
            storage_options = AzureConfig(**storage_options["azure"])  # type: ignore
        elif "gcs" in storage_options:
            storage_options = GcsConfig(**storage_options["gcs"])  # type: ignore
        else:
            raise NotImplementedError("No valid storage_options config found.")

        client_options = resource_config.get("client_options")
        client_options = (
            ClientConfig(**client_options).str_dict() if client_options is not None else {}  # type: ignore
        )

        options = {**storage_options.str_dict(), **client_options}
        table_config = resource_config.get("table_config")

        # Ignore schema if None or empty string, useful to set schema = "" which overrides the assetkey
        if table_slice.schema:
            table_uri = f"{root_uri}/{table_slice.schema}/{table_slice.table}"
        else:
            table_uri = f"{root_uri}/{table_slice.table}"
        conn = TableConnection(
            table_uri=table_uri,
            storage_options=options or {},
            table_config=table_config,
        )

        yield conn


def _partition_where_clause(
    partition_dimensions: Sequence[TablePartitionDimension],
) -> str:
    return " AND\n".join(
        (
            _time_window_where_clause(partition_dimension)
            if isinstance(partition_dimension.partitions, TimeWindow)
            else _static_where_clause(partition_dimension)
        )
        for partition_dimension in partition_dimensions
    )


def _time_window_where_clause(table_partition: TablePartitionDimension) -> str:
    partition = cast(TimeWindow, table_partition.partitions)
    start_dt, end_dt = partition
    start_dt_str = start_dt.strftime(DELTA_DATETIME_FORMAT)
    end_dt_str = end_dt.strftime(DELTA_DATETIME_FORMAT)
    return f"""{table_partition.partition_expr} >= '{start_dt_str}' AND {table_partition.partition_expr} < '{end_dt_str}'"""


def _static_where_clause(table_partition: TablePartitionDimension) -> str:
    partitions = ", ".join(f"'{partition}'" for partition in table_partition.partitions)
    return f"""{table_partition.partition_expr} in ({partitions})"""
