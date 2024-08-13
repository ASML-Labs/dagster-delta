import logging
import random
import time
from collections.abc import Callable, Generator, Sequence
from typing import Optional, Union
from urllib.parse import quote

from dagster import MetadataValue, OutputContext
from dagster._core.errors import DagsterExecutionHandleOutputError
from dagster._core.storage.db_io_manager import (
    DbTypeHandler,
    TableSlice,
)
from dagster_delta.io_manager import TableConnection
from deltalake._internal import DeltaError
from lakefs.exceptions import ConflictException

from dagster_delta_polars import DeltaLakePolarsIOManager, DeltaLakePolarsTypeHandler
from dagster_delta_polars.deltalake_polars_type_handler import PolarsTypes
from dagster_delta_polars.lakefs_io.lakefs_client_resource import LakeFSClient


def _should_retry(e: Optional[BaseException]) -> bool:
    """Returns true if this exception should be retried, e.g. on network timeouts"""
    if e is None:
        return False
    if isinstance(e, DagsterExecutionHandleOutputError):
        return _should_retry(e.__cause__)
    trigger_strings = ["generic s3 error", "timeout", "operation timed out"]
    return any(trigger_string in str(e).lower() for trigger_string in trigger_strings)


def retry_with_backoff(
    retries: int = 5,
    backoff_in_seconds: int = 1,
    exceptions: Union[type[BaseException], tuple[type[BaseException], ...]] = Exception,
) -> Callable:
    """Exponential backoff decorator"""
    logger = logging.getLogger()
    logger.setLevel("DEBUG")

    def rwb(f: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> None:
            retry_count = 0
            if retry_count > 0:
                logger.debug(f"Retry lakefs merge attempt: {retry_count}")
            while True:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    if retry_count == retries:
                        raise
                    if isinstance(e, DeltaError) and not _should_retry(e):
                        raise
                    sleep = backoff_in_seconds * 2**retry_count + random.uniform(0, 1)
                    time.sleep(sleep)
                    retry_count += 1

        return wrapper

    return rwb


def has_items(generator: Generator) -> bool:
    """Determines whether a generator has any items or not."""
    try:
        next(generator)
        return True
    except StopIteration:
        return False


def convert_s3_uri_to_lakefs_link(s3_uri: str, lakefs_base_url: str) -> str:
    """Convert an S3 uri to a link to lakefs"""
    s3_uri = s3_uri[len("s3://") :]
    parts = s3_uri.split("/", 2)
    if len(parts) < 3:
        return "https://error-invalid-s3-uri-format"
    repository = parts[0]
    ref = parts[1]
    path = parts[2]
    encoded_path = quote(path + "/")
    https_url = f"{lakefs_base_url.rstrip('/')}/repositories/{repository}/objects?ref={ref}&path={encoded_path}"
    return https_url


class DeltaLakePolarsLakeFSTypeHandler(DeltaLakePolarsTypeHandler):
    """This class extends the DeltaLakePolarsTypeHandler with LakeFS branching functionality"""

    def __init__(
        self,
        lakefs_client: LakeFSClient,
        source_branch_name: str,
        lakefs_base_url: Optional[str],
    ):
        self.repository = lakefs_client.repository
        self.source_branch_name = source_branch_name
        self.lakefs_base_url = lakefs_base_url

    @retry_with_backoff(5, 4, exceptions=(ConflictException, DeltaError))
    def handle_output(
        self,
        context: OutputContext,
        table_slice: TableSlice,
        obj: PolarsTypes,
        connection: TableConnection,
    ):
        """Overrides the handle_output of the parent class to add lakefs branching functionality. Will create
        a branch for each write action and then merge the branch, and retry in case of merge conflict.
        """
        logger = logging.getLogger()
        logger.setLevel("DEBUG")

        step_branch_name = f"{self.source_branch_name}-step-jobid-{context.run_id}-asset-{context.asset_key.to_user_string().replace('/','-').replace('_','-')}"[
            0:256
        ]
        self.repository.branch(step_branch_name).create(source_reference=self.source_branch_name)
        try:
            new_table_uri = connection.table_uri.replace(
                self.source_branch_name,
                step_branch_name,
                1,
            )
            new_connection = TableConnection(
                table_uri=new_table_uri,
                storage_options=connection.storage_options,
                table_config=connection.table_config,
            )
            super().handle_output(context, table_slice, obj, new_connection)
            self.repository.branch(step_branch_name).commit(
                f"Materialized asset {context.asset_key.to_user_string()} in job {context.job_name} in run {context.run_id}",
                allow_empty=True,
            )
            if has_items(
                self.repository.branch(self.source_branch_name).diff(
                    step_branch_name,
                    max_amount=1,
                ),
            ):
                self.repository.branch(step_branch_name).merge_into(
                    self.source_branch_name,
                )  # https://github.com/treeverse/lakeFS/issues/7528
        except ConflictException as e:
            context.consume_logged_metadata()
            raise e
        finally:
            self.repository.branch(step_branch_name).delete()
            logger.debug("LakeFS: deleted step branch '%s'", step_branch_name)

        # By default it will take the stepbranch uri but we want the original uri to be logged
        # Since we don't care in the logging it got branched out there.
        metadata = {**context.consume_logged_metadata()}
        metadata["table_uri"] = MetadataValue.path(connection.table_uri)
        if self.lakefs_base_url is not None:
            metadata["lakefs_link"] = MetadataValue.url(
                convert_s3_uri_to_lakefs_link(connection.table_uri, self.lakefs_base_url),
            )

        context.add_output_metadata(metadata)


class DeltaLakePolarsLakeFSIOManager(DeltaLakePolarsIOManager):
    """Extends functionality of DeltaLakePolarsIOManager with lakefs branching"""

    lakefs_client: LakeFSClient
    lakefs_base_url: Optional[str]

    def type_handlers(self) -> Sequence[DbTypeHandler]:  # type: ignore
        """Returns all available type handlers on this IO Manager."""
        source_branch_name = self.root_uri.split("/")[3]
        return [
            DeltaLakePolarsLakeFSTypeHandler(
                lakefs_client=self.lakefs_client,
                source_branch_name=source_branch_name,
                lakefs_base_url=self.lakefs_base_url,
            ),
        ]
