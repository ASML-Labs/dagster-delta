from collections.abc import Sequence
from typing import Union

import dagster as dg
from arro3.core import RecordBatchReader, Table
from arro3.core.types import ArrowArrayExportable, ArrowStreamExportable
from dagster._core.storage.db_io_manager import DbTypeHandler
from deltalake.schema import Schema
from deltalake.writer._conversion import _convert_arro3_schema_to_delta

from dagster_delta._handler.base import ArrowTypes, DeltalakeBaseArrowTypeHandler
from dagster_delta.io_manager.base import (
    BaseDeltaLakeIOManager as BaseDeltaLakeIOManager,
)


class DeltaLakePyarrowIOManager(BaseDeltaLakeIOManager):  # noqa: D101
    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:  # noqa: D102
        return [_DeltaLakePyArrowTypeHandler()]


class _DeltaLakePyArrowTypeHandler(DeltalakeBaseArrowTypeHandler[ArrowTypes]):  # noqa: D101
    def from_arrow(
        self,
        obj: Union[ArrowStreamExportable, ArrowArrayExportable],
        target_type: type[ArrowTypes],  # type: ignore
    ) -> ArrowTypes:  # noqa: D102 # type: ignore
        data = RecordBatchReader.from_arrow(obj)

        if target_type == Table:
            return data.read_all()

        try:
            import pyarrow as pa

            if target_type == pa.Table:
                return pa.table(data)
            elif target_type == pa.RecordBatchReader:
                return pa.RecordBatchReader.from_stream(data)
        except ImportError:
            pass
        return data

    def to_arrow(
        self,
        obj: Union[ArrowStreamExportable, ArrowArrayExportable],
    ) -> ArrowStreamExportable:  # noqa: D102
        return RecordBatchReader.from_arrow(obj)

    def get_delta_schema(self, obj: Union[ArrowStreamExportable, ArrowArrayExportable]) -> Schema:
        return Schema.from_arrow(
            _convert_arro3_schema_to_delta(RecordBatchReader.from_arrow(obj).schema),
        )

    def get_output_stats(self, obj: ArrowTypes) -> dict[str, dg.MetadataValue]:  # noqa: ARG002 # type: ignore
        """Returns output stats to be attached to the the context.

        Args:
            obj (ArrowTypes): Union[pa.Table, pa.RecordBatchReader, ds.Dataset]

        Returns:
            Mapping[str, MetadataValue]: metadata stats
        """
        return {}

    @property
    def supported_types(self) -> Sequence[type[object]]:
        """Returns the supported dtypes for this typeHandler"""
        supported_types = [Table, RecordBatchReader]
        try:
            import pyarrow as pa

            supported_types.extend([pa.Table, pa.RecordBatchReader])
        except ImportError:
            pass

        return supported_types
