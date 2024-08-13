from collections.abc import Sequence

from dagster._core.storage.db_io_manager import DbTypeHandler

from .config import (
    AzureConfig as AzureConfig,
)
from .config import (
    ClientConfig as ClientConfig,
)
from .config import (
    LocalConfig as LocalConfig,
)
from .config import (
    MergeConfig as MergeConfig,
)
from .config import (
    MergeType as MergeType,
)
from .config import (
    S3Config as S3Config,
)
from .handler import (
    DeltalakeBaseArrowTypeHandler as DeltalakeBaseArrowTypeHandler,
)
from .handler import (
    DeltaLakePyArrowTypeHandler as DeltaLakePyArrowTypeHandler,
)
from .io_manager import (
    DELTA_DATE_FORMAT as DELTA_DATE_FORMAT,
)
from .io_manager import (
    DELTA_DATETIME_FORMAT as DELTA_DATETIME_FORMAT,
)
from .io_manager import (
    DeltaLakeIOManager as DeltaLakeIOManager,
)
from .io_manager import (
    WriteMode as WriteMode,
)
from .io_manager import (
    WriterEngine as WriterEngine,
)
from .resource import DeltaTableResource as DeltaTableResource


class DeltaLakePyarrowIOManager(DeltaLakeIOManager):  # noqa: D101
    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:  # noqa: D102
        return [DeltaLakePyArrowTypeHandler()]
