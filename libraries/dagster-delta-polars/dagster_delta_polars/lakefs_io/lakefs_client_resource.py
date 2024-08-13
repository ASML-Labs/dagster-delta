from typing import TYPE_CHECKING, Optional

from dagster import ConfigurableResource, InitResourceContext
from lakefs import Branch, Client, ObjectReader, Repository
from pydantic.config import Extra

if TYPE_CHECKING:
    from lakefs import Client


class LakeFSClient(ConfigurableResource):
    """Wrapper for the lakefs client which is compatible to be used with dagster as a ConfigurableResource"""

    class Config:  # type: ignore
        """Configure pydantic to allow modification of this object"""

        frozen = False
        extra = Extra.allow

    repository_name: str
    host: str
    access_key_id: str
    secret_access_key: str
    current_branch_name: str

    def setup_for_execution(self, context: Optional[InitResourceContext]) -> "LakeFSClient":  # type: ignore #noqa
        """This method is executed before the dagster pipeline runs."""
        self.client = self._create_lakefs_client()
        self.repository = Repository(self.repository_name, client=self.client)
        self.current_branch = self.repository.branch(self.current_branch_name)
        return self

    def download_file(self, remote_path: str, local_path: str, branch: Optional[Branch] = None):
        """Download a file from LakeFS to the local filesystem"""
        if branch is None:
            branch = self.current_branch
        chunk_size_mb = 50
        reader: ObjectReader = branch.object(remote_path).reader()  # type: ignore
        with open(local_path, "wb") as f:
            while True:
                chunk_bytes = reader.read(1024 * 1024 * chunk_size_mb)
                if not len(chunk_bytes):
                    break
                if isinstance(chunk_bytes, bytes):
                    f.write(chunk_bytes)
                else:
                    raise Exception("Unknown data type for chunk_bytes")

    def _create_lakefs_client(self) -> "Client":
        """Returns a lakefs client object for interacting with the simba lakefs repository."""
        clt = Client(
            host=self.host,
            username=self.access_key_id,
            password=self.secret_access_key,
        )
        return clt
