import nbformat
from notebook.services.contents.tests.test_contents_api import (
    APITest, assert_http_error
)
from traitlets.config import Config

from hdfscm import HDFSContentsManager
from hdfscm.utils import to_fs_path

from .conftest import random_root_dir


class HDFSContentsAPITest(APITest):
    hidden_dirs = []
    root_dir = random_root_dir()
    config = Config()
    config.NotebookApp.contents_manager_class = HDFSContentsManager
    config.HDFSContentsManager.root_dir = root_dir

    @classmethod
    def setUpClass(cls):
        """Due to https://github.com/docker/for-linux/issues/250, tornado maps
        localhost to an unresolvable ipv6 address. The easiest way to workaround
        this is to make it look like python was built without ipv6 support. This
        patch could fail if `tornado.netutils.bind_sockets` is updated. Note
        that this doesn't indicate a problem with real world use."""
        import socket
        cls._has_ipv6 = socket.has_ipv6
        socket.has_ipv6 = False
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        """See setUpClass above"""
        import socket
        socket.has_ipv6 = cls._has_ipv6
        super().tearDownClass()

    def setUp(self):
        self.notebook.contents_manager.ensure_root_directory()
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.fs.delete(self.root_dir, recursive=True)

    @property
    def fs(self):
        return self.notebook.contents_manager.fs

    def get_hdfs_path(self, api_path):
        return to_fs_path(api_path, self.root_dir)

    def make_dir(self, api_path):
        self.fs.mkdir(self.get_hdfs_path(api_path))

    def make_blob(self, api_path, blob):
        hdfs_path = self.get_hdfs_path(api_path)
        with self.fs.open(hdfs_path, 'wb') as f:
            f.write(blob)

    def make_txt(self, api_path, txt):
        self.make_blob(api_path, txt.encode('utf-8'))

    def make_nb(self, api_path, nb):
        self.make_txt(api_path, nbformat.writes(nb, version=4))

    def delete_file(self, api_path):
        hdfs_path = self.get_hdfs_path(api_path)
        if self.fs.exists(hdfs_path):
            self.fs.delete(hdfs_path, recursive=True)

    delete_dir = delete_file

    def isfile(self, api_path):
        return self.fs.isfile(self.get_hdfs_path(api_path))

    def isdir(self, api_path):
        return self.fs.isdir(self.get_hdfs_path(api_path))

    # Test overrides.
    def test_checkpoints_separate_root(self):
        pass

    def test_delete_non_empty_dir(self):
        with assert_http_error(400):
            self.api.delete('å b')


del APITest
