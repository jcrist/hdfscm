import nbformat
from notebook.services.contents.tests.test_contents_api import (
    APITest, assert_http_error
)
from traitlets.config import Config

from hdfscm import HdfsContentsManager
from hdfscm.utils import to_fs_path

from .conftest import random_root_dir


def bind_sockets_patched(port, address, *args, **kwargs):
    import socket
    try:
        has_ipv6 = socket.has_ipv6
        socket.has_ipv6 = False
        return _orig_bind_sockets(port, address, *args, **kwargs)
    finally:
        socket.has_ipv6 = has_ipv6


import tornado.netutil
import tornado.tcpserver
_orig_bind_sockets = tornado.netutil.bind_sockets
tornado.tcpserver.bind_sockets = bind_sockets_patched


class HdfsContentsAPITest(APITest):
    hidden_dirs = []
    root_dir = random_root_dir()
    config = Config()
    config.NotebookApp.contents_manager_class = HdfsContentsManager
    config.HdfsContentsManager.root_dir = root_dir

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
