from notebook.services.contents.tests.test_manager import (
    TestContentsManager
)

from hdfscm import HDFSContentsManager, NoOpCheckpoints

from .conftest import random_root_dir


class HDFSContentsManagerTestCase(TestContentsManager):

    def setUp(self):
        self.root_dir = random_root_dir()
        self.contents_manager = HDFSContentsManager(root_dir=self.root_dir)

    def tearDown(self):
        self.contents_manager.fs.delete(self.root_dir, recursive=True)
        self.contents_manager.fs.close()

    def make_dir(self, api_path):
        self.contents_manager.new(
            model={"type": "directory"},
            path=api_path)


class HDFSContentsManagerNoOpCheckpointsTestCase(HDFSContentsManagerTestCase):

    def setUp(self):
        self.root_dir = random_root_dir()
        self.contents_manager = HDFSContentsManager(
            root_dir=self.root_dir,
            checkpoints_class=NoOpCheckpoints
        )

    def tearDown(self):
        self.contents_manager.fs.close()


del TestContentsManager
