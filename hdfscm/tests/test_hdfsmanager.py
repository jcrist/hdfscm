from notebook.services.contents.tests.test_manager import (
    TestContentsManager
)

from hdfscm import HdfsContentsManager, NoOpCheckpoints

from .conftest import random_root_dir


class HdfsContentsManagerTestCase(TestContentsManager):

    def setUp(self):
        self.root_dir = random_root_dir()
        self.contents_manager = HdfsContentsManager(root_dir=self.root_dir)

    def tearDown(self):
        self.contents_manager.fs.delete(self.root_dir, recursive=True)

    def make_dir(self, api_path):
        self.contents_manager.new(
            model={"type": "directory"},
            path=api_path)


class HdfsContentsManagerNoOpCheckpointsTestCase(HdfsContentsManagerTestCase):

    def setUp(self):
        self.root_dir = random_root_dir()
        self.contents_manager = HdfsContentsManager(
            root_dir=self.root_dir,
            checkpoints_class=NoOpCheckpoints
        )


del TestContentsManager
