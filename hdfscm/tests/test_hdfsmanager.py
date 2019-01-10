from notebook.services.contents.tests.test_manager import (
    TestContentsManager
)

from hdfscm import HdfsContentsManager


class HdfsContentsManagerTestCase(TestContentsManager):

    def setUp(self):
        self.contents_manager = HdfsContentsManager()
        self.tearDown()

    def tearDown(self):
        for item in self.contents_manager.fs.ls(self.contents_manager.root_dir):
            self.contents_manager.fs.delete(item, recursive=True)

    def make_dir(self, api_path):
        self.contents_manager.new(
            model={"type": "directory"},
            path=api_path)


del TestContentsManager
