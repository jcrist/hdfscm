import posixpath
from notebook.services.contents.checkpoints import Checkpoints
from tornado.web import HTTPError
from traitlets import Unicode, Instance, default
from pyarrow import hdfs

from .utils import to_fs_path, perm_to_403, utcfromtimestamp, utcnow


__all__ = ('HDFSCheckpoints', 'NoOpCheckpoints')


# Currently only one checkpoint is supported
CHECKPOINT_ID = "checkpoint"


class NoOpCheckpoints(Checkpoints):
    """A Checkpoints implementation that does nothing.

    Useful if you don't want checkpoints at all."""
    def create_checkpoint(self, contents_mgr, path):
        return {'id': CHECKPOINT_ID,
                'last_modified': utcnow()}

    def restore_checkpoint(self, contents_mgr, checkpoint_id, path):
        pass

    def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        pass

    def delete_checkpoint(self, checkpoint_id, path):
        pass

    def list_checkpoints(self, path):
        return []


class HDFSCheckpoints(Checkpoints):
    """A Checkpoints implementation that persists to HDFS"""

    checkpoint_dir = Unicode(
        '.ipynb_checkpoints',
        config=True,
        help="""The directory name in which to keep file checkpoints

        This is a path relative to the file's own directory.

        By default, it is .ipynb_checkpoints
        """
    )

    root_dir = Unicode()

    @default('root_dir')
    def _default_root_dir(self):
        return self.parent.root_dir

    fs = Instance(hdfs.HadoopFileSystem)

    @default('fs')
    def _default_fs(self):
        return self.parent.fs

    def create_checkpoint(self, contents_mgr, path):
        orig_path = to_fs_path(path, contents_mgr.root_dir)
        cp_path = self._checkpoint_path(CHECKPOINT_ID, path)
        self.log.debug("Creating checkpoint %s", cp_path)
        self._copy(orig_path, cp_path)
        return self._checkpoint_model(CHECKPOINT_ID, cp_path)

    def restore_checkpoint(self, contents_mgr, checkpoint_id, path):
        cp_path = self._checkpoint_path(checkpoint_id, path)
        orig_path = to_fs_path(path, contents_mgr.root_dir)
        self.log.debug("Restoring checkpoint %s", cp_path)
        self._copy(cp_path, orig_path)

    def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        old_cp_path = self._checkpoint_path(checkpoint_id, old_path)
        new_cp_path = self._checkpoint_path(checkpoint_id, new_path)
        if self.fs.isfile(old_cp_path):
            self.log.debug("Renaming checkpoint %s -> %s",
                           old_cp_path, new_cp_path)
            self._rename(old_cp_path, new_cp_path)

    def delete_checkpoint(self, checkpoint_id, path):
        path = path.strip('/')
        cp_path = self._checkpoint_path(checkpoint_id, path)
        if not self.fs.isfile(cp_path):
            raise HTTPError(
                404, 'Checkpoint does not exist: %s@%s' % (path, checkpoint_id)
            )
        self.log.debug("Deleting checkpoint %s", cp_path)
        self._delete(cp_path)

    def list_checkpoints(self, path):
        path = path.strip('/')
        cp_path = self._checkpoint_path(CHECKPOINT_ID, path)
        if not self.fs.isfile(cp_path):
            return []
        else:
            return [self._checkpoint_model(CHECKPOINT_ID, cp_path)]

    def _checkpoint_model(self, checkpoint_id, hdfs_path):
        with perm_to_403(hdfs_path):
            info = self.fs.info(hdfs_path)
        last_modified = utcfromtimestamp(info['last_modified'])
        return {'id': checkpoint_id,
                'last_modified': last_modified}

    def _checkpoint_path(self, checkpoint_id, path):
        path = path.strip('/')
        hdfs_path = to_fs_path(path, self.root_dir)
        directory, filename = posixpath.split(hdfs_path)
        name, ext = posixpath.splitext(filename)
        cp_filename = "{name}-{checkpoint_id}{ext}".format(
            name=name,
            checkpoint_id=checkpoint_id,
            ext=ext,
        )
        cp_dir = posixpath.join(directory, self.checkpoint_dir)
        with perm_to_403(cp_dir):
            self.fs.mkdir(cp_dir)
        cp_path = posixpath.join(cp_dir, cp_filename)
        return cp_path

    def _copy(self, src_path, dest_path):
        # TODO: pyarrow.hdfs currently doesn't implement copy, so this is
        # less efficient than it should be.
        with perm_to_403(src_path):
            with self.fs.open(src_path, 'rb') as source:
                with perm_to_403(dest_path):
                    with self.fs.open(dest_path, 'wb') as dest:
                        dest.upload(source)

    def _rename(self, old, new):
        with perm_to_403(old):
            self.fs.rename(old, new)

    def _delete(self, cp_path):
        with perm_to_403(cp_path):
            self.fs.delete(cp_path)
