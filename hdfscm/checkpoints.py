import os
import posixpath
from datetime import datetime
from notebook.services.contents.checkpoints import Checkpoints
from tornado.web import HTTPError
from traitlets import Unicode, Instance, default, HasTraits
from pyarrow import hdfs

from .utils import to_fs_path, perm_to_403


CHECKPOINT_ID = "checkpoint"


class NoOpCheckpoints(Checkpoints):
    """A Checkpoints implementation that does nothing.

    Useful if you don't want checkpoints at all."""
    def create_checkpoint(self, contents_mgr, path):
        return {'id': CHECKPOINT_ID,
                'last_modified': datetime.now()}

    def restore_checkpoint(self, contents_mgr, checkpoint_id, path):
        pass

    def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        pass

    def delete_checkpoint(self, checkpoint_id, path):
        pass

    def list_checkpoints(self, path):
        return []


class HdfsCheckpointsMixin(HasTraits):
    fs = Instance(hdfs.HadoopFileSystem)

    @default('fs')
    def _default_fs(self):
        return self.parent.fs

    def no_such_checkpoint(self, path, checkpoint_id):
        raise HTTPError(
            404, 'Checkpoint does not exist: %s@%s' % (path, checkpoint_id)
        )

    def create_checkpoint(self, contents_mgr, path):
        src_path = to_fs_path(path, contents_mgr.root_dir)
        dest_path = self._checkpoint_path(CHECKPOINT_ID, path)
        self._copy_from_manager(src_path, dest_path)
        return self._checkpoint_model(CHECKPOINT_ID, dest_path)

    def restore_checkpoint(self, contents_mgr, checkpoint_id, path):
        src_path = self._checkpoint_path(checkpoint_id, path)
        dest_path = to_fs_path(path, contents_mgr.root_dir)
        self._copy_to_manager(src_path, dest_path)

    def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        old_cp_path = self._checkpoint_path(checkpoint_id, old_path)
        new_cp_path = self._checkpoint_path(checkpoint_id, new_path)
        if self._checkpoint_exists(old_cp_path):
            self.log.debug("Renaming checkpoint %s -> %s",
                           old_cp_path, new_cp_path)
            self._rename(old_cp_path, new_cp_path)

    def delete_checkpoint(self, checkpoint_id, path):
        path = path.strip('/')
        cp_path = self._checkpoint_path(checkpoint_id, path)
        if not self._checkpoint_exists(cp_path):
            self.no_such_checkpoint(path, checkpoint_id)

        self.log.debug("Deleting checkpoint %s", cp_path)
        self._delete(cp_path)

    def list_checkpoints(self, path):
        path = path.strip('/')
        cp_path = self._checkpoint_path(CHECKPOINT_ID, path)
        if not self._checkpoint_exists(cp_path):
            return []
        else:
            return [self._checkpoint_model(CHECKPOINT_ID, cp_path)]


class HdfsCheckpoints(HdfsCheckpointsMixin, Checkpoints):
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

    def _checkpoint_model(self, checkpoint_id, hdfs_path):
        info = self.fs.info(hdfs_path)
        last_modified = datetime.fromtimestamp(info['last_modified'])
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

    def _copy_to_manager(self, src_path, dest_path):
        # TODO: pyarrow.hdfs currently doesn't implement copy, so this is
        # less efficient than it should be.
        with self.fs.open(src_path, 'rb') as source:
            with self.fs.open(dest_path, 'wb') as dest:
                dest.upload(source)

    _copy_from_manager = _copy_to_manager

    def _rename(self, old, new):
        with perm_to_403(old):
            self.fs.rename(old, new)

    def _delete(self, cp_path):
        with perm_to_403(cp_path):
            self.fs.delete(cp_path)

    def _checkpoint_exists(self, cp_path):
        with perm_to_403(cp_path):
            return self.fs.isfile(cp_path)


class LocalFileCheckpoints(HdfsCheckpointsMixin, Checkpoints):
    checkpoint_root_dir = Unicode(
        '.local_ipynb_checkpoints',
        config=True,
        help="""The root directory in which to keep file checkpoints

        This can either be an absolute path, or relative path to the current
        working directory.

        By default, it is .local_ipynb_checkpoints.
        """
    )

    checkpoint_root_dir_abs = Unicode()

    @default('checkpoint_root_dir_abs')
    def _default_checkpoint_root_dir_abs(self):
        return os.path.abspath(self.checkpoint_root_dir)

    def _checkpoint_model(self, checkpoint_id, os_path):
        stats = os.stat(os_path)
        return {'id': checkpoint_id,
                'last_modified': datetime.fromtimestamp(stats.st_mtime)}

    def _checkpoint_path(self, checkpoint_id, path):
        path = path.strip('/')
        parent, name = ('/' + path).rsplit('/', 1)
        parent = parent.strip('/')
        basename, ext = posixpath.splitext(name)
        filename = "{name}-{checkpoint_id}{ext}".format(
            name=basename,
            checkpoint_id=checkpoint_id,
            ext=ext,
        )

        cp_dir = to_fs_path(parent, self.checkpoint_root_dir_abs)

        # Create the directory if it doesn't exist
        with perm_to_403(cp_dir):
            os.makedirs(cp_dir, exist_ok=True)

        return posixpath.join(cp_dir, filename)

    def _copy_from_manager(self, src_path, dest_path):
        with self.fs.open(src_path, 'rb') as source:
            source.download(dest_path)

    def _copy_to_manager(self, src_path, dest_path):
        with open(src_path, 'rb') as source:
            with self.fs.open(dest_path, 'wb') as dest:
                dest.upload(source)

    def _delete_parent_dir_if_empty(self, cp_path):
        cp_dir = os.path.dirname(cp_path)
        if not os.listdir(cp_dir):
            try:
                os.rmdir(cp_dir)
            except Exception as exc:
                self.log.warn(
                    "Failed to delete empty checkpoint directory: %s, %s"
                    % (cp_dir, exc)
                )

    def _rename(self, old, new):
        with perm_to_403(old):
            os.rename(old, new)
            self._delete_parent_dir_if_empty(old)

    def _delete(self, cp_path):
        with perm_to_403(cp_path):
            os.unlink(cp_path)
            self._delete_parent_dir_if_empty(cp_path)

    def _checkpoint_exists(self, cp_path):
        return os.path.isfile(cp_path)
