import mimetypes
from base64 import encodebytes, decodebytes
from getpass import getuser
from urllib.parse import urlsplit

import nbformat
from notebook.services.contents.manager import ContentsManager
from pyarrow import hdfs, ArrowIOError
from traitlets import Unicode, Integer, Bool, default
from tornado.web import HTTPError

from .checkpoints import HDFSCheckpoints
from .utils import (to_fs_path, to_api_path, is_hidden, perm_to_403,
                    utcfromtimestamp)


class HDFSContentsManager(ContentsManager):
    """A ContentsManager implementation that persists to HDFS."""

    root_dir = Unicode(
        help="""
        The root directory to serve from.

        By default this is populated by ``root_dir_template``.
        """,
        config=True
    )

    @default('root_dir')
    def _default_root_dir(self):
        return self.root_dir_template.format(
            username=getuser()
        )

    root_dir_template = Unicode(
        default_value="/user/{username}/notebooks",
        config=True,
        help="""
        A template string to populate ``root_dir`` from.

        Receive the following format parameters:

        - username
        """
    )

    create_root_dir_on_startup = Bool(
        default_value=True,
        config=True,
        help="Create ``root_dir`` on startup if it doesn't already exist"
    )

    hdfs_host = Unicode(
        default_value="default",
        config=True,
        help="""
        The hostname of the HDFS namenode.

        By default this will be inferred from the HDFS configuration files.
        """
    )

    hdfs_port = Integer(
        default_value=0,
        config=True,
        help="""
        The port for the HDFS namenode.

        By default this will be inferred from the HDFS configuration files.
        """
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.debug("Connecting to HDFS at %s:%d",
                       self.hdfs_host, self.hdfs_port)
        self.fs = hdfs.connect(host=self.hdfs_host, port=self.hdfs_port)
        if self.create_root_dir_on_startup:
            self.ensure_root_directory()

    def ensure_root_directory(self):
        self.log.debug("Creating root notebooks directory: %s", self.root_dir)
        self.fs.mkdir(self.root_dir)

    def _checkpoints_class_default(self):
        return HDFSCheckpoints

    def info_string(self):
        return "Serving notebooks from HDFS directory: %s" % self.root_dir

    def infer_type(self, path):
        if path.endswith(".ipynb"):
            return "notebook"
        elif self.fs.isdir(path):
            return "directory"
        else:
            return "file"

    def is_hidden(self, path):
        hdfs_path = to_fs_path(path, self.root_dir)
        return is_hidden(hdfs_path, self.root_dir)

    def file_exists(self, path):
        hdfs_path = to_fs_path(path, self.root_dir)
        return self.fs.isfile(hdfs_path)

    def dir_exists(self, path):
        hdfs_path = to_fs_path(path, self.root_dir)
        return self.fs.isdir(hdfs_path)

    def exists(self, path):
        hdfs_path = to_fs_path(path, self.root_dir)
        return self.fs.exists(hdfs_path)

    def _info_and_check_kind(self, path, hdfs_path, kind):
        try:
            with perm_to_403(path):
                info = self.fs.info(hdfs_path)
        except ArrowIOError:
            raise HTTPError(404, "%s does not exist: %s"
                            % (kind.capitalize(), path))

        if info['kind'] != kind:
            raise HTTPError(400, "%s is not a %s" % (path, kind))
        return info

    def _model_from_info(self, info, type=None):
        if 'name' in info:
            hdfs_path = info['name']
            timestamp = info['last_modified_time']
        else:
            # info from `ls` is different for some reason
            hdfs_path = urlsplit(info['path']).path
            timestamp = info['last_modified']

        path = to_api_path(hdfs_path, self.root_dir)
        name = path.rsplit('/', 1)[-1]

        if type is None:
            if info['kind'] == 'directory':
                type = 'directory'
            elif path.endswith('.ipynb'):
                type = 'notebook'
            else:
                type = 'file'

        mimetype = mimetypes.guess_type(path)[0] if type == 'file' else None
        size = info['size'] if type != 'directory' else None
        timestamp = utcfromtimestamp(timestamp)
        model = {'name': name,
                 'path': path,
                 'last_modified': timestamp,
                 'created': timestamp,
                 'type': type,
                 'size': size,
                 'mimetype': mimetype,
                 'content': None,
                 'format': None,
                 'writable': True}

        return model

    def _dir_model(self, path, hdfs_path, content):
        info = self._info_and_check_kind(path, hdfs_path, 'directory')
        model = self._model_from_info(info, 'directory')
        if content:
            with perm_to_403(path):
                records = self.fs.ls(hdfs_path, True)
            contents = [self._model_from_info(i) for i in records]
            # Filter out hidden files/directories
            # These are rare, so do this after generating contents, not before
            model['content'] = [c for c in contents
                                if self.should_list(c['name']) and not
                                c['name'].startswith('.')]
            model['format'] = 'json'
        return model

    def _file_model(self, path, hdfs_path, content, format):
        info = self._info_and_check_kind(path, hdfs_path, 'file')
        model = self._model_from_info(info, 'file')

        if content:
            content, format = self._read_file(path, hdfs_path, format)
            if model['mimetype'] is None:
                model['mimetype'] = {
                    'text': 'text/plain',
                    'base64': 'application/octet-stream'
                }[format]

            model.update(
                content=content,
                format=format,
            )

        return model

    def _notebook_model(self, path, hdfs_path, content=True):
        info = self._info_and_check_kind(path, hdfs_path, 'file')
        model = self._model_from_info(info, 'notebook')

        if content:
            contents = self._read_notebook(path, hdfs_path)
            self.mark_trusted_cells(contents, path)
            model['content'] = contents
            model['format'] = 'json'
            self.validate_notebook_model(model)

        return model

    def _read_file(self, path, hdfs_path, format):
        if not self.fs.isfile(hdfs_path):
            raise HTTPError(400, "Cannot read non-file %s" % path)

        with perm_to_403(path):
            with self.fs.open(hdfs_path, 'rb') as f:
                bcontent = f.read()

        if format is None:
            try:
                return bcontent.decode('utf8'), 'text'
            except UnicodeError:
                return encodebytes(bcontent).decode('ascii'), 'base64'
        elif format == 'text':
            try:
                return bcontent.decode('utf8'), 'text'
            except UnicodeError:
                raise HTTPError(400, "%s is not UTF-8 encoded" % path,
                                reason='bad format')
        else:
            return encodebytes(bcontent).decode('ascii'), 'base64'

    def _read_notebook(self, path, hdfs_path):
        with perm_to_403(path):
            with self.fs.open(hdfs_path, 'rb') as f:
                content = f.read()
        try:
            return nbformat.reads(content.decode('utf8'), as_version=4)
        except Exception as e:
            raise HTTPError(400, "Unreadable Notebook: %s\n%r" % (path, e))

    def get(self, path, content=True, type=None, format=None):
        hdfs_path = to_fs_path(path, self.root_dir)

        if not self.fs.exists(hdfs_path):
            raise HTTPError(404, 'No such file or directory: %s' % path)
        elif not self.allow_hidden and is_hidden(hdfs_path, self.root_dir):
            self.log.debug("Refusing to serve hidden directory %r", hdfs_path)
            raise HTTPError(404, 'No such file or directory: %s' % path)

        if type is None:
            type = self.infer_type(hdfs_path)

        if type == 'directory':
            model = self._dir_model(path, hdfs_path, content)
        elif type == 'notebook':
            model = self._notebook_model(path, hdfs_path, content)
        else:
            model = self._file_model(path, hdfs_path, content, format)
        return model

    def _save_directory(self, path, hdfs_path, model):
        if not self.allow_hidden and is_hidden(hdfs_path, self.root_dir):
            raise HTTPError(400, 'Cannot create hidden directory %r' % path)

        if not self.fs.exists(hdfs_path):
            self.log.debug("Creating directory at %s", hdfs_path)
            with perm_to_403(path):
                self.fs.mkdir(hdfs_path)
        elif not self.fs.isdir(hdfs_path):
            raise HTTPError(400, 'Not a directory: %s' % path)

    def _save_file(self, path, hdfs_path, model):
        format = model['format']
        content = model['content']

        if format not in {'text', 'base64'}:
            raise HTTPError(
                400,
                "Must specify format of file contents as 'text' or 'base64'",
            )
        try:
            if format == 'text':
                bcontent = content.encode('utf8')
            else:
                b64_bytes = content.encode('ascii')
                bcontent = decodebytes(b64_bytes)
        except Exception as e:
            raise HTTPError(400, 'Encoding error saving %s: %s' % (path, e))

        self.log.debug("Saving file to %s", hdfs_path)
        with perm_to_403(path):
            with self.fs.open(hdfs_path, 'wb') as f:
                f.write(bcontent)

    def _save_notebook(self, path, hdfs_path, model):
        nb = nbformat.from_dict(model['content'])
        self.check_and_sign(nb, path)
        content = nbformat.writes(nb, version=nbformat.NO_CONVERT)
        bcontent = content.encode('utf8')
        self.log.debug("Saving notebook to %s", hdfs_path)
        with perm_to_403(path):
            with self.fs.open(hdfs_path, 'wb') as f:
                f.write(bcontent)
        self.validate_notebook_model(model)
        return model.get('message')

    def save(self, model, path):
        if 'type' not in model:
            raise HTTPError(400, 'No file type provided')

        typ = model['type']

        if 'content' not in model and typ != 'directory':
            raise HTTPError(400, 'No file content provided')

        hdfs_path = to_fs_path(path, self.root_dir)

        message = None
        if typ == 'notebook':
            message = self._save_notebook(path, hdfs_path, model)
        elif typ == 'file':
            self._save_file(path, hdfs_path, model)
        elif typ == 'directory':
            self._save_directory(path, hdfs_path, model)
        else:
            raise HTTPError(400, "Unhandled contents type: %s" % typ)

        model = self.get(path, type=model["type"], content=False)
        if message is not None:
            model['message'] = message

        return model

    def _is_dir_empty(self, path, hdfs_path):
        with perm_to_403(path):
            files = self.fs.ls(hdfs_path)
        if not files:
            return True
        cp_dir = getattr(self.checkpoints, 'checkpoint_dir', None)
        files = {f.rsplit('/', 1)[-1] for f in files} - {cp_dir}
        return not files

    def delete_file(self, path):
        hdfs_path = to_fs_path(path, self.root_dir)

        if not self.fs.exists(hdfs_path):
            raise HTTPError(
                404, 'File or directory does not exist: %s' % path
            )

        if self.fs.isdir(hdfs_path):
            if not self._is_dir_empty(path, hdfs_path):
                raise HTTPError(400, 'Directory %s not empty' % path)
            self.log.debug("Deleting directory at %s", hdfs_path)
            with perm_to_403(path):
                self.fs.delete(hdfs_path, recursive=True)
        else:
            self.log.debug("Deleting file at %s", hdfs_path)
            with perm_to_403(path):
                self.fs.delete(hdfs_path)

    def rename_file(self, old_path, new_path):
        if old_path == new_path:
            return

        old_hdfs_path = to_fs_path(old_path, self.root_dir)
        new_hdfs_path = to_fs_path(new_path, self.root_dir)

        if self.fs.exists(new_hdfs_path):
            raise HTTPError(409, 'File already exists: %s' % new_path)

        # Move the file
        self.log.debug("Renaming %s -> %s", old_hdfs_path, new_hdfs_path)
        try:
            with perm_to_403(old_path):
                self.fs.rename(old_hdfs_path, new_hdfs_path)
        except HTTPError:
            raise
        except Exception as e:
            raise HTTPError(
                500, 'Unknown error renaming file: %s\n%s' % (old_path, e)
            )
