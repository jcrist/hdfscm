import mimetypes
from base64 import encodebytes, decodebytes
from contextlib import contextmanager
from datetime import datetime
from getpass import getuser
from posixpath import normpath

import nbformat
from notebook.utils import to_os_path
from notebook.services.contents.manager import ContentsManager
from pyarrow import hdfs, ArrowIOError
from traitlets import Unicode, Integer, default
from tornado.web import HTTPError


@contextmanager
def perm_to_403(self, path):
    try:
        yield
    except (OSError, IOError):
        # For now we can't detect the errno from pyarrow (easily),
        # just treat all errors as permission errors
        raise HTTPError(403, u'Permission denied: %s' % path)


class HdfsContentsManager(ContentsManager):
    """ContentsManager that persists to HDFS rather than the local filesystem.
    """

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
        help="""
        A template string to populate ``root_dir`` from.

        Receive the following format parameters:

        - username
        """,
        default="/user/{username}/notebooks",
        config=True)

    hdfs_host = Unicode(
        help="""
        The hostname of the HDFS namenode.

        By default this will be inferred from the HDFS configuration files.
        """,
        default="default",
        config=True
    )

    hdfs_port = Integer(
        help="""
        The port for the HDFS namenode.

        By default this will be inferred from the HDFS configuration files.
        """,
        default=0,
        config=True
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fs = hdfs.connect(host=self.hdfs_host, port=self.hdfs_port)

    def info_string(self):
        return "Serving notebooks from HDFS directory: %s" % self.root_dir

    def infer_content_type(self, path):
        if path.endswith(".ipynb"):
            return "notebook"
        elif self.dir_exists(path):
            return "directory"
        else:
            return "file"

    def _get_hdfs_path(self, path):
        hdfs_path = normpath(to_os_path(path, self.root_dir))
        if not hdfs_path.startswith(self.root_dir):
            raise HTTPError(404, "%s is outside root directory" % path)
        return hdfs_path

    def is_hidden(self, path):
        return False

    def file_exists(self, path):
        hdfs_path = self._get_hdfs_path(path)
        return self.fs.isfile(hdfs_path)

    def dir_exists(self, path):
        hdfs_path = self._get_hdfs_path(path)
        return self.fs.isdir(hdfs_path)

    def exists(self, path):
        hdfs_path = self._get_hdfs_path(path)
        return self.fs.exists(hdfs_path)

    def _base_model(self, path, hdfs_path):
        if hdfs_path is None:
            hdfs_path = self._get_hdfs_path(path)
        try:
            info = self.fs.info(hdfs_path)
        except ArrowIOError:
            raise HTTPError(404, "Failed to access %s" % path)

        size = info['size']
        timestamp = datetime.fromtimestamp(info['last_modified_time'])
        model = {'name': path.rsplit('/', 1)[-1],
                 'path': path,
                 'last_modified': timestamp,
                 'created': timestamp,
                 'content': None,
                 'format': None,
                 'mimetype': None,
                 'writable': True,
                 'size': size}

        return model

    def _dir_model(self, path, content=True):
        hdfs_path = self._get_hdfs_path(path)

        if not self.fs.isdir(hdfs_path):
            raise HTTPError(404, "Directory does not exist: %s" % path)

        model = self._base_model(path, hdfs_path)
        model['type'] = 'directory'
        model['size'] = None
        if content:
            model['content'] = [self._model_from_info(info)
                                for info in self.fs.listdir(hdfs_path, True)]
            model['format'] = 'json'
        return model

    def _file_model(self, path, content=True, format=None):
        hdfs_path = self._get_hdfs_path(path)

        if not self.fs.isfile(hdfs_path):
            raise HTTPError(404, "File does not exist: %s" % path)

        model = self._base_model(path, hdfs_path)
        model['type'] = 'file'
        mimetype = mimetypes.guess_type(hdfs_path)[0]

        if content:
            content, format = self._read_file(path, hdfs_path, format)
            if mimetype is None:
                mimetype = {
                    'text': 'text/plain',
                    'base64': 'application/octet-stream'
                }[format]

            model.update(
                content=content,
                format=format,
            )

        model['mimetype'] = mimetype

        return model

    def _notebook_model(self, path, content=True):
        hdfs_path = self._get_hdfs_path(path)

        if not self.fs.isfile(hdfs_path):
            raise HTTPError(404, "File does not exist: %s" % path)

        model = self._base_model(path, hdfs_path)
        model['type'] = 'notebook'

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
        with self.fs.open(hdfs_path, 'rb') as f:
            content = f.read()
        try:
            return nbformat.reads(content.decode('utf8'), as_version=4)
        except Exception as e:
            raise HTTPError(400, "Unreadable Notebook: %s\n%r" % (path, e))

    def get(self, path, content=True, type=None, format=None):
        if not self.exists(path):
            raise HTTPError(404, 'No such file or directory: %s' % path)

        hdfs_path = self._get_hdfs_path(path)

        if type is None:
            type = self.infer_content_type(hdfs_path)

        if type == 'directory':
            model = self._dir_model(path, content=content)
        elif type == 'notebook':
            model = self._notebook_model(path, content=content)
        else:
            model = self._file_model(path, content=content, format=format)
        return model

    def _save_directory(self, path, hdfs_path, model):
        if not self.fs.exists(hdfs_path):
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

        with self.fs.open(hdfs_path, 'wb') as f:
            f.write(bcontent)

    def _save_notebook(self, path, hdfs_path, model):
        nb = nbformat.from_dict(model['content'])
        self.check_and_sign(nb, path)
        with self.fs.open(hdfs_path, 'wb') as f:
            nbformat.write(nb, f, version=nbformat.NO_CONVERT)
        self.validate_notebook_model(model)

    def save(self, model, path):
        if 'type' not in model:
            raise HTTPError(400, 'No file type provided')

        typ = model['type']

        if 'content' not in model and typ != 'directory':
            raise HTTPError(400, 'No file content provided')

        hdfs_path = self._get_hdfs_path(path)

        if typ == 'notebook':
            self._save_notebook(path, hdfs_path, model)
        elif typ == 'file':
            self._save_file(path, hdfs_path, model)
        elif typ == 'directory':
            self._save_directory(path, hdfs_path, model)
        else:
            raise HTTPError(400, "Unhandled contents type: %s" % typ)

        return model

    def delete_file(self, path):
        hdfs_path = self._get_hdfs_path(path)

        if not self.fs.exists(hdfs_path):
            raise HTTPError(
                404, 'File or directory does not exist: %s' % path
            )

        if self.fs.isdir(hdfs_path) and self.fs.listdir(hdfs_path):
            raise HTTPError(400, 'Directory %s not empty' % path)

        with perm_to_403(path):
            self.fs.delete(hdfs_path, recursive=True)

    def rename_file(self, old_path, new_path):
        if old_path == new_path:
            return

        old_hdfs_path = self._get_hdfs_path(old_path)
        new_hdfs_path = self._get_hdfs_path(new_path)

        if self.fs.exists(new_hdfs_path):
            raise HTTPError(409, 'File already exists: %s' % new_path)

        # Move the file
        try:
            with perm_to_403():
                self.fs.rename(old_hdfs_path, new_hdfs_path)
        except Exception as e:
            raise HTTPError(
                500, 'Unknown error renaming file: %s\n%s' % (old_path, e)
            )
