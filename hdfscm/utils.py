from contextlib import contextmanager

from tornado.web import HTTPError


def to_api_path(fs_path, root):
    if fs_path.startswith(root):
        fs_path = fs_path[len(root):]
    parts = fs_path.strip('/').split('/')
    parts = [p for p in parts if p != '']  # remove duplicate splits
    return '/'.join(parts)


def to_fs_path(path, root):
    parts = [root]
    split = path.strip('/').split('/')
    parts.extend(p for p in split if p != '')  # remove duplicate splits
    fs_path = '/'.join(parts)
    if not fs_path.startswith(root):
        raise HTTPError(404, "%s is outside root directory" % path)
    return fs_path


@contextmanager
def perm_to_403(path):
    try:
        yield
    except (OSError, IOError):
        # For now we can't detect the errno from pyarrow (easily),
        # just treat all errors as permission errors
        raise HTTPError(403, 'Permission denied: %s' % path)
