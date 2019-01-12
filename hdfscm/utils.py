from contextlib import contextmanager
from datetime import datetime, tzinfo, timedelta

from pyarrow import ArrowIOError
from tornado.web import HTTPError


_ZERO = timedelta(0)


class _utc_tzinfo(tzinfo):
    def utcoffset(self, d):
        return _ZERO

    dst = utcoffset


_UTC = _utc_tzinfo()


def utcfromtimestamp(t):
    return datetime.utcfromtimestamp(t).replace(tzinfo=_UTC)


def utcnow():
    return datetime.now().replace(tzinfo=_UTC)


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


def is_hidden(fs_path, root):
    path = to_api_path(fs_path, root)
    return any(part.startswith('.') for part in path.split("/"))


@contextmanager
def perm_to_403(path):
    try:
        yield
    except ArrowIOError as exc:
        # For now we can't access the errno attribute of the error directly,
        # detect it from the string instead.
        if 'errno: 13 (Permission denied)' in str(exc):
            raise HTTPError(403, 'Permission denied: %s' % path)
