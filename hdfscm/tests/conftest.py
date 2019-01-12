import os
import uuid
import posixpath


def random_root_dir():
    base = os.environ.get('HDFSCM_TESTS_ROOT_DIR', '/user/testuser/')
    suffix = uuid.uuid4().hex
    return posixpath.join(base, 'hdfscm-tests-%s' % suffix)
