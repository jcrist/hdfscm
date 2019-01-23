hdfscm
======

A custom ContentsManager_ for `Jupyter Notebooks`_ that stores contents on
HDFS_.

Installation
------------

``hdfscm`` should be installed in the same Python environment as the notebook
server.

Note that for use with JupyterHub_ this means the *user's* environment (which
is not necessarily the same environment run by the JupyterHub server).


**Install with Conda:**

.. code::

    conda install -c conda-forge jupyter-hdfscm

**Install with Pip:**

.. code::

    pip install jupyter-hdfscm

**Install from source:**

.. code::

    pip install git+https://github.com/jcrist/hdfscm.git


Configuration
-------------

To enable, add the following line to your ``jupyter_notebook_config.py``:

.. code-block:: python

    c.NotebookApp.contents_manager_class = 'hdfscm.HDFSContentsManager'

By default notebooks are stored on HDFS at ``'/user/{username}/notebooks'``. To
change this, configure either ``HDFSContentsManager.root_dir_template`` (a
template string) or ``HDFSContentsManager.root_dir`` directly:

.. code-block:: python

    # Example: Store notebooks in /jupyter/notebooks/{username} instead
    c.HDFSContentsManager.root_dir_template = '/jupyter/notebooks/{username}'

For most systems these parameters should be enough, other fields will be
inferred from the environment. Note that if your hadoop cluster has kerberos
enabled, you'll need to have acquired credentials before starting the notebook
server (either through ``kinit``, or distributed as a delegation token).

If you encounter classpath issues initializing the filesystem, refer to the
`pyarrow hdfs documentation`_. In most environments setting
``ARROW_LIBHDFS_DIR`` resolves these issues.

For more information on all configuration options, see :doc:`options`.


Additional Resources
--------------------

If you're interested in ``hdfscm``, you may also be interested in a few
other libraries:

- yarnspawner_: A JupyterHub Spawner_ for launching notebook servers on YARN.
  This can be used in tandem with ``hdfscm`` providing a way to persist
  notebooks between sessions.

- pgcontents_: A Jupyter ContentsManager_ for storing contents in a Postgres_
  database.

- s3contents_: A Jupyter ContentsManager_ for storing contents in an object
  store like S3_ or GCS_.

- pyarrow_: Among other things, this Python library provides the HDFS client
  used for ``hdfscm``.



.. toctree::
    :maxdepth: 2
    :hidden:

    options.rst


.. _ContentsManager: https://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html
.. _Jupyter Notebooks: https://jupyter.org/
.. _HDFS: http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
.. _JupyterHub: https://jupyterhub.readthedocs.io/en/stable/
.. _pyarrow hdfs documentation: https://arrow.apache.org/docs/python/filesystems.html#hadoop-file-system-hdfs
.. _yarnspawner: https://jcrist.github.io/yarnspawner/
.. _spawner: https://github.com/jupyterhub/jupyterhub/wiki/Spawners
.. _pgcontents: https://github.com/quantopian/pgcontents
.. _postgres: https://www.postgresql.org/
.. _s3contents: https://github.com/danielfrg/s3contents
.. _s3: https://aws.amazon.com/s3/
.. _gcs: https://cloud.google.com/storage/
.. _pyarrow: https://arrow.apache.org/docs/python/
