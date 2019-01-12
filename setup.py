import versioneer
from setuptools import setup

with open('README.rst') as f:
    long_description = f.read()

setup(name='jupyter-hdfscm',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      license='BSD',
      maintainer='Jim Crist',
      maintainer_email='jiminy.crist@gmail.com',
      description='A Jupyter ContentsManager for HDFS',
      long_description=long_description,
      url='http://github.com/jcrist/hdfscm',
      project_urls={
          'Source': 'https://github.com/jcrist/hdfscm',
          'Issue Tracker': 'https://github.com/jcrist/hdfscm/issues'
      },
      keywords='jupyter contentsmanager HDFS Hadoop',
      classifiers=['Topic :: System :: Systems Administration',
                   'Topic :: System :: Distributed Computing',
                   'License :: OSI Approved :: BSD License',
                   'Programming Language :: Python',
                   'Programming Language :: Python :: 3'],
      packages=['hdfscm'],
      python_requires='>=3.5',
      install_requires=['notebook>=4.0', 'pyarrow>=0.9.0'])
