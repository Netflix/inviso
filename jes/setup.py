#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='InvisoJES',
      version='0.1.0',
      description='Job Event Service',
      author_email='bigdataplatform@netflix.net',
      install_requires=[
          'elasticsearch>=0.4.4',
          'boto>=2.15.0',
          'pytz>=2013.8',
          'requests>=0.14.1',
          'python-dateutil>=2.2',
          'PyDSE>=0.4.32dev',
          'futures==2.1.6',
          'MySQL-python>=1.2.5',
          'arrow>=0.4.2',
          'snakebite>=2.3.5'],
      packages=find_packages(),
      scripts=[
          'scripts/jes-hdfs.py',
          'scripts/jes_mr1_s3.py',
          'scripts/jes_mr2_s3.py',
          'scripts/index_cluster_stats.py',
          'scripts/index_archive.py'],
      py_modules=['inviso', ],
      invlude_package_data=True,
      zip_safe=False,
)
