"""Packaging boilerplate for tap-criteo."""
import os
from setuptools import find_packages, setup

setup(name='tap-criteo',
      version='0.2.0',
      description='Singer.io tap for extracting data/metadata from the Criteo Marketing API',
      author='judah.rand@fospah.com',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_criteo'],
      include_package_data=True,
      python_requires='>=3.6',
      install_requires=[
          'singer-python==5.9.0',
          'criteo-marketing==1.0.159'
      ],
      extras_require={
          'dev': [
              'tox',
              'pylint'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-criteo=tap_criteo:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_criteo': [
              'schemas/*.json',
              'metadata/*.json'
          ]
      })
