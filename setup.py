#!/usr/bin/env python

from setuptools import setup

setup(name='tap-clubspeed',
      version='0.2.1',
      description='Singer.io tap for extracting data from the Clubspeed API',
      author='lambtron',
      author_email="andyjiang@gmail.com",
      url='https://andyjiang.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_clubspeed'],
      install_requires=[
          'singer-python==5.1.5',
          'requests==2.19.1'
      ],
      entry_points='''
          [console_scripts]
          tap-clubspeed=tap_clubspeed:main
      ''',
      packages=['tap_clubspeed'],
      include_package_data=True,
)
