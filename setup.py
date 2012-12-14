#!/usr/bin/env python

from os.path import exists
try:
    # Use setup() from setuptools(/distribute) if available
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from clojure.main import VERSION


setup(name='clojure_py',
      version=VERSION,
      author='Timothy Baldridge',
      author_email='tbaldridge@gmail.com',
      packages=['clojure', 'clojure/lang', 'clojure/util'],
      package_data={'clojure': ['core.clj', 'core-deftype.clj', 'core-multimethod.clj']},
      scripts=[],
      url='https://github.com/halgari/clojure-py',
      license='Eclipse 1.0',
      description='Clojure implemented on top of Python',
      long_description=open('README.md').read() if exists("README.md") else "",
      entry_points=dict(console_scripts=['clojurepy=clojure.main:main']),
      install_requires=[],
)
