#!/usr/bin/env python

import os
import sys

import eslib

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

requires = [
    'csv',
    'elasticsearch',
    'argparse',
    'pika', 'pyrabbit',   # for Rabbitmq
    'html.parser',
    'requests',
    'TwitterAPI',
    'PyYAML',             # for prog logging init stuff
    'feedparser'
]


setup(
    name='eslib',
    version=eslib.__version__,
    description='Document processing framework and utility for Elasticsearch (or whatever).',
    long_description=open("README.md").read(),
    author='Hans Terje Bakke',
    author_email='hans.terje.bakke@comperio.no',
    url='https://github.com/comperiosearch/elasticsearch-eslib',
    packages=['eslib'],
    package_dir={'eslib': 'eslib'},
    package_data={'': ['LICENSE', 'README.md', 'PROTOCOLS.md']},
    include_package_data=True,
    install_requires=requires,
    license='Apache 2.0',
    zip_safe=False,

    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    )
)
