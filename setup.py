#!/usr/bin/env python

import os
import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

requires = [
    'elasticsearch',
    'elasticsearch-curator',
    'argparse',
    'pika', 'pyrabbit',   # for Rabbitmq
    'HTMLParser',
    'requests',
    'TwitterAPI',
    'PyYAML',             # for prog logging init stuff
    'feedparser',         # for rss
    'python-dateutil',
    'mock'                # for testing
]


setup(
    name='eslib',
    version='0.0.1',
    description='Document processing framework and utility for Elasticsearch (or whatever).',
    #long_description=open("README.md").read(),
    author='Hans Terje Bakke',
    author_email='hans.terje.bakke@comperio.no',
    url='https://github.com/comperiosearch/elasticsearch-eslib',
    keywords="document processing docproc",
    packages=['eslib', 'eslib.procs', 'eslib.service'],
    package_data={'': ['LICENSE', 'README.md', 'PROTOCOLS.md']},
    include_package_data=True,
    # TODO: examples in package data
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
        'Programming Language :: Python :: 2.7'
    )
)
