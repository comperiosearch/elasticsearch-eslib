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

# For Python versions earlier than 2.3 the 'csv' package is also required. This is included in 2.3 and later. Is Pip
# able to conditionally require this package for earlier versions or maybe time to ditch 2.2 and below support?
requires = [
    'elasticsearch',
    'argparse',
    'pika', 'pyrabbit',   # for Rabbitmq
    'HTMLParser',
    'requests',
    'TwitterAPI',
    'PyYAML',             # for prog logging init stuff
    'feedparser',
    'python-dateutil',
    'mock'                # for testing
]


setup(
    name='eslib',
    version='0.0.1',
    description='Document processing framework and utility for Elasticsearch (or whatever).',
    long_description=open("README.md").read(),
    author='Hans Terje Bakke',
    author_email='hans.terje.bakke@comperio.no',
    url='https://github.com/comperiosearch/elasticsearch-eslib',
    packages=['eslib', 'eslib.procs'],
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
