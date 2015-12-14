#!/usr/bin/env python

import os
import sys
from glob import glob

# PREREQUISITES:
# yum install -y libxml2-devel libxslt-devel

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

requires = [
    'elasticsearch',
    'lxml',
    'oauthlib',
    'python-daemon',      # For services
    'argparse',
    'psutil', 'setproctitle',
    'pika', 'pyrabbit',   # for Rabbitmq
    'pykafka',            # For Kafka
    'HTMLParser',
    'requests>=2',        # version >=2 needed by TwitterAPI
    'TwitterAPI',
    'PyYAML',             # for prog logging init stuff
    'feedparser',         # for rss
    'python-dateutil',
#    'mock'                # for testing
    'beautifulsoup4',
    'textblob', 'justext' # for web.py
]


setup(
    name='eslib',
    version='0.0.6',
    description='Document processing framework and utility for Elasticsearch (or whatever).',
    #long_description=open("README.md").read(),
    author='Hans Terje Bakke',
    author_email='hans.terje.bakke@comperio.no',
    url='https://github.com/comperiosearch/elasticsearch-eslib',
    keywords="document processing docproc",
    packages=['eslib', 'eslib.procs', 'eslib.service'],
#    package_data={'': ['LICENSE', 'README.md', 'PROTOCOLS.md']},
    scripts=glob('bin/*'),
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
