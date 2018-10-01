#!/usr/bin/env python

from setuptools import setup, find_packages
import re

PROJECT = 'compy'
try:
    with open('compy/__init__.py') as init:
        VERSION = re.search("__version__\s*=\s*'(.*)'", init.read(), re.M).group(1)
except (AttributeError, IndexError, OSError, IOError) as e:
    VERSION = ''
REQUIRES = ["gevent",
            "greenlet",
            "pyzmq",
            "amqp",
            "pycrypto",
            "configobj",
            "lxml",
            "Pillow",
            "blockdiag",
            "bottle",
            "xmltodict",
            "jsonschema",
            "bs4",
            "apscheduler",
            "mimeparse"]

try:
     with open('README.rst', 'rt') as readme:
        long_description = readme.read()
except (OSError, IOError) as e:
    long_description = ''

setup(
    name=PROJECT,
    version=VERSION,
    description='Build event pipeline servers with minimal effort.',
    long_description=long_description,
    classifiers=['Development Status :: 5 - Production/Stable',
                 'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
                 'Programming Language :: Python',
                 'Programming Language :: Python :: 2',
                 'Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: 3.3',
                 'Programming Language :: Python :: 3.6',
                 'Intended Audience :: Developers',
                 'Intended Audience :: System Administrators'],
    platforms=['Linux'],
    install_requires=REQUIRES,
    namespace_packages=[],
    test_suite="tests",
    packages=find_packages(include=('compy*',)),
    package_data={'': ['*.txt', '*.rst', '*.xml', '*.xsl', '*.conf']},
    zip_safe=False)
