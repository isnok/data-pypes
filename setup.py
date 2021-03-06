#!/usr/bin/env python

#from distutils.core import setup
from setuptools import setup
from setuptools import find_packages

from versioning import get_cmdclass, get_version

import os

# General Info

setup_args = dict(
    name='data-pypes',
    version=get_version(),
    cmdclass=get_cmdclass(),
    description='A micro framework for processing and logging.',
    author='Konstantin Martini',
    author_email='k@tuxcode.org',
    url='https://github.com/isnok/data-pypes',
)


# Data Files

setup_args.update(
    packages=find_packages(),
    zip_safe=False,
)


# Requirements

setup_args.update(
    install_requires=[],
)


# Entry Points

setup_args.update(
    entry_points={},
)


# Techincal Details

setup_args.update(
    keywords=['logging', 'number-crunching'],
    platforms=['Debian/GNU Linux'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        #"License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Topic :: System :: Software Distribution",
        #"Topic :: System :: Systems Administration",
        "Topic :: Utilities",
        #"Topic :: Internet",
        "Intended Audience :: Developers",
        #"Intended Audience :: System Administrators",
        "Operating System :: POSIX",
        "Operating System :: Unix",
        #"Environment :: Console",
    ],
)

if __name__ == '__main__': setup(**setup_args)
