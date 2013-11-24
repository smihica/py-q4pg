# -*- coding: utf-8 -*-
from distutils.core import setup, Extension

setup(
    name = 'py-q4pg',
    py_modules = ['q4pg'],
    version = '0.0.9',
    description = 'A simple transactional message queue using PostgreSQL in Python.',
    author='Shin Aoyama',
    author_email = "smihica@gmail.com",
    url = "https://github.com/smihica/py-q4pg",
    download_url = "",
    keywords = ["db", "queue", "psql"],
    install_requires = ['psycopg2'],
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
        ],
    long_description = """\
A simple transactional message queue using PostgreSQL in Python.
""",
    )
