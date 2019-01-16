# https://realpython.com/pypi-publish-python-package

import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()


setup(
    name="ezpq",
    version="0.1.0",
    download_url = 'https://github.com/dm3ll3n/ezpq/archive/0.1.0.tar.gz',
    description="an easy parallel queueing system",
    long_description=README,
    long_description_content_type='text/markdown',
    url="https://www.github.com/dm3ll3n/ezpq",
    author="Donald Mellenbruch",
    author_email="dmellenbruch@outlook.com",
    license="MIT",
    packages=["ezpq"],
    keywords = ['Parallel', 'Queue'],
    include_package_data=True,
    install_requires=[],
    extras_require={
        'plot': ['numpy', 'pandas', 'matplotlib', 'plotnine'] # ['numpy>=1.15.2', 'pandas>=0.23.4', 'matplotlib>=3.0.0', 'plotnine>=0.5.1']
    },
)
