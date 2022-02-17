import os
from typing import List

from setuptools import find_packages, setup


def readlines(path: str) -> List[str]:
    with open(path, "r", encoding="utf8") as f:
        return [x.strip() for x in f.readlines() if x.strip() and not x.startswith("#")]


setup(
    version=os.getenv("VERSION", "0.3.0"),
    python_requires=">=3.6",
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    install_requires=[],
    extras_require={"plot": ["tqdm==4.62.3", "pandas==1.1.5", "plotnine==0.8.0"]},
    # entry_points={
    #     "console_scripts": [
    #         f"{PWD.name} = {PWD.name}.__main__:main",
    #     ],
    # },
)
