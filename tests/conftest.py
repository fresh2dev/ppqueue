import os
import re
import shutil
from pathlib import Path
from typing import Dict, Generator, Pattern

import pytest
from _pytest.config import Config
from packaging.version import VERSION_PATTERN


@pytest.fixture(name="root_dir", scope="session")
def fixture_root_dir(pytestconfig: Config) -> Path:
    test_dir: Path
    test_paths: None | str | list[str] = pytestconfig.inicfg.get("testpaths")
    if not test_paths:
        default_dir: Path = Path("tests")
        if not pytestconfig.args:
            test_dir = default_dir
        else:
            first_arg: Path = Path(pytestconfig.args[0])
            test_dir = first_arg if first_arg.is_dir() else default_dir
    elif isinstance(test_paths, list):
        test_dir = Path(test_paths[0])
    else:
        assert isinstance(test_paths, str)
        test_dir = Path(test_paths)

    test_dir = (pytestconfig.rootpath / test_dir).absolute()
    assert test_dir.is_dir()
    return test_dir


@pytest.fixture(name="resources_dir", scope="session")
def fixture_resources_dir(root_dir: Path) -> Path:
    res_dir: Path = root_dir / "resources"
    assert res_dir.is_dir()
    return res_dir


@pytest.fixture(name="version_pattern", scope="session")
def fixture_version_pattern() -> Pattern:
    return re.compile(
        r"^\s*" + VERSION_PATTERN + r"\s*$",
        flags=re.VERBOSE | re.IGNORECASE,
    )


@pytest.fixture(name="clean_dir")
def _fixture_clean_dir(tmp_path: Path) -> Generator[None, None, None]:
    cwd_og: Path = Path.cwd()
    if tmp_path.exists():
        shutil.rmtree(tmp_path)
    tmp_path.mkdir()
    os.chdir(tmp_path)

    yield
    os.chdir(cwd_og)
    shutil.rmtree(tmp_path)


@pytest.fixture(name="env", autouse=True)
def _fixture_env() -> Generator[None, None, None]:
    env_og: Dict[str, str] = os.environ.copy()

    yield

    os.environ.clear()
    os.environ.update(env_og)
