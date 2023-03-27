from __future__ import annotations

import logging
import os
from typing import List, Optional, TypeVar, Union


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """get or create a logger.

    Args:
        name (str)
        level (Optional[int]): Defaults to logging.DEBUG.

    Returns:
        logging.Logger
    """
    logger = logging.getLogger(name)

    logger.setLevel(logging.INFO if level is None else level)
    logger.propagate = False

    log_handler = logging.StreamHandler()
    log_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"),
    )
    logger.addHandler(log_handler)

    return logger


def is_windows_os() -> bool:
    return os.name == "nt"


def compare(num1: Union[int, float], num2: Union[int, float]) -> int:
    """compare two numbers.

    Args:
        num1 (Union[int, float]): the reference number.
        num2 (Union[int, float]): the comparison number.

    Returns:
        int:
            * 0  if num1 == num2
            * 1  if num1 > num2
            * -1 if num1 < num2
    """
    if num1 == num2:
        return 0

    diff = (num1 if num1 else 0) - (num2 if num2 else 0)

    if diff == 0:
        return int(diff)

    if diff > 0:
        return max(1, int(diff))

    # if diff < 0:
    return min(-1, int(diff))


def compare_by(object1: object, object2: object, by: List[str], _state: int = 0) -> int:
    """compare two objects by a list of attributes.
    attributes are compared iteratively and the comparison will
    short-circuit when/if the objects are determined unequal.

    Args:
        _state:
        object1 (object): the reference object.
        object2 (object): the comparison object.
        by (List[str]): list of attributes to compare.

    Returns:
        int:
            * 0  if object1 == object2
            * 1  if object1 > object2
            * -1 if object1 < object2
    """
    if _state and _state >= len(by):
        # if we've compared all attributes,
        # these objects are considered equal.
        return 0

    diff: int = compare(getattr(object1, by[_state]), getattr(object2, by[_state]))

    if diff == 0:
        # if equal, compare next attribute.
        return compare_by(object1, object2, by=by, _state=_state + 1)

    return diff


T = TypeVar("T")


def dedupe_list(l: list[T]) -> list[T]:
    return list(dict.fromkeys(l).keys())
