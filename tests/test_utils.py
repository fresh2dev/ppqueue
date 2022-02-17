import logging
import unittest
from unittest.case import TestCase

from ezpq import Job
from ezpq.utils import compare, compare_by, get_logger

LOG = get_logger(__name__)


class TestUtils(unittest.TestCase):
    def setUp(self) -> None:
        LOG.info(self.id())

    def tearDown(self) -> None:
        pass

    def test_get_logger(self):
        name: str = "test"
        level: int = logging.DEBUG
        logger: logging.Logger = get_logger(name=name, level=level)
        TestCase().assertEqual(name, logger.name)
        TestCase().assertEqual(level, logger.level)

    def test_compare(self):
        TestCase().assertEqual(0, compare(None, None))
        TestCase().assertEqual(0, compare(1, 1))
        TestCase().assertEqual(-1, compare(1, 2))
        TestCase().assertEqual(1, compare(3, 2))

    def test_compare_by(self):
        obj1: Job = Job(fun=None)
        obj2: Job = Job(fun=None)

        obj1._priority = 1
        obj2._priority = 1
        obj1._idx = 1
        obj2._idx = 1
        TestCase().assertEqual(0, compare_by(obj1, obj2, by=["priority", "idx"]))

        obj2._idx = 2
        TestCase().assertEqual(-1, compare_by(obj1, obj2, by=["priority", "idx"]))

    # TODO: `test_log_csv`
