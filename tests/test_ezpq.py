import unittest
from multiprocessing import Process
from threading import Thread
from typing import Tuple

import ezpq
from ezpq.utils import get_logger, is_windows_os
from tests.EzpqTestCases import EzpqTestCases, get_sample_data

LOG = get_logger(__name__)


class TestThreading(unittest.TestCase, EzpqTestCases):
    def setUp(self):
        LOG.info(self.id())
        self.input: Tuple[int] = get_sample_data()
        self.queue = ezpq.Queue(engine=Thread, max_concurrent=3)

    def tearDown(self):
        self.queue.dispose()
        self.assertFalse(self.queue.is_running)
        del self.queue


@unittest.skipIf(
    is_windows_os(),
    "multiprocessing tests are reliable on a windows os; skipping.",
)
class TestProcessing(unittest.TestCase, EzpqTestCases):
    def setUp(self):
        LOG.info(self.id())
        self.input: Tuple[int] = get_sample_data()
        self.queue = ezpq.Queue(engine=Process, max_concurrent=3)

    def tearDown(self):
        self.queue.dispose()
        self.assertFalse(self.queue.is_running)
        del self.queue


if __name__ == "__main__":
    unittest.main()
