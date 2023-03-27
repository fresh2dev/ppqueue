import unittest
from threading import Thread
from typing import Tuple

from ppqueue import Queue
from ppqueue.utils import get_logger

from .common import PPQueueTestCases, get_sample_data

LOG = get_logger(__name__)


class TestThreading(unittest.TestCase, PPQueueTestCases):
    def setUp(self):
        LOG.info(self.id())
        self.input: Tuple[int, ...] = get_sample_data()
        self.queue = Queue(engine=Thread, max_concurrent=3)

    def tearDown(self):
        self.queue.dispose()
        self.assertFalse(self.queue.is_running)
        del self.queue


if __name__ == "__main__":
    unittest.main()
