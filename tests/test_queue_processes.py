import unittest
from multiprocessing import Process
from typing import Tuple

from ppqueue import Queue
from ppqueue.utils import get_logger, is_windows_os

from .common import PPQueueTestCases, get_sample_data

LOG = get_logger(__name__)


@unittest.skipIf(
    is_windows_os(),
    "multiprocessing tests are reliable on a windows os; skipping.",
)
class TestProcessing(unittest.TestCase, PPQueueTestCases):
    def setUp(self):
        LOG.info(self.id())
        self.input: Tuple[int, ...] = get_sample_data()
        self.queue = Queue(engine=Process, max_concurrent=3)

    def tearDown(self):
        self.queue.dispose()
        self.assertFalse(self.queue.is_running)
        del self.queue


if __name__ == "__main__":
    unittest.main()
