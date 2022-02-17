import threading
import unittest
from typing import Tuple
from unittest.case import TestCase

import ezpq
from ezpq.utils import get_logger
from tests.EzpqTestCases import get_sample_data

LOG = get_logger(__name__)


def return_me(x):
    return x


class TestPlot(unittest.TestCase):
    def setUp(self):
        LOG.info(self.id())
        self.input: Tuple[int] = get_sample_data()

    def test_plot(self):
        with ezpq.Queue(engine=threading.Thread, max_concurrent=3) as queue:
            job_data = queue.map(return_me, self.input)

        TestCase().assertEqual(len(self.input), len(job_data))

        plot = ezpq.Plot(job_data).build()

        TestCase().assertIsNotNone(plot)
