import random
from typing import List, Tuple
from unittest import TestCase

import ezpq
from ezpq.utils import get_logger

LOG = get_logger(__name__)


def return_me(x):
    return x


def reciprocal(x):
    return 1 / x


def get_sample_data(n: int = 31) -> Tuple[int, ...]:
    sample: List[int] = list(range(n))
    random.shuffle(sample)
    return tuple(sample)


class EzpqTestCases(object):
    def setUp(self):
        self.queue: ezpq.Queue
        self.input: Tuple[int]
        raise NotImplementedError()

    def test_priority(self):
        TestCase().assertTrue(self.queue.is_running)

        self.queue.stop()

        TestCase().assertFalse(self.queue.is_running)

        for i, x in enumerate(self.input):
            self.queue.put(
                fun=return_me, args=x, priority=-i
            )  # should result in reversed inputs.

        self.queue.start()

        TestCase().assertTrue(self.queue.is_running)

        self.queue.wait()

        output = tuple(job.output for job in self.queue.collect())

        TestCase().assertTupleEqual(tuple(reversed(self.input)), output)

        TestCase().assertEqual(len(self.input), self.queue.count_input())
        TestCase().assertEqual(self.queue.count_input(), self.queue.count_output())

    def test_map(self):
        job_data = self.queue.map(fun=return_me, iterable=self.input)

        output = tuple(job.output for job in job_data)

        TestCase().assertTupleEqual(self.input, output)

        TestCase().assertEqual(len(self.input), self.queue.count_input())
        TestCase().assertEqual(self.queue.count_input(), self.queue.count_output())

    def test_starmap(self):
        job_data = self.queue.starmap(fun=return_me, iterable=[[x] for x in self.input])

        output = tuple(job.output for job in job_data)

        TestCase().assertTupleEqual(self.input, output)

        TestCase().assertEqual(len(self.input), self.queue.count_input())
        TestCase().assertEqual(self.queue.count_input(), self.queue.count_output())

    def test_starmapkw(self):
        job_data = self.queue.starmapkw(
            fun=return_me, iterable=[{"x": x} for x in self.input]
        )

        output = tuple(job.output for job in job_data)

        TestCase().assertTupleEqual(self.input, output)

        TestCase().assertEqual(len(self.input), self.queue.count_input())
        TestCase().assertEqual(self.queue.count_input(), self.queue.count_output())

    def test_lanes(self):
        for i, x in enumerate(self.input):
            self.queue.put(
                fun=return_me, args=x, lane=i % self.queue.max_concurrent
            )  # returns in order

        self.queue.wait()
        output = tuple(x.output for x in self.queue.collect())

        TestCase().assertTupleEqual(self.input, output)

        TestCase().assertEqual(len(self.input), self.queue.count_input())
        TestCase().assertEqual(self.queue.count_input(), self.queue.count_output())

    def test_lane_error(self):
        this_input = list(self.input)
        this_input[int(len(this_input) / 2)] = 0
        for i, x in enumerate(this_input):
            self.queue.put(
                reciprocal,
                x,
                lane=i % 3,
                suppress_errors=True,
                skip_on_lane_error=True,
            )
        self.queue.wait()
        output = self.queue.collect()

        TestCase().assertGreater(
            len(self.input), len([x for x in output if x.started is not None])
        )

    def test_size(self):
        for x in self.input:
            self.queue.put(fun=return_me, args=x)

        sizes = [
            self.queue.size()
            for _ in range(len(self.input))
            for _ in [self.queue.get(wait=True)]
        ]

        # numbers in `sizes` should decrement by 1 until reaching 0.
        TestCase().assertListEqual(sizes, list(reversed(range(len(self.input)))))

        TestCase().assertEqual(len(self.input), self.queue.count_input())
        TestCase().assertEqual(self.queue.count_input(), self.queue.count_output())

    def test_terminate(self):
        for x in self.input:
            self.queue.put(fun=return_me, args=x, priority=x)
        self.queue.stop_all()
        output = self.queue.collect()

        TestCase().assertListEqual(
            list(sorted([x.idx for x in output])), list(range(1, len(self.input) + 1))
        )

    def test_empty(self):
        TestCase().assertEqual(self.queue.size(), 0)
        TestCase().assertTrue(self.queue.empty())
