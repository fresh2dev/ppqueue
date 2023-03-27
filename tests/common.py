import random
from typing import List, Tuple
from unittest import TestCase

import ppqueue
from ppqueue.utils import get_logger

LOG = get_logger(__name__)


# pylint: disable=protected-access


def return_me(x):
    return x


def reciprocal(x):
    return 1 / x


def get_sample_data(n: int = 31) -> Tuple[int, ...]:
    sample: List[int] = list(range(n))
    random.shuffle(sample)
    return tuple(sample)


class PPQueueTestCases(object):
    def setUp(self):
        self.queue: ppqueue.Queue
        self.input: Tuple[int, ...]
        raise NotImplementedError()

    def test_priority(self):
        TestCase().assertTrue(self.queue.is_running)

        self.queue.stop()

        TestCase().assertFalse(self.queue.is_running)

        for i, x in enumerate(self.input):
            self.queue.enqueue(
                return_me,
                args=x,
                priority=-i,
            )  # should result in reversed inputs.

        self.queue.start()

        TestCase().assertTrue(self.queue.is_running)

        self.queue.wait()

        output = tuple(job.result for job in self.queue.collect())

        TestCase().assertTupleEqual(tuple(reversed(self.input)), output)

        TestCase().assertEqual(len(self.input), self.queue._count_input)
        TestCase().assertEqual(self.queue._count_input, self.queue._count_output)

    def test_map(self):
        job_data = self.queue.map(return_me, self.input)

        output = tuple(job.result for job in job_data)

        TestCase().assertTupleEqual(self.input, output)

        TestCase().assertEqual(len(self.input), self.queue._count_input)
        TestCase().assertEqual(self.queue._count_input, self.queue._count_output)

    def test_starmap(self):
        job_data = self.queue.starmap(return_me, [[x] for x in self.input])

        output = tuple(job.result for job in job_data)

        TestCase().assertTupleEqual(self.input, output)

        TestCase().assertEqual(len(self.input), self.queue._count_input)
        TestCase().assertEqual(self.queue._count_input, self.queue._count_output)

    def test_starmapkw(self):
        job_data = self.queue.starmapkw(
            return_me,
            [{"x": x} for x in self.input],
        )

        output = tuple(job.result for job in job_data)

        TestCase().assertTupleEqual(self.input, output)

        TestCase().assertEqual(len(self.input), self.queue._count_input)
        TestCase().assertEqual(self.queue._count_input, self.queue._count_output)

    def test_groups(self):
        for i, x in enumerate(self.input):
            self.queue.enqueue(
                return_me,
                args=x,
                group=i % self.queue.max_concurrent,
            )  # returns in order

        self.queue.wait()
        output = tuple(x.result for x in self.queue.collect())

        TestCase().assertTupleEqual(self.input, output)

        TestCase().assertEqual(len(self.input), self.queue._count_input)
        TestCase().assertEqual(self.queue._count_input, self.queue._count_output)

    def test_group_error(self):
        this_input = list(self.input)
        this_input[int(len(this_input) / 2)] = 0
        for i, x in enumerate(this_input):
            self.queue.enqueue(
                reciprocal,
                x,
                group=i % 3,
                suppress_errors=True,
                skip_on_group_error=True,
            )
        self.queue.wait()
        output = self.queue.collect()

        TestCase().assertGreater(
            len(self.input),
            len([x for x in output if x.start_timestamp is not None]),
        )

    def test_size(self):
        for x in self.input:
            self.queue.enqueue(return_me, args=x)

        sizes = [
            self.queue.size()
            for _ in range(len(self.input))
            for _ in [self.queue.dequeue(wait=True)]
        ]

        # numbers in `sizes` should decrement by 1 until reaching 0.
        TestCase().assertListEqual(sizes, list(reversed(range(len(self.input)))))

        TestCase().assertEqual(len(self.input), self.queue._count_input)
        TestCase().assertEqual(self.queue._count_input, self.queue._count_output)

    def test_terminate(self):
        for x in self.input:
            self.queue.enqueue(return_me, args=x, priority=x)
        self.queue._stop_all()
        output = self.queue.collect()

        TestCase().assertListEqual(
            list(sorted([x.idx for x in output])),
            list(range(1, len(self.input) + 1)),
        )

    def test_empty(self):
        TestCase().assertEqual(self.queue.size(), 0)
        TestCase().assertTrue(self.queue.is_empty())
