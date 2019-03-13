
import unittest
from multiprocessing import Process
from threading import Thread
import random

from _context import ezpq

def return_me(x):
    return x

def reciprocal(x):
    return 1/x

class TestEZPQ(unittest.TestCase):

    def setUp(self):
        self.Q = ezpq.Queue(job_runner=Thread, auto_start=True, n_workers=5)
        in_list = list(range(1000))
        random.shuffle(in_list)
        self.input = tuple(in_list)

    def test_priority(self):
        self.Q._stop()

        for i,x in enumerate(self.input):
            self.Q.put(function=return_me, args=x,
                        priority=-i) # should result in reversed inputs.

        self.Q.start()
        self.Q.wait()

        out_list = tuple(job['output'] for job in self.Q.collect())

        self.assertEqual(tuple(reversed(self.input)), out_list)

    def test_map(self):
        job_data = self.Q.map(function=return_me, iterable=self.input)

        out_list = tuple(job['output'] for job in job_data)

        self.assertEqual(self.input, out_list)

    def test_starmap(self):
        job_data = self.Q.starmap(function=return_me, iterable=[[x] for x in self.input])

        out_list = tuple(job['output'] for job in job_data)

        self.assertEqual(self.input, out_list)

    def test_starmapkw(self):
        job_data = self.Q.starmapkw(function=return_me, iterable=[{'x': x} for x in self.input])

        out_list = tuple(job['output'] for job in job_data)

        self.assertEqual(self.input, out_list)

    def test_lanes(self):
        for i, x in enumerate(self.input):
            self.Q.put(function=return_me, args=x,
                        lane = i % self.Q.n_workers()) # returns in order

        self.Q.wait()
        out_list = tuple(x['output'] for x in self.Q.collect())

        self.assertEqual(self.input, out_list)

    def test_lane_error(self):
        for i in range(100):
            self.Q.put(reciprocal, random.randint(0, 5), lane=i%5, suppress_errors=True, stop_on_lane_error=True)
        self.Q.wait()
        output = self.Q.collect()

        self.assertGreater(len(self.input), len([x for x in output if x['started'] is not None]))


    def test_size(self):
        for x in self.input:
            self.Q.put(function=return_me, args=x)

        sizes = [self.Q.size()
                for i in range(len(self.input))
                for _ in [self.Q.get(wait=True)]]

        # numbers in `sizes` should decrement by 1 until reaching 0.
        self.assertEqual(sizes, list(reversed(range(len(self.input)))))

    def test_empty(self):
        self.assertEqual(self.Q.size(), 0)

    def tearDown(self):
        self.Q.dispose()
        del(self.Q)

if __name__ == '__main__':
    unittest.main()
