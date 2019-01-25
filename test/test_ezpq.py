
import unittest
from multiprocessing import Process
from threading import Thread
import random

from _context import ezpq

def return_me(x=1):
    return x

class TestEZPQ(unittest.TestCase):

    def setUp(self):
        self.Q = ezpq.Queue(job_runner=Process, auto_start=True, n_workers=5)
        in_list = list(range(100))
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

    def test_lanes(self):
        for i, x in enumerate(self.input):
            self.Q.put(function=return_me, args=x,
                        lane = i % self.Q.n_workers()) # returns in order

        self.Q.wait()
        out_list = tuple(x['output'] for x in self.Q.collect())

        self.assertEqual(self.input, out_list)

    def test_size(self):
        self.assertEqual(self.Q.size(), 0)

    def tearDown(self):
        self.Q.dispose()
        del(self.Q)

if __name__ == '__main__':
    unittest.main()
