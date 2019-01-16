
import unittest
from multiprocessing import Process
from threading import Thread

from _context import ezpq

def return_i(i=1):
    return i

class TestEZPQ(unittest.TestCase):

    def setUp(self):
        self.Q = ezpq.Queue(job_runner=Process, auto_start=True)
        self.input = tuple(range(100))

    def test_priority(self):
        self.Q._stop()
        
        for i in self.input:
            self.Q.put(function=return_i, args=i,
                        priority=-i) # should result in reversed inputs.
        
        self.Q.start()
        self.Q.wait()

        out_list = tuple(job['output'] for job in self.Q.collect())

        self.assertEqual(tuple(reversed(self.input)), out_list)

    def test_map(self):               
        job_data = self.Q.map(function=return_i, iterable=self.input, ordered=True)

        out_list = tuple(job['output'] for job in job_data)

        self.assertEqual(self.input, out_list)

    def test_size(self):
        self.assertEqual(self.Q.size(), 0)

    def tearDown(self):
        self.Q.dispose()
        del(self.Q)

if __name__ == '__main__':
    unittest.main()
