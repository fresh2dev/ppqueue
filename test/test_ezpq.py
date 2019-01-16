
import unittest
from multiprocessing import Process
from threading import Thread

from _context import ezpq

def return_i(i):
    return i

class TestEZPQ(unittest.TestCase):

    def setUp(self):
        self.Q = ezpq.Queue(job_runner=Process, auto_start=True)

    def test_priority(self):
        self.Q._stop()
        
        in_list = tuple(reversed(range(100)))
        
        for i in in_list:
            self.Q.put(function=return_i, args=i, priority=-i)
        
        self.Q.start()
        self.Q.wait()

        out_list = tuple(job['output'] for job in self.Q.collect())

        self.assertEqual(in_list, out_list)

    def tearDown(self):
        self.Q.dispose()
        del(self.Q)

if __name__ == '__main__':
    unittest.main()
