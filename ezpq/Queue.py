import ezpq.Job
import multiprocessing as mp
import threading as thread
from heapq import heappush, heappop
import os
import csv
import time
import logging as log

class Queue():

    class RepeatedTimer():
        """Runs a function using threads on a periodic schedule.
        Used internally to periodically process jobs in an EZPQ Queue.
        Borrowed from: https://stackoverflow.com/a/13151299
        """

        def __init__(self, interval, function, *args, **kwargs):
            self._timer     = None
            self._interval   = interval
            self.function   = function
            self.args       = args
            self.kwargs     = kwargs
            self.is_running = False
            # self.start()

        def _run(self):
            self.is_running = False
            self.start()
            self.function(*self.args, **self.kwargs)

        def start(self):
            if not self.is_running:
                self._timer = thread.Timer(self._interval, self._run)
                self._timer.start()
                self.is_running = True

        def stop(self):
            if self.is_running:
                self._timer.cancel()
                self.is_running = False


    def __init__(self,
                 job_runner = mp.Process,
                 n_workers = mp.cpu_count(),
                 max_size = 0,
                 auto_remove = False,
                 auto_start = True,
                 auto_stop = False,
                 callback = None,
                 log_file = None,
                 poll = 0.1):
        """Implements a parallel queueing system.

        Args:
            job_runner: the class to use to invoke new jobs.
                - Accepts: multiprocessing.Process, threading.Thread
                - Default: multiprocessing.Process
            n_workers: the max number of concurrent jobs.
                - Accepts: int
                - Default: cpu_count()
            max_size: when > 0, will throw an exception the number of enqueued jobs exceeds this value. Otherwise, no limit.
                - Accepts: int
                - Default: 0 (unlimited)
            auto_remove: controls whether jobs are discarded of after completion.
                - Accepts: bool
                - Default: False
            auto_start: controls whether the queue system "pulse" is started upon instantiation (default), or manually.
                - Accepts: bool
                - Default: True
            auto_stop: controls whether the queue system "pulse" stops itself after all jobs are complete.
                - Accepts: bool
                - Default: False
            callback: optional function to execute synchronously immediately after a job completes.
                - Accepts: function object
                - Default: None
            log_file: if file path is specified, job data is written to this path in CSV format.
                - Accepts: str
                - Default: None
            poll: controls the pulse frequency; the amount of time slept between operations.
                - Accepts: float
                - Default: 0.1

        Returns:
            ezpq.Queue object.
        """

        assert(job_runner in (mp.Process, thread.Thread))
        assert(poll >= 0.01) # max 100 pulses per sec.

        self._max_size = max_size
        self._n_submitted = 0
        self._n_completed = 0
        self._n_workers = n_workers
        self._callback = callback
        self._log_file = log_file

        self._lock = thread.Lock() # https://opensource.com/article/17/4/grok-gil
        self._job_runner = job_runner

        self._working = dict()
        self._waiting = list()
        self._completed = list()
        self._auto_remove = auto_remove

        if self._job_runner is mp.Process:
            self._output = mp.Manager().dict()
        else:
            self._output = dict()

        log.debug('Initialized queue with {} workers.'.format(self._n_workers))

        self._poll = poll
        self._ticker = Queue.RepeatedTimer(interval=self._poll, function=self._pulse)
        if auto_start is True:
            self.start()
        self._auto_stop = auto_stop

        log.debug('Initialized pulse.')

    def __call__(self, fun, *args, **kwargs):
        '''Decorator guided by http://scottlobdell.me/2015/04/decorators-arguments-python/'''
        if not self.is_started():
            self.start()

        def wrapped_f(iterable, *args, **kwargs):
            for x in iterable:
                self.put(function=fun, args=[x]+list(args), kwargs=kwargs)
            self.wait()
            job_data = self.collect()
            self.dispose()
            return job_data
        return wrapped_f

    def _job_wrap(self, _job, *args, **kwargs):
        '''Used internally to wrap a job, capture output and any exception.'''
        out = None
        err = False

        try:
            out = _job.function(*args, **kwargs)
        except Exception as ex:
            err = True
            out = str(ex)

        self._output.update({ _job._id: {'_ended':time.time(), '_output':out} })

        if err:
            raise Exception(out)

    @staticmethod
    def log_csv(job, path='ezpq_log.csv', append=True):
        try:
            csv_exists = os.path.exists(path)

            mode = 'x' # create

            if csv_exists:
                if append:
                    mode = 'a' # append
                else:
                    mode = 'w' # write

            with open(path, mode) as csvfile:
                writer = None

                if isinstance(job, dict):
                    writer = csv.DictWriter(csvfile, fieldnames=list(job.keys()))

                    if not csv_exists:
                        writer.writeheader()

                    writer.writerow(job)

            return 0

        except IOError as ex:
            log.error("Logging error: {0}".format(str(ex)))

        return -1

    def __del__(self):
        self.dispose()

    def start(self):
        '''Starts the queue system pulse.'''
        self._ticker.start()

    def _stop(self):
        '''Stops the queue system pulse.'''
        if self._ticker is not None:
            self._ticker.stop()
            log.debug('Stopped pulse.')

    def dispose(self):
        '''Clears all output and stops the queue system pulse.'''
        with self._lock:
            self.clear()
            self._stop()

    def stop_all(self, wait=False):
        '''Stops all jobs in the working dictionary.'''
        keys = list(self._working.keys())
        for k in keys:
            job = self._working.get(k)
            if job is not None: job._stop()

        if wait: self.wait(n=0)

    def clear(self):
        '''Clears the queue system components: waiting, working, completed.
        Also resets the counters.
        '''
        self.stop_all(wait=True)

        self._working = dict()
        log.debug('Removed jobs.')
        self._output = dict()
        log.debug('Removed output.')
        self._completed = list()
        log.debug('Removed completed.')
        self._n_submitted = 0
        self._n_completed = 0
        log.debug('Reset counters.')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.dispose()

    def n_submitted(self):
        '''Returns the total number of jobs that have entered the queueing system.'''
        return self._n_submitted

    def n_completed(self):
        '''Returns the total number of jobs that are in the completed queue.'''
        return self._n_completed

    def n_workers(self):
        '''Returns max concurrency limit.'''
        return self._n_workers

    def _pulse(self):
        '''Used internally; manages the queue system operations.'''

        start = time.time()

        if self._lock.locked():
            log.warning('Pulse already running.') #' If you see this repeatedly, consider increasing the value of "poll", or removing a slow callback.')
        elif self._auto_stop and not self.has_work():
            self._stop()
        else:
            with self._lock:
                for job_id, job in self._working.items():
                    if job.is_expired():
                        job._stop()

                for job_id in list(self._working.keys()):
                    job = self._working[job_id]

                    if not job.is_running() and not job.is_processed():
                        job._join()
                        if not job._cancelled:
                            try:
                                job_data = self._output.pop(job._id)
                                job._ended = job_data['_ended']
                                job._output = job_data['_output']
                            except KeyError as ex:
                                job._ended = time.time()
                                job._output = 'No output for job {}; it may have exited unexpectedly.'.format(str(ex))

                        if self._callback is not None:
                            try:
                                job._callback = self._callback(job.to_dict())
                            except Exception as ex:
                                job._callback = str(ex)

                        job._processed = time.time()

                        log.debug("Completed job: '{}'".format(job._id))
                        if not self._auto_remove:
                            Queue._push_job(self._completed, job) # self._working.pop(job._id))

                        if self._log_file is not None:
                            Queue.log_csv(job=job.to_dict(), path=self._log_file)

                        del(self._working[job_id])

                        self._n_completed += 1

                n_to_start = self.n_workers_free()

                while n_to_start > 0 and self.has_waiting():
                    _, _, job = heappop(self._waiting)
                    self._start_job(job=job)
                    n_to_start -= 1

            # log.debug("Pulse completed in: {}s; n_submitted={}; n_completed={}".format(round(time.time() - start, 4), self._n_submitted, self._n_completed))

    @staticmethod
    def _push_job(queue, job):
        '''Internal; pushes a job into the provided list, prioritized by (1) priority and (2) arrival time.'''
        heappush(queue, (int(job.priority), job._submitted, job))

    def size(self, waiting=False, working=False, completed=False):
        """Returns the number of jobs in the corresponding queue(s).

        Args:
            waiting: include jobs in the waiting queue?
                - Accepts: bool
                - Default: False
            working: include jobs in the working table?
                - Accepts: bool
                - Default: False
            completed: include jobs in the completed queue?
                - Accepts: bool
                - Default: False

        Note: when all are False, all jobs are counted (default).

        Returns:
            int
        """
        size = 0
        if not any([waiting, working, completed]):
            waiting, working, completed = (True, True, True)

        if waiting: size += len(self._waiting)
        if working: size += len(self._working)
        if completed: size += len(self._completed)

        return size


    def has_waiting(self):
        '''True if there are jobs in the waiting queue; otherwise False.'''
        return self.size(waiting=True) > 0

    def has_completed(self):
        '''True if there are jobs in the completed queue; otherwise False.'''
        return self.size(completed=True) > 0

    def has_work(self):
        '''True if there are jobs either waiting or being worked; otherwise False.'''
        return self.size(waiting=True, working=True) > 0

    def n_workers_free(self):
        '''Returns the number of available processes.'''
        return self._n_workers - self.size(working=True)

    def is_working(self):
        '''True if there are running jobs.'''
        return self.size(working=True) > 0

    def is_busy(self):
        '''True if max concurrent limit (n_workers) is reached or if there are waiting jobs.'''
        return self.n_workers_free() <= 0 or self.has_waiting()

    def is_started(self):
        '''True if the queue system pulse is running.'''
        return self._ticker.is_running

    def is_empty(self):
        '''True if there are no jobs in the queue system.'''
        return self.size() <= 0

    def is_full(self):
        '''True if the number of jobs in the queue system is equal to max_size.'''
        return self._max_size > 0 and self.size() == self._max_size

    def remaining_jobs(self):
        '''The difference between the number of jobs submitted and the number completed.'''
        return self._n_submitted - self._n_completed

    def wait_worker(self, poll=0.1, timeout=0):
        """Waits for the number of running jobs to fall below the max concurrent limit (n_workers)

        Args:
            poll: the time, in seconds, between checks.
                - Accepts: float
                - Default: 0.1
            timeout: when > 0, the maximum time to wait, in seconds. Otherwise, no limit.
                - Accepts: float
                - Default: 0 (unlimited)

        Returns:
            True if a worker is available; False otherwise.
        """
        start = time.time()
        while self.is_busy() and (timeout==0 or time.time() - start < timeout):
            time.sleep(poll)

        return not self.is_busy()

    def wait(self, n=0, poll=0.1, timeout=0):
        """Waits for jobs to be completed by the queue system.

        Args:
            n: the number of jobs to wait for.
                - Accepts: int
                - Default: 0 (all jobs)
            poll: the time, in seconds, between checks.
                - Accepts: float
                - Default: 0.1
            timeout: when > 0, the maximum time to wait, in seconds. Otherwise, no limit.
                - Accepts: float
                - Default: 0 (unlimited)

        Returns:
            0 if the expected number of jobs completed. > 0 otherwise.
        """

        n_pending = self.size(waiting=True, working=True)

        if n > 0:
            n = min(n, n_pending)
        else:
            n = n_pending

        n_remaining = (n_pending+n) - n_pending

        start = time.time()
        while n_remaining > 0 and (timeout==0 or time.time() - start < timeout):
            time.sleep(poll)
            # n_remaining = self.size(waiting=True, working=True) - (n_pending - n)
            n_remaining = (self.size(waiting=True, working=True)+n) - n_pending

        return n_remaining

    def _start_job(self, job):
        '''Internal; invokes jobs.'''
        job_args = dict()

        if job.args is not None:
            if not isinstance(job.args, list):
                job_args['args'] = [job, job.args] #[job, self._output] + [job.args]
            else:
                job_args['args'] = [job] + job.args #[job, self._output] + list(job.args)

        if job.kwargs is None:
            job_args['kwargs'] = dict()
        else:
            job_args['kwargs'] = dict(job.kwargs)

        if job.args is None:
            job_args['kwargs'].update({'_job': job}) # , '_output': self._output})

        j = self._job_runner(name=str(job._id),
                             target=self._job_wrap,
                             **job_args)
        j.start()

        job._started = time.time()
        job._inner_job = j
        self._working.update({job._id: job})

        log.debug("Started job '{}'".format(job._id))

    def submit(self, job):
        '''Submits a job into the ezpq.Queue system.

        Throws an exception if:
            1. the Queue uses a Thread job_runner and this job has a timeout (can't terminate Threads),
            2. the Queue max_size will be exceeded after adding this job.

        Returns:
            The number of jobs submitted to the queue.
        '''

        assert(not (self._job_runner is thread.Thread and job.timeout > 0))

        if self._max_size > 0 and self.size()+1 > self._max_size:
            raise Exception('Max size exceeded.')

        job._submitted = time.time()

        self._n_submitted += 1
        job._id = self._n_submitted

        if job.name is None: job.name = job._id

        if self.is_started() and not self.is_busy():
            self._start_job(job = job)
        else:
            Queue._push_job(self._waiting, job)
            log.debug("Queued job: '{}'".format(job._id))

        return job._id

    def put(self, function, args=None, kwargs=None, name=None, priority=100, timeout=0):
        """Creates a ezpq.Job object with the given parameters, then submits it to the ezpq.Queue system.

        Args:
            function: the function to run.
                - Accepts: function object
            args: optional positional arguments to pass to the function.
                - Accepts: list, tuple
                - Default: None
            kwargs: optional keyword arguments to pass to the function.
                - Accepts: dict
                - Default: None
            name: optional name to give to the job. Does not have to be unique.
                - Accepts: str
                - Default: None; assumes same name as job id.
            priority: priority value to assign. Lower values get processed sooner.
                - Accepts: int
                - Default: 100
            timeout: When > 0, if this value (in seconds) is exceeded, the job is terminated. Otherwise, no limit enforced.
                - Accepts: float
                - Default: 0 (unlimited)

        Returns:
            The number of jobs submitted to the queue.
        """

        job = ezpq.Job(function=function, args=args, kwargs=kwargs, name=name, priority=priority, timeout=timeout)

        return self.submit(job)

    def map(self, function, iterable, args=None, kwargs=None, ordered=True, timeout=0):
        
        assert hasattr(iterable, '__iter__')

        if args is None:
            args = [None]
        elif not hasattr(args, '__iter__'):
            args = [None] + [args]
        elif len(args) == 0:
            args = [None]
        else:
            args = [None] + list(args)
        
        if ordered and self.is_started():
            self._stop()

        for i,x in enumerate(iterable):
            args[0] = x
            job = ezpq.Job(function=function, args=list(args), kwargs=kwargs, priority=1, timeout=timeout)
            if ordered: job.priority = i
            self.submit(job)
    
        if ordered:
            self.start()

        self.wait()
        return self.collect()

    def get(self, poll=0, timeout=0):
        """Pops the highest priority item from the completed queue.

        Args:
            poll: when > 0, time between checks
                - Accepts: float
                - Default: 0 (no wait); 0.1 if timeout > 0.
            timeout: the maximum time, in seconds, to wait for a job to complete.
                - Accepts: float
                - Default: 0 (no wait/unlimited wait)

        Notes:
            - when both poll and timeout are 0, only one check is done;
            - when either is > 0, the method will block until output is available.

        Returns:
            Dictionary of the most recently completed, highest priority job.
        """

        assert(not self._auto_remove)

        job = None

        if timeout > 0 and poll <= 0:
            poll = 0.1

        start = time.time()

        while True:
            if self.has_completed():
                _,_,x = heappop(self._completed)
                job = x.to_dict()
            elif (timeout > 0 or poll > 0) and (timeout <= 0 or time.time() - start < timeout):
                time.sleep(poll)
                continue

            break

        return job

    def collect(self, n=0):
        """Repeatedly calls `get()` and returns a list of job data.

        Args:
            n: the number of jobs to pop from the completed queue.
                - Accepts: int
                - Default: 0 (all)

        Returns:
            a list of dictionary objects.
        """

        if n <= 0:
            n = self.size(completed=True)
        else:
            n = min(n, self.size(completed=True))

        return [self.get() for _ in range(n)]

    def set_workers(self, n):
        '''Allows adjusting max concurrency.'''
        diff = n - self._n_workers
        self._n_workers = n
        log.debug('Added {} workers to the queue.'.format(diff))
