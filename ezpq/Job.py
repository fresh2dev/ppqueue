import time
from datetime import datetime
import logging as log

class Job():

    def __init__(self, function, args=None, kwargs=None, name=None, priority=100, lane=None, timeout=0):
        """Defines what to run within a `ezpq.Queue`, and how to run it.

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
            ezpq.Job object
        """

        self._qid = None # automatically assigned during processing.
        self._id = None # automatically assigned during processing.
        self.name = name
        self.lane = lane
        self.timeout = timeout
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.priority = priority
        self._inner_job = None
        self._cancelled = False
        self._submitted = None
        self._started = None
        self._ended = None
        self._processed = None
        self._output = None
        self._callback = None

    def is_running(self):
        '''Returns true if the inner job is alive; false otherwise.'''
        return self._inner_job is not None and self._inner_job.is_alive()

    def is_expired(self):
        '''Returns true if the job is running and its timeout is exceeded; false otherwise.'''
        return self.is_running() and \
                self.timeout > 0 and \
                self._started is not None and \
                self._ended is None and \
                self._started + self.timeout < time.time()

    def _join(self):
        '''Waits for the inner job to complete.

        Returns:
            None
        '''
        if self._inner_job is not None:
            self._inner_job.join()

    def get_exitcode(self):
        '''Returns the exit code of the inner job. Only works for processes, not threads.'''
        if self._inner_job is not None and hasattr(self._inner_job, 'exitcode'):
            return self._inner_job.exitcode
        return None

    def _terminate(self):
        if self._inner_job is not None:
            self._inner_job.terminate()

    def _stop(self):
        '''Terminates an existing process. Does not work for threads.'''
        if self.is_running():
            if not hasattr(self._inner_job, 'terminate'):
                log.error('Unable to terminate thread.')
            else:
                self._inner_job.terminate()
                self._inner_job.join()
                self._ended = time.time()
                self._cancelled = True
                log.debug("Stopped job: '{}'".format(self._id))

    def run_time(self):
        '''Returns the runtime of a completed job.
        Includes actual start time to finish time, not any overhead after.
        '''
        if self._ended:
            return self._ended - self._started
        return None

    def waiting_time(self):
        '''Returns the amount of time a job has spent in the waiting queue.'''
        if self._started:
            return self._started - self._submitted
        else:
            return time.time() - self._submitted
        return time.time() - self._started

    def total_time(self):
        '''Returns the total time a completed job spent in the ezpq.Queue system.
        Includes actual submit time to finish time, not any overhead after.
        '''
        if self._ended:
            return self._ended - self._submitted
        return time.time() - self._submitted

    def get_submit_time(self):
        '''Returns a datetime object of the time this job was submitted.'''
        if self._submitted:
            return datetime.fromtimestamp(self._submitted)
        return None

    def get_start_time(self):
        '''Returns a datetime object of the time this job was started.'''
        if self._started:
            return datetime.fromtimestamp(self._started)
        return None

    def get_end_time(self):
        '''Returns a datetime object of the time this job finished.'''
        if self._ended:
            return datetime.fromtimestamp(self._ended)
        return None

    def get_processed_time(self):
        '''Returns a datetime object of the time this job was processed.'''
        if self._processed:
            return datetime.fromtimestamp(self._processed)
        return None

    def is_processed(self):
        '''Returns true if this job has been processed; false otherwise.
        A processed job is one that has had its output gathered, callback called,
          before being removed from the working dictionary.
        '''
        return self._processed is not None

    def __str__(self):
        return str(self.to_dict())

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        '''Returns a dictionary of the ezpq.Job object.'''
        return {
            'qid': self._qid,
            'id': self._id,
            'name': self.name,
            'priority': self.priority,
            'lane': self.lane,
            'timeout':self.timeout,
            'function': self.function.__name__,
            'args': self.args,
            'kwargs': self.kwargs,
            'submitted': self.get_submit_time(),
            'started': self.get_start_time(),
            'ended': self.get_end_time(),
            'processed': self.get_processed_time(),
            'exitcode': self.get_exitcode(),
            'cancelled': self._cancelled,
            'runtime': self.run_time(),
            'output': self._output,
            'callback': self._callback
        }
