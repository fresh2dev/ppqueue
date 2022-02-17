import logging as log
import time
from datetime import datetime
from typing import Callable, Sequence, Mapping, Optional, Any

from ezpq import utils


class Job:
    """used internally in `ezpq.Queue`.
    defines what to run, with what priority, and other options.
    """

    def __init__(
            self,
            fun: Callable,
            args: Sequence[Any] = None,
            kwargs: Mapping[str, Any] = None,
            name: Optional[str] = None,
            priority: Optional[int] = 100,
            lane: Optional[int] = None,
            timeout: Optional[int] = 0,
            suppress_errors: Optional[bool] = False,
            skip_on_lane_error: Optional[bool] = False,
    ):
        """
        Required Arguments:
            fun {Callable}

        Keyword Arguments:
            args {Sequence[Any]} -- (default: {None})
            kwargs {Mapping[str, Any]} -- (default: {None})
            name {Optional[str]} -- (default: {None})
            priority {Optional[int]} -- (default: {100})
            lane {Optional[int]} -- (default: {None})
            suppress_errors {Optional[bool]} -- (default: {False})
            skip_on_lane_error {Optional[bool]} -- (default: {False})
        """
        self._qid = None  # automatically assigned during processing.
        self._idx = None  # automatically assigned during processing.
        self.name = name
        self.lane = lane
        self.timeout = timeout
        self.fun = fun

        if args is None:
            self.args = None
        elif not hasattr(args, "__iter__"):
            self.args = [args]
        else:
            self.args = list(args)

        if kwargs is None:
            self.kwargs = None
        else:
            self.kwargs = dict(kwargs)

        self._priority = priority
        self._suppress_errors = suppress_errors
        self._skip_on_lane_error = skip_on_lane_error
        self._inner_job = None
        self._cancelled = False
        self._submit_time = None
        self._start_time = None
        self._finish_time = None
        self._process_time = None
        self._output = None
        self._exitcode = None
        self._exception_txt = None
        self._callback = None

    def compare(self, job) -> int:
        """compares two jobs by priority or index.

        Arguments:
            job {ezpq.Job}

        Returns:
            int -- `1` if `self` is greater than comparison,
                  `-1` if `self` is less than,
                  `0` if equal.
        """
        return utils.compare_by(self, job, by=["priority", "idx"])

    def __eq__(self, job) -> bool:
        return self.compare(job) == 0

    def __ne__(self, job) -> bool:
        return self.compare(job) != 0

    def __lt__(self, job) -> bool:
        return self.compare(job) < 0

    def __le__(self, job) -> bool:
        return self.compare(job) <= 0

    def __gt__(self, job) -> bool:
        return self.compare(job) > 0

    def __ge__(self, job) -> bool:
        return self.compare(job) >= 0

    def is_running(self) -> bool:
        """`True` if the inner job/thread is alive,
        else `False`."""
        return self._inner_job is not None and self._inner_job.is_alive()

    def is_expired(self) -> bool:
        """`True` if the job is running and its timeout is exceeded,
        else `False`."""
        return (
                self.is_running()
                and self.timeout > 0
                and self._start_time is not None
                and self._finish_time is None
                and self._start_time + self.timeout < time.time()
        )

    def join(self) -> None:
        """waits for the job/thread to complete.

        Returns:
            None
        """
        if self._inner_job is not None:
            self._inner_job.join()

    @property
    def exitcode(self):
        if (
                self._inner_job is not None
                and hasattr(self._inner_job, "exitcode")
                and self._inner_job.exitcode is not None
                and not self._suppress_errors
        ):
            return self._inner_job.exitcode
        else:
            return self._exitcode

    def terminate(self):
        if self._inner_job is not None:
            self._inner_job.terminate()

    def stop(self):
        """Terminates an existing process. Does not work for threads."""
        if self.is_running():
            if not hasattr(self._inner_job, "terminate"):
                log.error("Unable to terminate thread.")
            else:
                self._inner_job.terminate()
                self._inner_job.join()
                self._finish_time = time.time()
                self._cancelled = True
                log.debug("Stopped data: '%d'", self._idx)

    def running_time(self):
        """Returns the runtime of a finished data.
        Includes actual start time to finish time, not any overhead after.
        """
        if self._finish_time:
            return self._finish_time - self._start_time
        return None

    def waiting_time(self):
        """Returns the amount of time a data has spent in the waiting queue."""
        if self._start_time:
            return self._start_time - self._submit_time
        return time.time() - self._submit_time

    def total_time(self):
        """Returns the total time a finished data spent in the ezpq.Queue system.
        Includes actual submit time to finish time, not any overhead after.
        """
        if self._finish_time:
            return self._finish_time - self._submit_time
        return time.time() - self._submit_time

    def get_submit_time(self):
        """Returns a datetime object of the time this data was submitted."""
        if self._submit_time:
            return datetime.utcfromtimestamp(self._submit_time)
        return None

    def get_start_time(self):
        """Returns a datetime object of the time this data was started."""
        if self._start_time:
            return datetime.utcfromtimestamp(self._start_time)
        return None

    def get_end_time(self):
        """Returns a datetime object of the time this data finished."""
        if self._finish_time:
            return datetime.utcfromtimestamp(self._finish_time)
        return None

    def get_processed_time(self):
        """Returns a datetime object of the time this data was processed."""
        if self._process_time:
            return datetime.utcfromtimestamp(self._process_time)
        return None

    def is_processed(self):
        """Returns true if this data has been processed; false otherwise.
        A processed data is one that has had its output gathered, callback called,
          before being removed from the working dictionary.
        """
        return self._process_time is not None

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return self.__str__()

    @property
    def cancelled(self):
        return self._cancelled

    @cancelled.setter
    def cancelled(self, value):
        self._cancelled = value

    @property
    def idx(self):
        return self._idx

    @idx.setter
    def idx(self, value: int):
        self._idx = value

    @property
    def suppress_errors(self):
        return self._suppress_errors

    @suppress_errors.setter
    def suppress_errors(self, value: bool):
        self._suppress_errors = value

    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, value: int):
        self._priority = value

    @property
    def qid(self):
        return self._qid

    @property
    def output(self):
        return self._output

    @property
    def exception_txt(self):
        return self._exception_txt

    @property
    def callback(self):
        return self._callback
