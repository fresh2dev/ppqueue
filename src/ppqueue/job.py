from __future__ import annotations

import logging as log
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from multiprocessing import Process
from threading import Thread
from typing import Any, Callable, Sequence

from .utils import compare_by


class JobState(Enum):
    UNSUBMITTED = auto()
    QUEUED = auto()
    RUNNING = auto()
    CANCELLED = auto()
    FINISHED = auto()
    UNKNOWN = auto()


@dataclass
class Job(object):
    """Represents a job returned by a Queue."""

    fun: Callable[..., Any]
    args: Sequence[Any] | None = field(default_factory=list)
    kwargs: dict[str, Any] | None = field(default_factory=dict)
    name: str | None = None
    priority: int = 100
    group: int | None = None
    timeout: float = 0
    suppress_errors: bool = False
    skip_on_group_error: bool = False
    # automatically assigned during processing.
    qid: str | None = field(default=None, init=False)
    """The ID of the queue that ran this job."""
    idx: int | None = field(default=None, init=False)
    """The ID of this job."""
    cancelled: bool = field(default=False, init=False)
    """Did this job timeout or was it cancelled?"""
    submit_timestamp: float | None = field(default=None, init=False)
    """Unix timestamp of when this job was submitted."""
    start_timestamp: float | None = field(default=None, init=False)
    """Unix timestamp of when this job started."""
    finish_timestamp: float | None = field(default=None, init=False)
    """Unix timestamp of when this job finished."""
    process_timestamp: float | None = field(default=None, init=False)
    """Unix timestamp of when this job was processed from the working queue."""
    result: Any = field(default=None, init=False)
    """Result of this job, if it exited gracefully."""
    callback_result: str | None = field(default=None, init=False)
    """Result of the callback for this job."""
    error: str | None = field(default=None, init=False)
    """Exception text for this job, if an error occurred."""
    exitcode: int | None = field(default=None, init=False)
    inner_job: Process | Thread | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        if self.args is None:
            self.args = []
        elif isinstance(self.args, str) or not isinstance(self.args, Iterable):
            self.args = [self.args]

        if self.kwargs is None:
            self.kwargs = {}

    def _compare(self, job: object) -> int:
        """compares two jobs by priority or index.

        Arguments:
            job {ppqueue.Job}

        Returns:
            int -- `1` if `self` is greater than comparison,
                  `-1` if `self` is less than,
                  `0` if equal.
        """
        return compare_by(self, job, by=["priority", "idx"])

    def __eq__(self, job: object) -> bool:
        return self._compare(job) == 0

    def __ne__(self, job: object) -> bool:
        return self._compare(job) != 0

    def __lt__(self, job: object) -> bool:
        return self._compare(job) < 0

    def __le__(self, job: object) -> bool:
        return self._compare(job) <= 0

    def __gt__(self, job: object) -> bool:
        return self._compare(job) > 0

    def __ge__(self, job: object) -> bool:
        return self._compare(job) >= 0

    def is_running(self) -> bool:
        return self.inner_job is not None and self.inner_job.is_alive()

    def is_expired(self) -> bool:
        return (
            self.is_running()
            and self.timeout > 0
            and self.start_timestamp is not None
            and self.finish_timestamp is None
            and self.start_timestamp + self.timeout < time.time()
        )

    def join(self, *args, **kwargs) -> None:
        # waits for the job to complete.
        if self.inner_job is not None:
            self.inner_job.join(*args, **kwargs)

    def get_exit_code(self) -> int | None:
        """Exit code of the job."""
        if (
            not self.inner_job
            or not hasattr(self.inner_job, "exitcode")
            or self.inner_job.exitcode is None
        ):
            return None
        return self.inner_job.exitcode

    def terminate(self) -> None:
        if self.inner_job is not None:
            self.inner_job.terminate()

    def stop(self) -> None:
        # Terminates an existing process. Does not work for threads.
        if not self.inner_job or not self.is_running():
            return
        elif not hasattr(self.inner_job, "terminate"):
            log.error("Unable to terminate thread.")
        else:
            self.inner_job.terminate()
            self.inner_job.join()
            self.finish_timestamp = time.time()
            self.cancelled = True
            log.debug("Stopped data: '%d'", self.idx)

    def get_seconds_running(self) -> float | None:
        """The number of seconds a job was running for.

        Examples:
            >>> from ppqueue import Queue
            >>> from time import sleep
            ...
            >>> with Queue() as queue:
            ...     for i in range(5):
            ...         jobs = queue.map(sleep, [1, 2, 3])
            ...
            >>> [int(job.get_seconds_running()) for job in jobs]
            [1, 2, 3]
        """
        if not self.start_timestamp or not self.finish_timestamp:
            return None
        return self.finish_timestamp - self.start_timestamp

    def get_seconds_waiting(self) -> float | None:
        """The amount of time a data has spent in the waiting queue.

        Examples:
            >>> job.get_seconds_waiting()  # doctest: +SKIP
        """
        if not self.submit_timestamp:
            return None

        if not self.start_timestamp:
            return time.time() - self.submit_timestamp

        return self.start_timestamp - self.submit_timestamp

    def get_seconds_total(self) -> float | None:
        """Returns the waiting + running duration of this job.

        Examples:
            >>> job.get_seconds_total()  # doctest: +SKIP
        """
        if not self.submit_timestamp:
            return None

        if not self.finish_timestamp:
            return time.time() - self.submit_timestamp

        return self.finish_timestamp - self.submit_timestamp

    def get_submit_timestamp(self) -> datetime | None:
        """The time this job was submitted.

        Examples:
            >>> job.get_submit_timestamp()  # doctest: +SKIP
        """
        if self.submit_timestamp:
            return datetime.utcfromtimestamp(self.submit_timestamp)
        return None

    def get_start_timestamp(self) -> datetime | None:
        """Returns a datetime object of the time this data was started."""
        if self.start_timestamp:
            return datetime.utcfromtimestamp(self.start_timestamp)
        return None

    def get_finish_timestamp(self) -> datetime | None:
        """Returns a datetime object of the time this data finished."""
        if self.finish_timestamp:
            return datetime.utcfromtimestamp(self.finish_timestamp)
        return None

    def get_processed_timestamp(self) -> datetime | None:
        """Returns a datetime object of the time this data was processed."""
        if self.process_timestamp:
            return datetime.utcfromtimestamp(self.process_timestamp)
        return None

    def is_processed(self) -> bool:
        # Returns true if this data has been processed; false otherwise.
        # A processed data is one that has had its output gathered, callback called,
        #   before being removed from the working dictionary.

        return self.process_timestamp is not None

    def get_state(self) -> JobState:
        if not self.submit_timestamp:
            return JobState.UNSUBMITTED
        if self.is_running():
            return JobState.RUNNING
        if self.cancelled:
            return JobState.CANCELLED
        if self.finish_timestamp:
            return JobState.FINISHED
        return JobState.UNKNOWN
