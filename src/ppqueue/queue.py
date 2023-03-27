from __future__ import annotations

import logging
import multiprocessing as mp
import threading
import time
import traceback
from heapq import heappop, heappush
from typing import Any, Callable, Sequence, Type
from uuid import uuid4

from tqdm.auto import tqdm

from .job import Job
from .utils import get_logger, is_windows_os

LOG = get_logger(__name__, level=logging.INFO)


class PulseTimer:
    """Periodically runs a function in a background thread.

    ref: https://stackoverflow.com/a/13151299
    """

    def __init__(
        self,
        *args: Any,
        interval_ms: int,
        fun: Callable[..., Any],
        **kwargs: Any,
    ):
        self._timer: threading.Timer | None = None
        self._interval: int = interval_ms
        self._function: Callable[..., Any] = fun
        self._args: tuple[Any, ...] = args
        self._kwargs: dict[str, Any] = kwargs
        self._is_enabled: bool = False
        self._is_running: bool = False

    def _run(self) -> None:
        self._is_running = False
        self.start()
        self._function(*self._args, **self._kwargs)

    def start(self) -> None:
        if not self._is_running:
            self._timer = threading.Timer(self._interval / 1000, self._run)
            self._timer.start()
            self._is_running = True

        self._is_enabled = True

    def stop(self) -> None:
        if self._timer is not None:
            self._timer.cancel()

        self._is_enabled = False
        self._is_running = False

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def is_enabled(self) -> bool:
        return self._is_enabled


class Queue:
    DEFAULT_GROUP: int | None = None

    def __init__(
        self,
        max_concurrent: int = mp.cpu_count(),
        *,
        max_size: int = 0,
        engine: Type[threading.Thread] | Type[mp.Process] = threading.Thread,
        name: str | None = None,
        callback: Callable[[Job], Any] | None = None,
        show_progress: bool = False,
        drop_finished: bool = False,
        stop_when_idle: bool = False,
        pulse_freq_ms: int = 100,
        no_start: bool = False,
    ):
        """

        Args:
            max_concurrent: max number of concurrently running jobs.
            max_size: max size of the queue (default=0, unlimited).
            engine: the engine used to run jobs; threads (default) or processes.
            name: an identifier for this queue.
            callback: a callable that is called immediately after each job is finished.
            show_progress: global setting for showing progress bars.
            drop_finished: if True, the queue will not store finished jobs for retrieval.
            stop_when_idle: if True, the queue will stop the pulse when all jobs are finished.
            pulse_freq_ms: the interval at which jobs are transitioned between internal queues.
            no_start: if True, do not start the queue pulse on instantiation.

        Examples:
            >>> from ppqueue import Queue
            >>> from time import sleep
            ...
            >>> with Queue() as queue:
            ...     jobs = queue.map(sleep, [1, 2, 3, 4, 5])
            ...
            >>> len(jobs)
            5
        """

        self._qid: str = name if name else str(uuid4())[:8]

        self.show_progress: bool = show_progress
        self._max_size: int = max_size
        self._count_input: int = 0
        self._count_output: int = 0
        self._max_concurrent: int = max_concurrent
        self._callback: Callable[..., Any] | None = callback

        self._lock: threading.Lock = (
            threading.Lock()
        )  # https://opensource.com/article/17/4/grok-gil

        if engine is mp.Process and is_windows_os():
            LOG.warning(
                (
                    "multiprocessing performance is degraded on Windows systems. see: "
                    "https://docs.python.org/3/library/multiprocessing.html?highlight=process#the-spawn-and-forkserver-start-methods"
                ),
            )

        self._engine: Type[mp.Process] | Type[threading.Thread] = engine

        self._waiting_groups: dict[int | None, list[Job]] = {None: []}
        self._working_queue: dict[int | None, Job] = {}
        self._finished_queue: list[Job] = []

        self._drop_finished: bool = drop_finished
        self._mp_manager: mp.Manager | None = None

        self._output: dict[int, Job]

        if self._engine is mp.Process:
            self._mp_manager = mp.Manager()
            self._output = self._mp_manager.dict()
        else:
            self._output = {}

        LOG.debug("Initialized queue with %d max_concurrent.", self._max_concurrent)

        self._pulse_freq_ms: int = pulse_freq_ms
        self._timer: PulseTimer = PulseTimer(
            interval_ms=self._pulse_freq_ms,
            fun=self._pulse,
        )

        if not no_start:
            self.start()

        self._stop_when_idle: bool = stop_when_idle

        LOG.debug("Initialized pulse.")

    # decorator
    def __call__(self, fun: Callable[..., Any], *args: Any, **kwargs: Any):
        self.start()

        def wrapped_f(iterable: Sequence[Any], *args: Any, **kwargs: Any) -> list[Job]:
            for x in iterable:
                self.enqueue(fun, args=[x, *args], kwargs=kwargs)
            self.wait()
            job_data = self.collect()
            self.dispose()
            return job_data

        return wrapped_f

    def __del__(self) -> None:
        self.dispose()

    def start(self) -> None:
        """Start the queue pulse."""
        self._timer.start()

    def stop(self) -> None:
        """Stop the queue pulse."""
        self._timer.stop()

    def dispose(self) -> None:
        """Stop running jobs, then clear the queue, then stop the queue pulse."""
        LOG.debug("Disposing")

        if self.is_running:
            self._stop_all(wait=True)
            self.stop()

        LOG.debug("Removed jobs.")
        self._output = None
        LOG.debug("Removed output.")
        self._finished_queue.clear()  # [:] = []
        LOG.debug("Removed finished.")
        self._count_input = 0
        self._count_output = 0
        LOG.debug("Reset counters.")

        if self._mp_manager is not None:
            self._mp_manager.shutdown()

    def _clear_waiting(self) -> None:
        """Clear the queue of pending jobs."""

        LOG.debug("Clearing waiting queue")
        with self._lock:
            keys = list(self._waiting_groups.keys())
            for k in keys:
                group_jobs = self._waiting_groups.get(k)
                for _ in range(len(group_jobs)):
                    job = heappop(group_jobs)
                    job.cancelled = True
                    job.process_timestamp = time.time()
                    if not self._drop_finished:
                        heappush(self._finished_queue, job)
                    else:
                        self._count_output += 1
                del self._waiting_groups[k]

    def _stop_all(self, *, wait: bool = True) -> None:
        """Stop and remove all running and waiting jobs."""

        self._clear_waiting()

        keys = list(self._working_queue.keys())
        for k in keys:
            job = self._working_queue.get(k)
            if job is not None:
                job.stop()

        if wait:
            self.wait()

    def __enter__(self) -> Queue:
        return self

    def __exit__(self, *args: Any) -> None:
        self.dispose()

    def __iter__(self) -> Queue:
        return self

    def __next__(self) -> Job:
        job: Job | None = self.dequeue()
        if job is None:
            raise StopIteration
        return job

    def _pulse(self) -> None:
        # TODO: document this better.
        """Used internally; manages the queue system operations."""

        with self._lock:
            try:
                if self._stop_when_idle and self.is_idle():
                    self.stop()
                else:
                    # stop expired jobs.
                    for job_id, job in self._working_queue.items():
                        if job.is_expired():
                            job.stop()

                    for job_id in list(self._working_queue.keys()):
                        job = self._working_queue[job_id]

                        if not job.is_running() and not job.is_processed():
                            job.join()
                            if not job.cancelled:
                                try:
                                    (
                                        stdout,
                                        stderr,
                                        exitcode,
                                        timestamp,
                                    ) = self._output.pop(job.idx)
                                    job.result = stdout
                                    job.error = stderr
                                    job.exitcode = exitcode
                                    job.finish_timestamp = timestamp
                                except KeyError:
                                    job.exception_txt = str(
                                        Exception(
                                            "{}\n\nNo data for data; it may have exited"
                                            " unexpectedly.".format(job.idx),
                                        ),
                                    )

                            if self._callback is not None:
                                try:
                                    job.callback_result = self._callback(job)
                                except (
                                    Exception  # pylint: disable=broad-exception-caught
                                ) as ex:
                                    job.callback_result = str(ex)

                            job.process_timestamp = time.time()

                            LOG.debug("Completed job: %d", job.idx)

                            if not self._drop_finished:
                                heappush(self._finished_queue, job)
                            else:
                                self._count_output += 1

                            del self._working_queue[job_id]

                            if (
                                job.group is not None
                                and job.group != self.DEFAULT_GROUP
                            ):
                                group_jobs: list[Job] = self._waiting_groups.get(
                                    job.group,
                                )
                                if group_jobs is not None:
                                    next_job = None
                                    parent_exitcode = job.exitcode
                                    while len(group_jobs) > 0:
                                        next_job = heappop(group_jobs)

                                        if (
                                            parent_exitcode == 0
                                            or not next_job.skip_on_group_error
                                        ):
                                            break
                                        else:
                                            next_job.cancelled = True
                                            next_job.exitcode = parent_exitcode
                                            next_job.error = (
                                                "skip_on_group_error = True and"
                                                " preceding data ({}) exit code is {}".format(
                                                    job.idx,
                                                    parent_exitcode,
                                                )
                                            )
                                            next_job.process_timestamp = time.time()
                                            if not self._drop_finished:
                                                heappush(
                                                    self._finished_queue,
                                                    next_job,
                                                )
                                            else:
                                                self._count_output += 1
                                            next_job = None
                                            continue

                                    if len(group_jobs) == 0:
                                        del self._waiting_groups[job.group]

                                    if next_job is not None:
                                        self._working_queue[next_job.idx] = next_job
                                        self._start_job(next_job)

                    while (
                        len(self._waiting_groups[self.DEFAULT_GROUP]) > 0
                        and self.max_concurrent - self._count_working() > 0
                    ):
                        job = heappop(self._waiting_groups[self.DEFAULT_GROUP])
                        self._working_queue[job.idx] = job
                        self._start_job(job)
            except:  # pylint: disable=bare-except
                self.stop()
                LOG.error(traceback.format_exc())
            finally:
                LOG.debug(
                    "waiting=%d; working=%d; finished=%d.",
                    self._count_waiting(),
                    self._count_working(),
                    self._count_finished(),
                )

    def _count_waiting(self) -> int:
        """Get the number of pending jobs."""
        return sum(len(v) for _, v in self._waiting_groups.items())

    def _count_working(self) -> int:
        """Get the number of running jobs."""
        return len(self._working_queue)

    def _count_finished(self) -> int:
        """Get the number of completed jobs."""
        return len(self._finished_queue)

    def _count_remaining(self) -> int:
        """Get the number of unfinished jobs (i.e., waiting + working)."""
        return self.size(waiting=True, working=True)

    def _sizes(
        self,
        *,
        waiting: bool = False,
        working: bool = False,
        finished: bool = False,
    ) -> list[int]:
        """Returns the number of jobs in the corresponding queue(s).

        Args:
            waiting: include jobs in the waiting queue?
                - Accepts: bool
                - Default: False
            working: include jobs in the working table?
                - Accepts: bool
                - Default: False
            finished: include jobs in the completed queue?
                - Accepts: bool
                - Default: False

        Note: when all are False, all jobs are counted (default).

        Returns:
            int
        """

        counts = {}

        is_locked = False

        try:
            to_tally = sum([waiting, working, finished])
            if to_tally != 1:
                if to_tally == 0:
                    waiting, working, finished = True, True, True
                # must lock when more than 1 component included.
                is_locked = self._lock.acquire()

            if waiting:
                counts["waiting"] = self._count_waiting()
            if working:
                counts["working"] = self._count_working()
            if finished:
                counts["finished"] = self._count_finished()
        finally:
            if is_locked:
                self._lock.release()

        return list(counts.values())

    def __len__(self) -> int:
        return self.size()

    def size(
        self,
        *,
        waiting: bool = False,
        working: bool = False,
        finished: bool = False,
    ) -> int:
        """Get the number of jobs in the queue in state: waiting, working, and/or finished.

        If no options are given, the total number of jobs in the queue is returned.

        Args:
            waiting: include waiting jobs.
            working: include working jobs.
            finished: include finished jobs.

        Examples:
            >>> from ppqueue import Queue
            ...
            >>> def add_nums(x: int, y: int) -> int:
            ...     return x + y
            ...
            >>> with Queue() as queue:
            ...     for i in range(5):
            ...         _ = queue.enqueue(add_nums, args=[i, 100])
            ...         print(queue.size())
            1
            2
            3
            4
            5
        """
        return sum(self._sizes(waiting=waiting, working=working, finished=finished))

    def is_idle(self) -> bool:
        return self._count_waiting() == 0 and self._count_working() == 0

    def is_busy(self) -> bool:
        """True if max concurrent limit is reached or if there are waiting jobs."""
        return self._count_waiting() > 0 or self.max_concurrent <= self._count_working()

    def is_empty(self) -> bool:
        """True if there are no jobs in the queue system."""
        return self.size() == 0

    def is_full(self) -> bool:
        """True if the number of jobs in the queue system is equal to max_size."""
        return 0 < self._max_size <= self.size()

    def join(self, *args, **kwargs) -> int:
        return self.wait(*args, **kwargs)

    def wait(
        self,
        *,
        n: int = 0,
        timeout: float = 0,
        poll_ms: int = 0,
        show_progress: bool | None = None,
    ) -> int:
        """Wait for jobs to finish.

        Args:
            n: the number of jobs to wait for (default=0, all).
            timeout: seconds to wait before raising `TimeoutError` (default=0, indefinitely).
            poll_ms: milliseconds to pause between checks (default=100).
            show_progress: if True, present a progress bar.
        """
        if poll_ms is None or poll_ms <= 0:
            poll_ms = self._pulse_freq_ms
        elif poll_ms < self._pulse_freq_ms:
            err: str = "`poll_ms` must be >= `ppqueue.Queue.pulse_freq_ms`"
            raise ValueError(err)

        if show_progress is None:
            show_progress = self.show_progress

        _fun: Callable[..., Any] | None = None
        _target_value: int | None = None
        _comparator: Callable[..., bool] | None = None

        if n <= 0:
            _fun = self._count_remaining
            _target_value = 0
            _comparator = int.__le__
        else:
            _fun = self._count_finished
            _target_value = n
            _comparator = int.__ge__

        current_value: int = _fun()

        if not _comparator(current_value, _target_value):
            start = time.time()

            pb = None

            try:
                if show_progress:
                    pb = tqdm(total=current_value, unit="op")

                while not _comparator(current_value, _target_value) and (
                    timeout == 0 or time.time() - start < timeout
                ):
                    time.sleep(poll_ms / 1000)

                    tmp_value = _fun()
                    if pb is None:
                        current_value = tmp_value
                    else:
                        diff_value = current_value - tmp_value
                        if diff_value > 0:
                            current_value = tmp_value
                            pb.update(diff_value)
            finally:
                if pb:
                    pb.close()

        return current_value

    @staticmethod
    def _job_wrapper(
        _job: Job,
        _output: dict[int, tuple[str | None, str | None, int, float]],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[str | None, str | None, int, float]:
        """Used internally to wrap data, capture output and any exception."""
        stdout: str | None = None
        stderr: str | None = None
        exitcode: int
        timestamp: float

        try:
            stdout = _job.fun(*args, **kwargs)
            exitcode = 0
        except Exception as ex:  # pylint: disable=broad-exception-caught
            stderr = traceback.format_exc()
            exitcode = -1
            if not _job.suppress_errors:
                raise ex
        finally:
            timestamp = time.time()
            _output[_job.idx] = (stdout, stderr, exitcode, timestamp)

    def _start_job(self, job: Job, /) -> None:
        inner_job: mp.Process | threading.Thread = self._engine(
            name=str(job.idx),
            target=self._job_wrapper,
            args=[job, self._output, *job.args],
            kwargs=job.kwargs,
        )

        LOG.debug("starting job %d (name=%s)", job.idx, job.name)
        inner_job.start()

        job.start_timestamp = time.time()
        job.inner_job = inner_job

    def _submit(self, job: Job, /) -> int:
        """Submits a job into the ppqueue.Queue system.

        Throws an exception if:
            1. the Queue uses a Thread job_runner and this data has a timeout (can't terminate Threads),
            2. the Queue max_size will be exceeded after adding this job.

        Returns:
            The number of jobs submitted to the queue.
        """

        assert not (self._engine is threading.Thread and job.timeout > 0)

        if self.is_full():
            raise OverflowError("Max queue size exceeded.")

        job.submit_timestamp = time.time()

        job.qid = self._qid
        self._count_input += 1
        job.idx = self._count_input

        if job.name is None:
            job.name = job.idx

        LOG.debug("queuing job %d (name=%s)", job.idx, job.name)

        group_jobs = self._waiting_groups.get(job.group)
        if group_jobs is not None:
            heappush(group_jobs, job)
        else:
            group_jobs = []
            self._waiting_groups[job.group] = group_jobs

            # if this is the first job in this group,
            # it should be pushed to the default group.
            heappush(self._waiting_groups[self.DEFAULT_GROUP], job)

        return job.idx

    def enqueue(
        self,
        fun: Callable[..., Any],
        /,
        args: Sequence[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        name: str | None = None,
        priority: int = 100,
        group: int | None = None,
        timeout: float = 0,
        suppress_errors: bool = False,
        skip_on_group_error: bool = False,
    ) -> int:
        """Adds a job to the queue.

        Args:
            args:
            kwargs:
            name:
            priority:
            group:
            timeout:
            suppress_errors:
            skip_on_group_error:

        Examples:
            >>> from ppqueue import Queue
            ...
            >>> def add_nums(x: int, y: int) -> int:
            ...     return x + y
            ...
            >>> with Queue() as queue:
            ...     for i in range(5):
            ...         _ = queue.enqueue(add_nums, args=[i, 100])
            ...
            ...     jobs = queue.collect(wait=True)
            ...
            >>> [job.result for job in jobs]
            [100, 101, 102, 103, 104]
        """
        job = Job(
            fun=fun,
            args=args,
            kwargs=kwargs,
            name=name,
            priority=priority,
            group=group,
            timeout=timeout,
            suppress_errors=suppress_errors,
            skip_on_group_error=skip_on_group_error,
        )

        return self._submit(job)

    def put(self, *args, **kwargs) -> int:
        """Alias for `enqueue`. Adds a job to the queue.

        Examples:
            >>> from ppqueue import Queue
            ...
            >>> def add_nums(x: int, y: int) -> int:
            ...     return x + y
            ...
            >>> with Queue() as queue:
            ...     for i in range(5):
            ...         _ = queue.put(add_nums, args=[i, 100])
            ...     jobs = queue.collect(wait=True)
            ...
            >>> [job.result for job in jobs]
            [100, 101, 102, 103, 104]
        """

        return self.enqueue(*args, **kwargs)

    def map(
        self,
        fun: Callable[..., Any],
        iterable: Sequence[Any],
        /,
        *args: Any,
        timeout: float = 0,
        show_progress: bool | None = None,
        **kwargs: Any,
    ) -> list[Job]:
        """Submits many jobs to the queue -- one for each item in the iterable. Waits for all to finish, then returns the results.

        Args:
            fun:
            iterable:
            timeout:
            show_progress:

        Examples:
            >>> from ppqueue import Queue
            >>> from time import sleep
            ...
            >>> with Queue() as queue:
            ...     jobs = queue.map(sleep, [1, 2, 3, 4, 5])
            ...
            >>> len(jobs)
            5
        """
        for x in iterable:
            job = Job(
                fun=fun,
                args=[x, *args],
                kwargs=dict(kwargs),
                timeout=timeout,
            )
            self._submit(job)

        return self.collect(wait=True, show_progress=show_progress)

    def starmap(
        self,
        fun: Callable[..., Any],
        iterable: Sequence[Sequence[Any]],
        /,
        *args: Any,
        timeout: float = 0,
        show_progress: bool | None = None,
        **kwargs: Any,
    ) -> list[Job]:
        """Submits many jobs to the queue -- one for each sequence in the iterable. Waits for all to finish, then returns the results.

        Args:
            fun:
            iterable:
            timeout:
            show_progress:

        Examples:
            >>> from ppqueue import Queue
            ...
            >>> def add_nums(x: int, y: int) -> int:
            ...     return x + y
            ...
            >>> with Queue() as queue:
            ...     jobs = queue.starmap(
            ...         add_nums, [(1, 2), (3, 4)]
            ...     )
            ...
            >>> [job.result for job in jobs]
            [3, 7]
        """
        for x in iterable:
            job = Job(
                fun=fun,
                args=[*x, *args],
                kwargs=dict(kwargs),
                timeout=timeout,
            )
            self._submit(job)

        return self.collect(wait=True, show_progress=show_progress)

    def starmapkw(
        self,
        fun: Callable[..., Any],
        iterable: Sequence[dict[str, Any]],
        /,
        *args: Any,
        timeout: float = 0,
        show_progress: bool | None = None,
        **kwargs: Any,
    ) -> list[Job]:
        """Submits many jobs to the queue -- one for each dictionary in the iterable. Waits for all to finish, then returns the results.

        Args:
            fun:
            iterable:
            timeout:
            show_progress:

        Examples:
            >>> from ppqueue import Queue
            >>> from time import sleep
            ...
            >>> def add_nums(x: int, y: int) -> int:
            ...     return x + y
            ...
            >>> with Queue() as queue:
            ...     jobs = queue.starmapkw(
            ...         add_nums, [{'x': 1, 'y': 2}, {'x': 3, 'y': 4}]
            ...     )
            ...
            >>> [job.result for job in jobs]
            [3, 7]
        """
        for x in iterable:
            job = Job(
                fun=fun,
                args=list(args),
                kwargs={**x, **kwargs},
                timeout=timeout,
            )
            self._submit(job)

        return self.collect(wait=True, show_progress=show_progress)

    def dequeue(
        self,
        *,
        wait: bool = False,
        _peek: bool = False,
        **kwargs: Any,
    ) -> Job | None:
        """Removes and returns the job with the highest priority from the queue.

        Args:
            wait:
            **kwargs: passed to `Queue.wait`

        Examples:
            >>> from ppqueue import Queue
            ...
            >>> def add_nums(x: int, y: int) -> int:
            ...     return x + y
            ...
            >>> with Queue() as queue:
            ...     for i in range(5):
            ...         _ = queue.enqueue(add_nums, args=[i, 100])
            ...
            ...     jobs = [queue.dequeue(wait=True) for _ in range(queue.size())]
            ...
            >>> [job.result for job in jobs]
            [100, 101, 102, 103, 104]
        """

        if self._count_finished() == 0:
            if not wait:
                return None

            kwargs["show_progress"] = False
            self.wait(n=1, **kwargs)

        job: Job
        if _peek:
            job = self._finished_queue[0]
        else:
            job = heappop(self._finished_queue)
            self._count_output += 1

        return job

    def pop(self, *args: Any, **kwargs: Any) -> Job | None:
        """Alias for `dequeue`. Removes and returns the job with the highest priority from the queue.

        Examples:
            >>> from ppqueue import Queue
            ...
            >>> def add_nums(x: int, y: int) -> int:
            ...     return x + y
            ...
            >>> with Queue() as queue:
            ...     for i in range(5):
            ...         _ = queue.put(add_nums, args=[i, 100])
            ...
            ...     jobs = [queue.pop(wait=True) for _ in range(queue.size())]
            ...
            >>> [job.result for job in jobs]
            [100, 101, 102, 103, 104]
        """
        return self.dequeue(*args, **kwargs)

    def peek(self, *args: Any, **kwargs: Any) -> Job | None:
        """Returns the job with the highest priority from the queue. Similar to `enqueue` / `pop`, but the job remains in the queue.

        Args:
            *args:
            **kwargs:

        Examples:
            >>> from ppqueue import Queue
            ...
            >>> def add_nums(x: int, y: int) -> int:
            ...     return x + y
            ...
            >>> with Queue() as queue:
            ...     for i in range(5):
            ...         _ = queue.put(add_nums, args=[i, 100])
            ...
            ...     print('Before:', queue.size())
            ...
            ...     job = queue.peek(wait=True)
            ...
            ...     print('After:', queue.size())
            ...
            Before: 5
            After: 5
            >>> job.result
            100
        """
        return self.dequeue(*args, **kwargs, _peek=True)

    def collect(self, n: int = 0, wait: bool = False, **kwargs: Any) -> list[Job]:
        """Removes and returns all finished jobs from the queue.

        Args:
            n: collect this many jobs (default=0, all)
            wait: If True, block until this many jobs are finished. Else, immediately return all finished.
            **kwargs: kwargs given to `Queue.wait`.

        Examples:
            >>> from ppqueue import Queue
            ...
            >>> def add_nums(x: int, y: int) -> int:
            ...     return x + y
            ...
            >>> with Queue() as queue:
            ...     for i in range(5):
            ...         _ = queue.enqueue(add_nums, args=[i, 100])
            ...
            ...     jobs = queue.collect(wait=True)
            ...
            >>> type(jobs)
            <class 'list'>
            >>> len(jobs)
            5
        """

        if wait:
            self.wait(n=n, **kwargs)

        n_finished: int = self._count_finished()

        n_to_collect: int = min(n, n_finished) if n > 0 else n_finished

        return list(next(self) for _ in range(n_to_collect))

    @property
    def max_concurrent(self) -> int:
        return self._max_concurrent

    @max_concurrent.setter
    def max_concurrent(self, value: int):
        self._max_concurrent = value

    @property
    def max_size(self) -> int:
        return self._max_size

    @max_size.setter
    def max_size(self, value: int) -> None:
        self._max_size = value

    @property
    def is_running(self) -> bool:
        return self._timer.is_enabled
