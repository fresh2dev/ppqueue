import logging
import multiprocessing as mp
import threading
import time
import traceback
from collections import defaultdict
from heapq import heappop, heappush
from typing import Any, Callable, Dict, List, Optional, Sequence, Union, Type
from uuid import uuid4

try:
    from tqdm.auto import tqdm
except ImportError as e:
    print("Unable to import `tqdm`; continuing.")

from ezpq.FinishedJob import FinishedJob
from ezpq.Job import Job
from ezpq.utils import get_logger, log_csv, is_windows_os

LOG = get_logger(__name__, level=logging.INFO)


class Queue(object):
    class RepeatedTimer(object):
        """Periodically runs a function in a background thread.
        from: https://stackoverflow.com/a/13151299

        used to run `ezpq.Queue._pulse` asynchronously.
        """

        def __init__(self, interval_ms: int, fun: Callable, *args, **kwargs):
            self._timer: Optional[threading.Timer] = None
            self._interval: int = interval_ms
            self._function: Callable = fun
            self._args: Sequence[Any] = args
            self._kwargs: Dict[str, Any] = kwargs
            self._is_enabled: bool = False
            self._is_running: bool = False

        def _run(self):
            self._is_running = False
            self.start()
            self._function(*self._args, **self._kwargs)

        def start(self):
            if not self._is_running:
                self._timer = threading.Timer(self._interval / 1000, self._run)
                self._timer.start()
                self._is_running = True

            self._is_enabled = True

        def stop(self):
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

    DEFAULT_LANE: Optional[str] = None

    def __init__(
            self,
            max_concurrent: int = mp.cpu_count(),
            engine: Union[threading.Thread, mp.Process] = threading.Thread,
            name: Optional[str] = None,
            drop_finished: bool = False,
            start: bool = True,
            stop_when_idle: bool = False,
            callback: Optional[Callable] = None,
            output_file: Optional[str] = None,
            pulse_freq_ms: int = 100,
            show_progress: bool = False,
            max_size: int = 0,
    ):
        """Implements a parallel queueing system.

        Args:
            max_concurrent: the max number of concurrent jobs.
                - Accepts: int
                - Default: cpu_count()
            max_size: when > 0, will throw an exception the number of enqueued jobs exceeds this value. Otherwise, no limit.
                - Accepts: int
                - Default: 0 (unlimited)
            engine: the class to use to invoke new jobs.
                - Accepts: multiprocessing.Process, threading.Thread
                - Default: multiprocessing.Process
            drop_finished: controls whether jobs are discarded of after completion.
                - Accepts: bool
                - Default: False
            start: controls whether the queue system "pulse" is started upon instantiation (default), or manually.
                - Accepts: bool
                - Default: True
            stop_when_idle: controls whether the queue system "pulse" stops itself after all jobs are complete.
                - Accepts: bool
                - Default: False
            callback: optional fun to execute synchronously immediately after a data completes.
                - Accepts: fun object
                - Default: None
            output_file: if file path is specified, job data is written to this path in CSV format.
                - Accepts: str
                - Default: None
            pulse_freq_ms: controls the pulse frequency; the amount of time slept between operations.
                - Accepts: float
                - Default: 0.1

        Returns:
            ezpq.Queue object.
        """

        self._qid: str = name if name else str(uuid4())[:8]

        self.show_progress: bool = show_progress
        self._max_size: int = max_size
        self._count_input: int = 0
        self._count_output: int = 0
        self._max_concurrent: int = max_concurrent
        self._callback: Callable = callback
        self._log_file: str = output_file

        self._lock: threading.Lock = (
            threading.Lock()
        )  # https://opensource.com/article/17/4/grok-gil

        if engine is mp.Process and is_windows_os():
            LOG.warning(
                "multiprocessing performance is degraded on Windows systems. see: "
                "https://docs.python.org/3/library/multiprocessing.html?highlight=process#the-spawn-and-forkserver-start-methods"
            )

        self._engine: Union[mp.Process, threading.Thread] = engine

        self._waiting_lanes: Dict[Optional[str], List[Job]] = {None: []}
        self._working_queue: Dict[int, Job] = {}
        self._finished_queue: List[FinishedJob] = []

        self._drop_finished: bool = drop_finished
        self._mp_manager: Optional[mp.Manager] = None

        if self._engine is mp.Process:
            self._mp_manager = mp.Manager()
            self._output = self._mp_manager.dict()
        else:
            self._output = {}

        LOG.debug("Initialized queue with %d max_concurrent.", self._max_concurrent)

        self._pulse_freq_ms: int = pulse_freq_ms
        self._timer: Queue.RepeatedTimer = Queue.RepeatedTimer(
            interval_ms=self._pulse_freq_ms, fun=self._pulse
        )

        if start is True:
            self.start()

        self._stop_when_idle: bool = stop_when_idle

        LOG.debug("Initialized pulse.")

    def __call__(self, fun, *args, **kwargs):
        """Decorator guided by http://scottlobdell.me/2015/04/decorators-arguments-python/"""

        self.start()

        def wrapped_f(iterable, *args, **kwargs):
            for x in iterable:
                self.put(fun=fun, args=[x] + list(args), kwargs=kwargs)
            self.wait()
            job_data = self.collect()
            self.dispose()
            return job_data

        return wrapped_f

    def __del__(self):
        self.dispose()

    def start(self):
        """Starts the queue system pulse."""
        LOG.debug("Starting pulse.")
        self._timer.start()

    def stop(self):
        """Stops the queue system pulse."""
        LOG.debug("Stopping pulse.")
        self._timer.stop()

    def dispose(self):
        """Clears all output and stops the queue system pulse."""
        LOG.debug("Disposing")

        if self.is_running:
            self.stop_all(wait=True)
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

    def clear_waiting(self):
        LOG.debug("Clearing waiting queue")
        with self._lock:
            keys = list(self._waiting_lanes.keys())
            for k in keys:
                lane_jobs = self._waiting_lanes.get(k)
                for _ in range(len(lane_jobs)):
                    job = heappop(lane_jobs)
                    job._cancelled = True
                    job._process_time = time.time()
                    if not self._drop_finished:
                        heappush(self._finished_queue, FinishedJob(job))
                    else:
                        self._count_output += 1
                del self._waiting_lanes[k]

    def stop_all(self, wait=True):
        """Stops working jobs and clears waiting jobs."""

        self.clear_waiting()

        keys = list(self._working_queue.keys())
        for k in keys:
            job = self._working_queue.get(k)
            if job is not None:
                job.stop()

        if wait:
            self.wait()

    def __enter__(self):
        LOG.debug("Entered context manager")
        return self

    def __exit__(self, *args):
        LOG.debug("Exiting context manager")
        self.dispose()

    def _pulse(self):
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
                                    job_data = self._output.pop(job.idx)
                                    job._finish_time = job_data["_finish_time"]
                                    job._output = job_data["_output"]
                                    job._exitcode = job_data["_exitcode"]
                                    job._exception_txt = job_data["_exception_txt"]
                                except KeyError as ex:
                                    job._finish_time = time.time()
                                    job._output = None
                                    job._exitcode = 1
                                    job._exception_txt = Exception(
                                        "{}\n\nNo data for data; it may have exited unexpectedly.".format(
                                            str(ex)
                                        )
                                    )

                            if self._callback is not None:
                                # TODO: write this to a new property.
                                try:
                                    job._callback = self._callback(job)
                                except Exception as ex:
                                    job._callback = str(ex)

                            job._process_time = time.time()

                            LOG.debug("Completed job: %d", job.idx)

                            finished_job = FinishedJob(job)

                            if not self._drop_finished:
                                heappush(self._finished_queue, finished_job)
                            else:
                                self._count_output += 1

                            if self._log_file is not None:
                                # TODO: use a persistent file connection.
                                log_csv(data=finished_job.__dict__, path=self._log_file)

                            del self._working_queue[job_id]

                            if job.lane is not None and job.lane != Queue.DEFAULT_LANE:
                                lane_jobs: List[Job] = self._waiting_lanes.get(job.lane)
                                if lane_jobs is not None:
                                    next_job = None
                                    parent_exitcode = job.exitcode
                                    while len(lane_jobs) > 0:
                                        next_job = heappop(lane_jobs)

                                        if parent_exitcode == 0 or not next_job._skip_on_lane_error:
                                            break
                                        else:
                                            next_job._cancelled = True
                                            next_job._exitcode = parent_exitcode
                                            next_job._exception_txt = "skip_on_lane_error = True and preceding data ({}) exit code is {}".format(
                                                job.idx, parent_exitcode
                                            )
                                            next_job._process_time = time.time()
                                            if not self._drop_finished:
                                                heappush(
                                                    self._finished_queue,
                                                    FinishedJob(next_job),
                                                )
                                            else:
                                                self._count_output += 1
                                            next_job = None
                                            continue

                                    if len(lane_jobs) == 0:
                                        del self._waiting_lanes[job.lane]

                                    if next_job is not None:
                                        self._working_queue[next_job.idx] = next_job
                                        self._start_job(job=next_job)

                    while len(self._waiting_lanes[Queue.DEFAULT_LANE]) > 0 \
                            and self.max_concurrent - self.count_working() > 0:
                        job = heappop(self._waiting_lanes[Queue.DEFAULT_LANE])
                        self._working_queue[job.idx] = job
                        self._start_job(job=job)
            except:
                self.stop()
                LOG.error(traceback.format_exc())
            finally:
                LOG.debug(
                    "waiting={}; working={}; finished={}.".format(
                        self.count_waiting(),
                        self.count_working(),
                        self.count_finished()
                    )
                )

    def count_input(self) -> int:
        return self._count_input

    def count_output(self) -> int:
        return self._count_output

    def count_waiting(self) -> int:
        return sum(len(v) for _, v in self._waiting_lanes.items())

    def count_working(self) -> int:
        return len(self._working_queue)

    def count_finished(self) -> int:
        return len(self._finished_queue)

    def count_remaining(self) -> int:
        return sum(self.count_queues(waiting=True, working=True))

    def __len__(self):
        return self.size()

    def size(self, *args, **kwargs) -> List[int]:
        return sum(self.count_queues(*args, **kwargs))

    def count_queues(self, waiting=False, working=False, finished=False) -> List[int]:
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

        counts = defaultdict(lambda: -1)

        is_locked = False

        try:
            to_tally = sum([waiting, working, finished])
            if to_tally != 1:
                if to_tally == 0:
                    waiting, working, finished = True, True, True
                is_locked = (
                    self._lock.acquire()
                )  # must lock when more than 1 component included.

            if waiting:
                counts['waiting'] = self.count_waiting()
            if working:
                counts['working'] = self.count_working()
            if finished:
                counts['finished'] = self.count_finished()
        finally:
            if is_locked:
                self._lock.release()

        return [v for _, v in counts.items() if v != -1]

    def is_idle(self) -> bool:
        return self.count_waiting() == 0 and self.count_working() == 0

    def is_busy(self) -> bool:
        """True if max concurrent limit is reached or if there are waiting jobs."""
        return self.count_waiting() > 0 or self.max_concurrent <= self.count_working()

    def empty(self):
        """True if there are no jobs in the queue system."""
        return self.size() == 0

    def full(self):
        """True if the number of jobs in the queue system is equal to max_size."""
        return 0 < self._max_size <= self.size()

    def join(self, *args, **kwargs):
        self.wait(*args, **kwargs)

    def wait(self,
             timeout: Optional[int] = 0,
             poll_ms: Optional[int] = 0,
             show_progress: Optional[bool] = None,
             _fun: Optional[Callable] = None,
             _target_value: Optional[int] = None,
             _comparator: Optional[int] = None
             ):
        """Waits for jobs to be finished by the queue system.

        Args:
            _comparator:
            _target_value:
            _fun:
            poll_ms: the time, in seconds, between checks.
                - Accepts: float
                - Default: ezpq.Queue.poll_ms
            timeout: when > 0, the maximum time to wait, in seconds. Otherwise, no limit.
                - Accepts: float
                - Default: 0 (unlimited)
            show_progress: show `tqdm` progress bar; (pass static_args to `waitpb`)
                - Accepts: bool
                - Default: False

        Returns:
            0 if the expected number of jobs finished. > 0 otherwise.
        """

        if poll_ms is None or poll_ms <= 0:
            poll_ms = self._pulse_freq_ms
        elif poll_ms < self._pulse_freq_ms:
            raise ValueError("`poll_ms` must be >= `ezpq.Queue.pulse_freq_ms`")

        if show_progress is None:
            show_progress = self.show_progress

        if _fun is None:
            _fun = self.count_remaining

        if _target_value is None:
            _target_value = 0

        if _comparator is None:
            _comparator = int.__le__

        current_value: int = _fun()

        if not _comparator(current_value, _target_value):
            start = time.time()

            pb = None

            try:
                if show_progress:
                    pb = tqdm(total=current_value, unit="op")

                while not _comparator(current_value, _target_value) and (timeout == 0 or time.time() - start < timeout):
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

    # TODO: return a `FinishedJob`
    @staticmethod
    def _job_wrapper(_job: Job, _output, *args, **kwargs):
        """Used internally to wrap a data, capture output and any exception."""
        out = None
        ex_obj: Optional[object] = None
        ex_msg: Optional[str] = None
        code: int = 0

        try:
            out = _job.fun(*args, **kwargs)
        except Exception as ex:
            ex_obj = ex
            ex_msg = traceback.format_exc()
            code = -1
        finally:
            # TODO: create class of this.
            _output.update(
                {
                    _job.idx: {
                        "_finish_time": time.time(),
                        "_output": out,
                        "_exception_txt": ex_msg,
                        "_exitcode": code,
                    }
                }
            )

        if not _job.suppress_errors and ex_obj is not None:
            raise ex_obj

    def _start_job(self, job):
        """Internal; invokes jobs."""

        job_args = {}

        if job.args is not None:
            # if not isinstance(data.static_args, list):
            #     job_args['static_args'] = [data, self._output, data.static_args]
            # else:
            job_args["args"] = [job, self._output] + job.args

        if job.kwargs is None:
            job_args["kwargs"] = {}
        else:
            job_args["kwargs"] = dict(job.kwargs)

        if job.args is None:
            job_args["kwargs"].update({"_job": job, "_output": self._output})

        j: Union[Type[mp.Process], Type[threading.Thread]] = self._engine(
            name=str(job.idx), target=Queue._job_wrapper, **job_args
        )

        LOG.debug("starting job %d (name=%s)", job.idx, job.name)
        j.start()

        job._start_time = time.time()
        job._inner_job = j

    def submit(self, job):
        """Submits a data into the ezpq.Queue system.

        Throws an exception if:
            1. the Queue uses a Thread job_runner and this data has a timeout (can't terminate Threads),
            2. the Queue max_size will be exceeded after adding this data.

        Returns:
            The number of jobs submitted to the queue.
        """

        assert not (self._engine is threading.Thread and job.timeout > 0)

        if self.full():
            raise OverflowError("Max size exceeded.")

        job._submit_time = time.time()

        job._qid = self._qid
        self._count_input += 1
        job.idx = self._count_input

        if job.name is None:
            job.name = job.idx

        LOG.debug("queuing job %d (name=%s)", job.idx, job.name)

        lane_jobs = self._waiting_lanes.get(job.lane)
        if lane_jobs is not None:
            heappush(lane_jobs, job)
        else:
            lane_jobs = []
            self._waiting_lanes[job.lane] = lane_jobs

            # if this is the first job in this lane,
            # it should be pushed to the default lane.
            heappush(self._waiting_lanes[Queue.DEFAULT_LANE], job)

        return job.idx

    def put(self, *args, **kwargs):
        """Creates a job and submits it to an ezpq queue.
        see `help(ezpq.Job.__init__)`"""

        job = Job(*args, **kwargs)

        return self.submit(job)

    def map(
            self,
            fun,
            iterable,
            static_args=None,
            static_kwargs=None,
            timeout=0,
            show_progress=None,
    ):

        assert hasattr(iterable, "__iter__")

        if static_args is None:
            static_args = [None]
        elif not hasattr(static_args, "__iter__"):
            static_args = [None] + [static_args]
        elif len(static_args) == 0:
            static_args = [None]
        else:
            static_args = [None] + list(static_args)

        for x in iterable:
            static_args[0] = x
            job = Job(
                fun=fun, args=list(static_args), kwargs=static_kwargs, timeout=timeout
            )
            self.submit(job)

        self.wait(show_progress=show_progress)

        return self.collect()

    def starmap(
            self,
            fun,
            iterable,
            static_args=None,
            static_kwargs=None,
            timeout=0,
            show_progress=None,
    ):

        assert hasattr(iterable, "__iter__")

        if static_args is None:
            static_args = []
        elif not isinstance(static_args, list):
            static_args = list(static_args)

        for x in iterable:
            job = Job(
                fun=fun,
                args=list(x) + static_args,
                kwargs=static_kwargs,
                timeout=timeout,
            )
            self.submit(job)

        self.wait(show_progress=show_progress)

        return self.collect()

    def starmapkw(
            self,
            fun,
            iterable,
            static_args=None,
            static_kwargs=None,
            timeout=0,
            show_progress=None,
    ):

        assert hasattr(iterable, "__iter__")

        if static_kwargs is None:
            static_kwargs = {}
        elif not isinstance(static_kwargs, dict):
            static_kwargs = dict(static_kwargs)

        for x in iterable:
            job = Job(
                fun=fun,
                args=static_args,
                kwargs={**x, **static_kwargs},
                timeout=timeout,
            )
            self.submit(job)

        self.wait(show_progress=show_progress)

        return self.collect()

    def get(self, wait: Optional[bool] = False, **kwargs) -> Optional[FinishedJob]:
        """Pops the highest priority item from the finished queue.

        Args:
            wait:
            poll_ms: when > 0, time between checks
                - Accepts: float
                - Default: 0 (no wait)
            timeout: the maximum time, in seconds, to wait for a data to complete.
                - Accepts: float
                - Default: 0 (no wait/unlimited wait)

        Notes:
            - when both poll_ms and timeout are 0, only one check is done;
            - when either is > 0, the method will block until output is available.

        Returns:
            Dictionary of the most recently finished, highest priority data.
        """

        if self._drop_finished:
            raise ValueError("`get` is not applicable when `drop_finished` is True.")

        if self.count_finished() == 0:
            if wait:
                kwargs['show_progress'] = False
                self.wait(_fun=self.count_finished, _target_value=1, _comparator=int.__ge__, **kwargs)
            else:
                return None

        job: FinishedJob = heappop(self._finished_queue)
        self._count_output += 1
        return job

    def collect(self, n=0):
        """Repeatedly calls `get()` and returns a list of job data.

        Args:
            n: the number of jobs to pop from the finished queue.
                - Accepts: int
                - Default: 0 (all)

        Returns:
            a list of dictionary objects.
        """

        if n <= 0:
            n = self.count_finished()
        else:
            n = min(n, self.count_finished())

        return [self.get() for _ in range(n)]

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
