__version__ = "0.1.0"

#%% imports

import multiprocessing as mp
import threading as thread
from heapq import heappush, heappop
import os
import csv
import time
from datetime import datetime

import logging as log

#%%

class Plot():
    """Functions to produce Gantt chart from completed job schedules."""

    def __init__(self, jobs, color_by='priority', show_legend=True, bar_width=1, title=None):

        import pandas

        assert(color_by in ['priority', 'cancelled', 'exitcode', 'name', 'output'])
        self.color_by = color_by
        self.show_legend = show_legend
        self.bar_width = bar_width
        self.title = title

        df = pandas.DataFrame(jobs)
        min_time = df['submitted'].min()
        df['submitted_offset'] = df['submitted'] - min_time
        df['started_offset'] = df['started'] - min_time
        df['ended_offset'] = df['ended'] - min_time
        df['processed_offset'] = df['processed'] - min_time
        self.jobs_df = df

    def _plot_theme(self, grid_axis='both', grid_lines='both'):
        """Internal function provides consistent theme across plots.
        Currently a slightly modified version of theme_bw() with configurable grid lines.

        Args:
            grid_axis: controls the axis on which to draw grid lines; accepts: None, 'x', 'y', 'both'
            grid_lines: controls whether major or minor grid lines are drawn; accepts: None, 'major', 'minor', 'both'

        Returns:
            A theme object to be added to a plotnine.ggplot() object.
        """

        import plotnine as gg

        assert(grid_axis in [None, 'x', 'y', 'both'])
        assert(grid_lines in [None, 'major', 'minor', 'both'])

        drop_grid = set()

        if grid_axis is None or grid_lines is None:
            drop_grid.update(['panel_grid_major', 'panel_grid_minor'])
        elif grid_axis == 'x':
            drop_grid.update(['panel_grid_major_y', 'panel_grid_minor_y'])
            if grid_lines == 'major':
                drop_grid.add('panel_grid_minor_y')
            elif grid_lines == 'minor':
                drop_grid.add('panel_grid_major_y')
        elif grid_axis == 'y':
            drop_grid.update(['panel_grid_major_x', 'panel_grid_minor_x'])
            if grid_lines == 'major':
                drop_grid.add('panel_grid_minor_x')
            elif grid_lines == 'minor':
                drop_grid.add('panel_grid_major_x')

        grid_opt = dict()
        for x in drop_grid:
            grid_opt[x] = gg.element_blank()

        return gg.theme_bw() + \
                gg.theme(panel_border = gg.element_blank(),
                          axis_line = gg.element_line(color = "black"),
                          **grid_opt)

    # def save(self, *args, **kwargs):
    #     plt = self.show()
    #     plt.save(*args, **kwargs)

    def build(self):
        """Produces a Gannt chart from a dictionary of completed job schedules.

        Args:
            jobs: a dictionary of jobs obtained from an EZPQ Queue.
                - Accepts: dict()
            color_by: the name of the field used to color the bars
                - Accepts: 'cancelled', 'exitcode', 'name', 'output', 'priority'
                - Default: 'priority'
            show_legend: controls whether the legend key is drawn.
                - Accepts: bool()
                - Default: True
            bar_width: controls the size of the bar width.
                - Accepts: int(), float()
                - Default: 1
            title: optional title text.
                - Accepts: str()
                - Default: None

        Returns:
            The Gannt chart produced from plotnine.ggplot().
        """

        import plotnine as gg

        df2 = self.jobs_df.loc[:, ['id', self.color_by, 'submitted_offset', 'started_offset', 'ended_offset', 'processed_offset']].melt(id_vars=['id', self.color_by])

        df_submit_start = df2[(df2['variable'] == 'submitted_offset') | (df2['variable'] == 'started_offset')]
        df_start_end = df2[(df2['variable'] == 'started_offset') | (df2['variable'] == 'ended_offset')]
        df_end_processed = df2[(df2['variable'] == 'ended_offset') | (df2['variable'] == 'processed_offset')]

        labs = { 'x': 'duration', 'y': 'job id' }
        if self.title is not None:
            labs['title'] = self.title

        return gg.ggplot(gg.aes(x='value', y='id', group='factor(id)')) + \
                gg.geom_line(df_submit_start, color='gray', size=self.bar_width, alpha=0.25) + \
                gg.geom_line(df_start_end,
                             gg.aes(color='factor({})'.format(self.color_by)),
                             size=self.bar_width, show_legend=bool(self.show_legend)) + \
                gg.geom_line(df_end_processed, color='gray', size=self.bar_width, alpha=0.25) + \
                gg.labs(**labs) + \
                gg.labs(color=self.color_by) + \
                self._plot_theme(grid_axis='x') + \
                gg.scale_color_hue(h=.65)


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
        self._timer.cancel()
        self.is_running = False


class Job():
    """Example function with PEP 484 type annotations.

    Args:
        param1: The first parameter.
        param2: The second parameter.

    Returns:
        The return value. True for success, False otherwise.
    """

    def __init__(self, function, args=None, kwargs=None, name=None, priority=100, timeout=0):
        self._id = None # assigned by the Mgr.
        self.name = name
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
        return self._inner_job is not None and self._inner_job.is_alive()

    def is_expired(self):
        return self.is_running() and \
                self.timeout > 0 and \
                self._started is not None and \
                self._ended is None and \
                self._started + self.timeout < time.time()

    def _join(self):
        if self._inner_job is not None:
            self._inner_job.join()

    def get_exitcode(self):
        if self._inner_job is not None and hasattr(self._inner_job, 'exitcode'):
            return self._inner_job.exitcode
        return None

    def _stop(self):
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
        if self._ended:
            return self._ended - self._started
        return None

    def waiting_time(self):
        if self._started:
            return self._started - self._submitted
        return time.time() - self._started

    def total_time(self):
        if self._ended:
            return self._ended - self._submitted
        return time.time() - self._submitted

    def get_submit_time(self):
        if self._submitted:
            return datetime.fromtimestamp(self._submitted)
        return None

    def get_start_time(self):
        if self._started:
            return datetime.fromtimestamp(self._started)
        return None

    def get_end_time(self):
        if self._ended:
            return datetime.fromtimestamp(self._ended)
        return None

    def get_processed_time(self):
        if self._processed:
            return datetime.fromtimestamp(self._processed)
        return None

    def is_processed(self):
        return self._processed is not None

    def __str__(self):
        return str(self.to_dict())

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        return {
            'id':self._id,
            'name':self.name,
            'priority': self.priority,
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


class Queue():
    """Example function with PEP 484 type annotations.

    Args:
        param1: The first parameter.
        param2: The second parameter.

    Returns:
        The return value. True for success, False otherwise.
    """

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
        """Example function with PEP 484 type annotations.

        Args:
            param1: The first parameter.
            param2: The second parameter.

        Returns:
            The return value. True for success, False otherwise.
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
        self._ticker = RepeatedTimer(interval=self._poll, function=self._pulse)
        if auto_start is True:
            self.start()
        self._auto_stop = auto_stop

        log.debug('Initialized pulse.')

    def __call__(self, fun, *args, **kwargs):
        '''Decorator inspired by http://scottlobdell.me/2015/04/decorators-arguments-python/'''
        if not self.is_started():
            self.start()

        def wrapped_f(items, *args, **kwargs):
            for x in items:
                self.put(function=fun, args=[x]+list(args), kwargs=kwargs)
            self.wait()
            job_data = self.collect()
            self.dispose()
            return job_data
        return wrapped_f

    # @staticmethod
    def job_wrap(self, _job, *args, **kwargs):
        out = None

        try:
            out = _job.function(*args, **kwargs)
        except Exception as ex:
            out = str(ex)
            raise Exception(out)

        self._output.update({ _job._id: {'_ended':time.time(), '_output':out} })

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
        self._ticker.start()

    def _stop(self):
        if self._ticker is not None:
            self._ticker.stop()
            log.debug('Stopped pulse.')

    def dispose(self):
        with self._lock:
            self.clear()
            self._stop()

    def stop_all(self, wait=False):

        keys = list(self._working.keys())
        for k in keys:
            job = self._working.get(k)
            if job is not None: job._stop()

        if wait: self.wait(n=0)

    def clear(self):
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
        return self._n_submitted

    def n_completed(self):
        return self._n_completed

    def n_workers(self):
        return self._n_workers

    def _pulse(self):
        start = time.time()

        if self._lock.locked():
            log.warning('Pulse already running. If you see this repeatedly, consider increasing the value of "poll", or removing a slow callback.')
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
        heappush(queue, (int(job.priority), job._submitted, job))

    def size(self, waiting=False, working=False, completed=False):
        size = 0
        if not any([waiting, working, completed]):
            waiting, working, completed = (True, True, True)

        if waiting: size += len(self._waiting)
        if working: size += len(self._working)
        if completed: size += len(self._completed)

        return size


    def has_waiting(self):
        return self.size(waiting=True) > 0

    def has_completed(self):
        return self.size(completed=True) > 0

    def has_work(self):
        return self.size(waiting=True, working=True) > 0

    def n_workers_free(self):
        return self._n_workers - self.size(working=True)

    def is_working(self):
        return self.size(working=True) > 0

    def is_busy(self):
        return self.n_workers_free() <= 0 or self.has_waiting()

    def is_started(self):
        return self._ticker.is_running

    def is_empty(self):
        return self.size() <= 0

    def is_full(self):
        return self._max_size > 0 and self.size() == self._max_size

    def remaining_jobs(self):
        return self._n_submitted - self._n_completed

    def wait_worker(self, poll=0.1, timeout=0):
        start = time.time()
        while self.is_busy() and (timeout==0 or time.time() - start < timeout):
            time.sleep(poll)

    def wait(self, n=0, poll=0.1, timeout=0):
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
                             target=self.job_wrap,
                             **job_args)
        j.start()

        job._started = time.time()
        job._inner_job = j
        self._working.update({job._id: job})

        log.debug("Started job '{}'".format(job._id))


    def put(self, function, args=None, kwargs=None, name=None, priority=100, timeout=0):
        # thread.Thread cannot be stopped; timeout not supported.
        assert(not (self._job_runner is thread.Thread and timeout > 0))

        if self._max_size > 0 and self.size()+1 > self._max_size:
            raise Exception('Max size exceeded.')

        job = Job(function=function, args=args, kwargs=kwargs, name=name, priority=priority, timeout=timeout)

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

    def get(self, poll=0, timeout=0):
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
        if n <= 0:
            n = self.size(completed=True)
        else:
            n = min(n, self.size(completed=True))

        return [self.get() for _ in range(n)]

    def set_workers(self, n):
        diff = n - self._n_workers
        self._n_workers = n
        log.debug('Added {} workers to the queue.'.format(diff))
