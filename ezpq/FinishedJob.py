from datetime import datetime
from typing import Sequence, Mapping, Any

from ezpq import utils
from ezpq.Job import Job


class FinishedJob(object):
    # TODO: change sig to: (job: Job, end_time: int, cancelled: bool, output: str, exception: str, exitcode: int, callback_output)
    def __init__(self, job: Job):
        self.qid: str = job.qid
        self.idx: int = job.idx
        self.name: str = job.name
        self.priority: int = job.priority
        self.lane: int = job.lane
        self.timeout: int = job.timeout
        self.function: str = job.fun.__name__
        self.args: Sequence[Any] = job.args
        self.kwargs: Mapping[str, Any] = job.kwargs
        self.submitted: datetime = job.get_submit_time()
        self.started: datetime = job.get_start_time()
        self.ended: datetime = job.get_end_time()
        self.processed: datetime = job.get_processed_time()
        self.exitcode: int = job.exitcode
        self.cancelled: bool = job._cancelled
        self.runtime: float = job.running_time()
        self.output: Any = job.output
        self.exception: str = job.exception_txt

        # TODO: use a dedicated property name `callback_output`
        self.callback: Any = job.callback

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

    def __eq__(self, job):
        return self.compare(job) == 0

    def __ne__(self, job):
        return self.compare(job) != 0

    def __lt__(self, job):
        return self.compare(job) < 0

    def __le__(self, job):
        return self.compare(job) <= 0

    def __gt__(self, job):
        return self.compare(job) > 0

    def __ge__(self, job):
        return self.compare(job) >= 0
