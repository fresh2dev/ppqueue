# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## v0.2.1

### Added

- `clear_waiting()` function to clear the waiting queue.
- Added `stop_on_lane_error` parameter to `ezpq.Job` to allow for short-circuiting a synchronous lane if a job in the lane fails. When set to `True` and the preceding job has a non-zero exit code, this job will not be run. Note that this is to be set per-job for flexibility.
- Additional unit tests.

### Changed

- `stop_all()` function now clears the waiting queue and terminate running jobs. This addresses a bug where a queue would fail to close when disposing with jobs still in the waiting queue.
- The default `poll` for the queue itself is still `0.1`. Now, the default `poll` for `get` and `wait` is equal to the `poll` for the queue itself, as it makes no sense to check for changes more freqeuntly than changes could arise.

### Removed

- Removed functions `has_waiting`, `has_work`, and `has_completed`. Use `size(...)` for this.
- Renamed `Queue.is_started` to `Queue.is_running`.

## v0.2.0

### Added

- Added `map()` function to `ezpq.Queue`.
- Integration with tqdm package for progress bars.
- Added `lane` property to `ezpq.Job`. When set, jobs in the same lane are processing sequentially.
- Added `exception` property to `ezpq.Job`. Stores the exception of a failed job.
- Added `suppress_errors` parameter to `ezpq.Job`.
- Added `facet_by`, `theme`, and `color_pal` parameters to `ezpq.Plot.build()`.
- Added `qid` (queue id) to job data output. This can be used to facet an `ezpq.Plot`.
- Unit tests.

### Changed

- Removed parameter `n` from the `wait()` function of `ezpq.Queue`; `wait()` will halt execution until all jobs in the queue are processed.
- Moved `ezpq.Plot()` plotting parameters to `ezpq.Plot.build()`.
- Reordered `ezpq.Queue()` init parameters.

## v0.1.0

- Initial release
