# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

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
