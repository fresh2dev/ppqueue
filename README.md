# ppqueue

> A Parallel Process Queue for Python.

| Links         |                                          |
|---------------|------------------------------------------|
| Code Repo     | https://www.github.com/fresh2dev/ppqueue |
| Documentation | https://www.f2dv.com/r/ppqueue           |
| Changelog     | https://www.f2dv.com/r/ppqueue/changelog |
| License       | https://www.f2dv.com/r/ppqueue/license   |
| Funding       | https://www.f2dv.com/fund                |

[![GitHub Repo stars](https://img.shields.io/github/stars/fresh2dev/ppqueue?color=blue&style=for-the-badge)](https://star-history.com/#fresh2dev/ppqueue&Date)
[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/fresh2dev/ppqueue?color=blue&style=for-the-badge)](https://www.f2dv.com/r/ppqueue/changelog)
[![GitHub Release Date](https://img.shields.io/github/release-date/fresh2dev/ppqueue?color=blue&style=for-the-badge)](https://www.f2dv.com/r/ppqueue/changelog)
[![License](https://img.shields.io/github/license/fresh2dev/ppqueue?color=blue&style=for-the-badge)](https://www.f2dv.com/r/ppqueue/license)
<!-- [![GitHub issues](https://img.shields.io/github/issues-raw/fresh2dev/ppqueue?color=blue&style=for-the-badge)](https://www.github.com/fresh2dev/ppqueue/issues) -->
<!-- [![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/fresh2dev/ppqueue?color=blue&style=for-the-badge)](https://www.github.com/fresh2dev/ppqueue/pulls) -->
<!-- [![PyPI - Downloads](https://img.shields.io/pypi/dm/ppqueue?color=blue&style=for-the-badge)](https://pypi.org/project/ppqueue) -->
<!-- [![Docker Pulls](https://img.shields.io/docker/pulls/fresh2dev/ppqueue?color=blue&style=for-the-badge)](https://hub.docker.com/r/fresh2dev/ppqueue) -->
<!-- [![Funding](https://img.shields.io/badge/funding-%24%24%24-blue?style=for-the-badge)](https://www.f2dv.com/fund) -->

---

## Overview

`ppqueue` is a Python module that serves as an abstraction layer to both `multiprocessing.Process` and `threading.Thread`. I built `ppqueue` because I too often notice that parallelizing code results in *ugly* code. With this simple Queue, you can parallelize code easily and attractively. ppqueue offers:

- a single API for parallel execution using processes or threads.
- FIFO priority queueing.
- Gantt charts of job execution (thanks `plotnine` + `pandas`)
- progress bars (thanks to `tqdm`)

![](https://img.fresh2.dev/1687407526_84b23a13b5f.svg)

## Install

Install from PyPi:

```python
pip install ppqueue[plot]
```

## Examples

An notebook of examples is available at:

https://www.f2dv.com/r/ppqueue/page/examples/

And more examples are provided in the reference docs:

https://www.f2dv.com/r/ppqueue/reference/

## Support

If this project delivers value to you, please [provide feedback](https://www.github.com/fresh2dev/ppqueue/issues), code contributions, and/or [funding](https://www.f2dv.com/fund).

---

*Brought to you by...*

<a href="https://www.f2dv.com"><img src="https://img.fresh2.dev/fresh2dev.svg" style="filter: invert(50%);"></img></a>
