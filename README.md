# ppqueue

> Parallel Process Queue (ppqueue) for Python

> Formerly known as `ezpq`

| Links         |                        |
|---------------|------------------------|
| Code Repo     | https://www.github.com/fresh2dev/ppqueue           |
| Mirror Repo   | https://www.Fresh2.dev/code/r/ppqueue        |
| Documentation | https://www.Fresh2.dev/code/r/ppqueue/i           |
| Changelog     | https://www.Fresh2.dev/code/r/ppqueue/i/changelog |
| License       | https://www.Fresh2.dev/code/r/ppqueue/i/license   |
| Funding       | https://www.Fresh2.dev/funding        |

[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/fresh2dev/ppqueue?color=blue&style=for-the-badge)](https://www.github.com/fresh2dev/ppqueue/releases)
[![GitHub Release Date](https://img.shields.io/github/release-date/fresh2dev/ppqueue?color=blue&style=for-the-badge)](https://www.github.com/fresh2dev/ppqueue/releases)
[![License](https://img.shields.io/github/license/fresh2dev/ppqueue?color=blue&style=for-the-badge)](https://www.Fresh2.dev/code/r/ppqueue/i/license)
[![GitHub issues](https://img.shields.io/github/issues-raw/fresh2dev/ppqueue?color=blue&style=for-the-badge)](https://www.github.com/fresh2dev/ppqueue/issues)
[![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/fresh2dev/ppqueue?color=blue&style=for-the-badge)](https://www.github.com/fresh2dev/ppqueue/pulls)
[![GitHub Repo stars](https://img.shields.io/github/stars/fresh2dev/ppqueue?color=blue&style=for-the-badge)](https://star-history.com/#fresh2dev/ppqueue&Date)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/ppqueue?color=blue&style=for-the-badge)](https://pypi.org/project/ppqueue)
[![Docs Website](https://img.shields.io/website?down_message=unavailable&label=docs&style=for-the-badge&up_color=blue&up_message=available&url=https://www.Fresh2.dev/code/r/ppqueue/i)](https://www.Fresh2.dev/code/r/ppqueue/i)
[![Coverage Website](https://img.shields.io/website?down_message=unavailable&label=coverage&style=for-the-badge&up_color=blue&up_message=available&url=https://www.Fresh2.dev/code/r/ppqueue/i/tests/coverage)](https://www.Fresh2.dev/code/r/ppqueue/i/tests/coverage)
[![Funding](https://img.shields.io/badge/funding-%24%24%24-blue?style=for-the-badge)](https://www.Fresh2.dev/funding)


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

https://www.f2dv.com/code/r/ppqueue/i/page/examples/

And more examples are provided in the reference docs:

https://www.f2dv.com/code/r/ppqueue/i/reference/

---

*Brought to you by...*

<a href="https://www.fresh2.dev"><img src="https://img.fresh2.dev/fresh2dev.svg" style="filter: invert(50%);"></img></a>