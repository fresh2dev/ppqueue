from __future__ import annotations

import logging as log
from enum import Enum, auto

import pandas as pd
import plotnine as gg

from .job import Job
from .utils import dedupe_list

__all__ = [
    "plot_jobs",
    "PlotColorBy",
    "PlotFacetBy",
    "PlotFacetScale",
    "PlotGridLines",
    "PlotTheme",
]


class PlotColorBy(Enum):
    """Enum used to define color groups using this job property."""

    QID = auto()
    PRIORITY = auto()
    GROUP = auto()
    CANCELLED = auto()
    EXITCODE = auto()
    NAME = auto()
    RESULT = auto()
    CALLBACK_RESULT = auto()


class PlotFacetBy(Enum):
    """Enum used to facet the plot by this job property."""

    QID = auto()
    PRIORITY = auto()
    GROUP = auto()
    CANCELLED = auto()
    EXITCODE = auto()
    NAME = auto()
    RESULT = auto()
    CALLBACK_RESULT = auto()


class PlotTheme(Enum):
    """Enum used to expose supported plot themes."""

    BW = auto()
    CLASSIC = auto()
    GRAY = auto()
    GREY = auto()
    SEABORN = auto()
    DARK = auto()
    MATPLOTLIB = auto()
    MINIMAL = auto()
    XKCD = auto()
    LIGHT = auto()


class PlotFacetScale(Enum):
    FIXED = auto()
    FREE = auto()
    FREE_X = auto()
    FREE_Y = auto()


class PlotGridLines(Enum):
    NONE = auto()
    MAJOR = auto()
    MINOR = auto()
    BOTH = auto()


class Plot:
    def __init__(self, *args: list[Job]):
        df = pd.DataFrame([j.__dict__ for jobs in args for j in jobs])
        df.group.fillna(value="", inplace=True)
        min_time = df["submit_timestamp"].min()
        df["submitted_offset"] = df["submit_timestamp"] - min_time
        df["started_offset"] = df["start_timestamp"] - min_time
        df["ended_offset"] = df["finish_timestamp"] - min_time
        df["processed_offset"] = df["process_timestamp"] - min_time
        self.jobs_df = df

    @staticmethod
    def _plot_theme(
        theme: PlotTheme,
        grid_lines: PlotGridLines,
        grid_axis: str | None,
    ) -> gg.theme:
        # Used to provide a consistent theme across plots.

        drop_grid = set()

        if grid_axis is None or grid_lines == PlotGridLines.NONE:
            drop_grid.update(["panel_grid_major", "panel_grid_minor"])
        elif grid_axis == "x":
            drop_grid.update(["panel_grid_major_y", "panel_grid_minor_y"])
            if grid_lines == PlotGridLines.MAJOR:
                drop_grid.add("panel_grid_minor_y")
            elif grid_lines == PlotGridLines.MINOR:
                drop_grid.add("panel_grid_major_y")
        elif grid_axis == "y":
            drop_grid.update(["panel_grid_major_x", "panel_grid_minor_x"])
            if grid_lines == PlotGridLines.MAJOR:
                drop_grid.add("panel_grid_minor_x")
            elif grid_lines == PlotGridLines.MINOR:
                drop_grid.add("panel_grid_major_x")

        grid_opt = {}
        for x in drop_grid:
            grid_opt[x] = gg.element_blank()

        return getattr(gg, "theme_" + theme.name.lower())() + gg.theme(
            panel_border=gg.element_blank(),
            axis_line=gg.element_line(color="black"),
            **grid_opt,
        )

    def build(
        self,
        title: str | None,
        color_by: PlotColorBy,
        facet_by: PlotFacetBy,
        facet_scale: PlotFacetScale,
        theme: PlotTheme,
        no_legend: bool,
        bar_width: int,
        color_pal: list[str] | None,
    ) -> gg.ggplot:
        df2 = self.jobs_df.loc[
            :,
            dedupe_list(
                [
                    "qid",
                    "idx",
                    color_by.name.lower(),
                    facet_by.name.lower(),
                    "submitted_offset",
                    "started_offset",
                    "ended_offset",
                    "processed_offset",
                ],
            ),
        ].melt(
            id_vars=dedupe_list(
                ["qid", "idx", color_by.name.lower(), facet_by.name.lower()],
            ),
        )

        df2 = df2[df2["value"].notnull()]

        df_submit_start = df2[
            (df2["variable"] == "submitted_offset")
            | (df2["variable"] == "started_offset")
        ]
        df_start_end = df2[
            (df2["variable"] == "started_offset") | (df2["variable"] == "ended_offset")
        ]
        df_end_processed = df2[
            (df2["variable"] == "ended_offset")
            | (df2["variable"] == "processed_offset")
        ]

        labs = {"x": "duration", "y": "job index"}
        if title is not None:
            labs["title"] = title

        gg_obj = (
            gg.ggplot(gg.aes(x="value", y="idx", group="factor(idx)"))
            + gg.geom_line(df_submit_start, color="gray", size=bar_width, alpha=0.2)
            + gg.geom_line(
                df_start_end,
                gg.aes(color=f"factor({color_by.name.lower()})"),
                size=bar_width,
                show_legend=not bool(no_legend),
            )
            + gg.geom_line(df_end_processed, color="gray", size=bar_width, alpha=0.2)
            + gg.labs(**labs)
            + gg.labs(color=color_by.name.lower())
            + Plot._plot_theme(
                theme=theme,
                grid_lines=PlotGridLines.BOTH,
                grid_axis="x",
            )
            + gg.facet_grid(
                facets=facet_by.name.lower() + "~",
                labeller="label_both",
                scales=facet_scale.name.lower(),
                as_table=True,
            )
        )

        if not color_pal:
            gg_obj += gg.scale_color_hue(h=0.65)
        else:
            n_colors = self.jobs_df[color_by.name.lower()].unique().size

            if len(color_pal) < n_colors:
                log.warning("Insufficient number of colors; need at least %d", n_colors)
                gg_obj += gg.scale_color_hue(h=0.65)
            else:
                gg_obj += gg.scale_color_manual(color_pal[:n_colors])

        return gg_obj


def plot_jobs(
    *args: list[Job],
    title: str | None = None,
    color_by: PlotColorBy = PlotColorBy.QID,
    facet_by: PlotFacetBy = PlotFacetBy.QID,
    facet_scale: PlotFacetScale = PlotFacetScale.FIXED,
    theme: PlotTheme = PlotTheme.BW,
    no_legend: bool = False,
    bar_width: int = 1,
    color_pal: list[str] | None = None,
) -> gg.ggplot:
    """

    Args:
        title:
        color_by:
        facet_by:
        facet_scale:
        theme:
        no_legend:
        bar_width:
        color_pal: a sequence of colors used to color each group of `color_by`.

    Examples:
        >>> from ppqueue import Queue
        >>> from ppqueue.plot import plot_jobs
        >>> from time import sleep
        ...
        >>> with Queue() as queue:
        ...     q1_jobs = queue.map(sleep, [1, 2, 3, 4, 5])
        ...
        >>> with Queue() as queue:
        ...     q2_jobs = queue.map(sleep, [1, 2, 3, 4, 5])
        ...
        >>> plot = plot_jobs(
        ...     q1_jobs, q2_jobs,
        ...     title='Demo',
        ...     color_by=PlotColorBy.QID,
        ...     facet_by=PlotFacetBy.QID,
        ...     theme=PlotTheme.DARK,
        ...     bar_width=2,
        ...     color_pal=['red', 'blue']
        ... )
        ...
        >>> print(plot)  # doctest: +SKIP
    """

    return Plot(*args).build(
        color_by=color_by,
        facet_by=facet_by,
        facet_scale=facet_scale,
        no_legend=no_legend,
        bar_width=bar_width,
        title=title,
        color_pal=color_pal,
        theme=theme,
    )
