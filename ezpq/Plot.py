import logging as log

class Plot():
    """Functions to produce Gantt chart from completed job schedules."""

    def __init__(self, jobs):
        """Some description.

        Args:
            jobs:
                - Accepts: list of dictionary objects, as returned by ezpq.Queue.collect()
        Returns:
            self
        """

        import pandas

        df = pandas.DataFrame(jobs)
        df.lane.fillna(value='', inplace=True)
        min_time = df['submitted'].min()
        df['submitted_offset'] = df['submitted'] - min_time
        df['started_offset'] = df['started'] - min_time
        df['ended_offset'] = df['ended'] - min_time
        df['processed_offset'] = df['processed'] - min_time
        self.jobs_df = df

    @staticmethod
    def _plot_theme(grid_axis='both', grid_lines='both', theme='bw'):
        """Internal function provides consistent theme across plots.
        Currently a slightly modified version of theme_bw() with configurable grid lines.

        Args:
            grid_axis: controls the axis on which to draw grid lines
                - Accepts: None, 'x', 'y', 'both'
                - Default: 'both'
            grid_lines: controls whether major or minor grid lines are drawn
                - Accepts: None, 'major', 'minor', 'both'
                - Default: 'both'
            theme:
                - Accepts: 'bw', 'classic', 'gray', 'grey', 'seaborn', '538', 'dark', 'matplotlib', 'minimal', 'xkcd', 'light'
                - Default: 'bw'
        Returns:
            A theme object to be added to a plotnine.ggplot() object.
        """

        import plotnine as gg

        assert(grid_axis in [None, 'x', 'y', 'both'])
        assert(grid_lines in [None, 'major', 'minor', 'both'])
        assert(theme in ['bw', 'classic', 'gray', 'grey', 'seaborn', '538', 'dark', 'matplotlib', 'minimal', 'xkcd', 'light'])

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

        return getattr(gg, 'theme_'+theme)() + \
                gg.theme(panel_border = gg.element_blank(),
                          axis_line = gg.element_line(color = "black"),
                          **grid_opt)

    def build(self, color_by='qid', facet_by='qid', facet_scale='fixed', show_legend=True, bar_width=1, title=None, color_pal=None, theme='bw'):
        """Produces a plot based on the data and options provided to a `ezpq.Plot()` object.

        Args:
            color_by: controls the column to use for coloring the bars.
                - Accepts: one of 'qid', 'priority', 'lane', 'cancelled', 'exitcode', 'name', 'output'
                - Default: 'qid'
            facet_by: controls the column to use for facetting the plot.
                - Accepts: one of 'qid', 'priority', 'lane', 'cancelled', 'exitcode', 'name', 'output'
                - Default: 'qid'
            facet_scale: controls the scale of the x/y axis across facets.
                - Accepts: one of 'fixed', 'free', 'free_x', 'free_y'
                - Default: 'fixed'
            show_legend: controls whether the legend is drawn.
                - Accepts: bool
                - Default: True
            bar_width: controls the bar width
                - Accepts: float
                - Default: 1
            title: optional title to be drawn above the plot.
                - Accepts: str, None
                - Default: None
            theme:
                - Accepts: 'bw', 'classic', 'gray', 'grey', 'seaborn', '538', 'dark', 'matplotlib', 'minimal', 'xkcd', 'light'
                - Default: 'bw'
        Returns:
            The plot produced from plotnine.ggplot().
        """

        assert(color_by in ['qid', 'priority', 'lane', 'cancelled', 'exitcode', 'name', 'output'])
        assert(facet_by in ['qid', 'priority', 'lane', 'cancelled', 'exitcode', 'name', 'output'])
        assert(facet_scale in ['fixed', 'free', 'free_x', 'free_y'])

        import plotnine as gg

        df2 = self.jobs_df.loc[:, set(['qid', 'id', color_by, facet_by, 'submitted_offset', 'started_offset', 'ended_offset', 'processed_offset'])].melt(id_vars=set(['qid', 'id', color_by, facet_by]))

        df_submit_start = df2[(df2['variable'] == 'submitted_offset') | (df2['variable'] == 'started_offset')]
        df_start_end = df2[(df2['variable'] == 'started_offset') | (df2['variable'] == 'ended_offset')]
        df_end_processed = df2[(df2['variable'] == 'ended_offset') | (df2['variable'] == 'processed_offset')]

        labs = { 'x': 'duration', 'y': 'job id' }
        if title is not None:
            labs['title'] = title

        gg_obj = gg.ggplot(gg.aes(x='value', y='id', group='factor(id)')) + \
                    gg.geom_line(df_submit_start, color='gray', size=bar_width, alpha=0.25) + \
                    gg.geom_line(df_start_end,
                                gg.aes(color='factor({})'.format(color_by)),
                                size=bar_width, show_legend=bool(show_legend)) + \
                    gg.geom_line(df_end_processed, color='gray', size=bar_width, alpha=0.25) + \
                    gg.labs(**labs) + \
                    gg.labs(color=color_by) + \
                    Plot._plot_theme(grid_axis='x', theme=theme) + \
                    gg.facet_grid(facets='{}~'.format(facet_by), labeller='label_both', scales=facet_scale, as_table=True)
        
        if color_pal is None or not isinstance(color_pal, list):
            gg_obj += gg.scale_color_hue(h=.65)
        else:
            n_colors = self.jobs_df[color_by].unique().size

            if len(color_pal) < n_colors:
                log.warning('Insufficient number of colors; need at least ' + str(n_colors))
                gg_obj += gg.scale_color_hue(h=.65)
            else:
                gg_obj += gg.scale_color_manual(color_pal[:n_colors])

        return gg_obj
