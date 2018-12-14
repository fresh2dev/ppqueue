#%%
%matplotlib inline
import ezpq
import random
import time
from threading import Thread

import pandas as pd

import logging as log
logger = log.getLogger()
logger.setLevel(log.DEBUG)

#%%

def timerfunc(func):
    """
    A timer decorator
    """
    def function_timer(*args, **kwargs):
        """
        A nested function for timing other functions
        """
        start = time.time()
        value = func(*args, **kwargs)
        end = time.time()
        runtime = end - start
        msg = "The runtime for {func} took {time} seconds to complete"
        print(msg.format(func=func.__name__,
                         time=runtime))
        return value
    return function_timer


class MyTimer():
    '''https://www.blog.pythonlibrary.org/2016/05/24/python-101-an-intro-to-benchmarking-your-code/'''
    def __init__(self):
        self.start = time.time()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end = time.time()
        runtime = round(end - self.start, 3)
        msg = 'Runtime: {time}'
        print(msg.format(time=runtime))
        


#%%

import random

def random_sleep(x, min=0, max=1):
    n = random.randint(min*1000, max*1000) / 1000
    time.sleep(n)
    return 'Job {} slept {} seconds.'.format(x, round(n, 2))

output = [random_sleep(x, 0, 2) for x in range(10)]

output

#%%

@ezpq.Queue(n_workers=5)
def random_sleep(x, min=0, max=1):
    n = random.randint(min*1000, max*1000) / 1000
    time.sleep(n)
    return 'Job {} slept {} seconds.'.format(x, round(n, 2))

output = random_sleep(items=range(10), min=0, max=2)

output

#%%

df = pd.DataFrame(output)

df.head()

#%%

plt = ezpq.Plot(output).build()
plt.save('./imgs/quickplot.png')
plt

#%%

def random_sleep(x, min=0, max=1):
    n = random.randint(min*1000, max*1000) / 1000
    time.sleep(n)
    return 'Job {} slept {} seconds.'.format(x, round(n, 2))

logger.setLevel(log.INFO)

def print_sizes(Q):
    msg = 'Total: {0}; Waiting: {1}; Working: {2}; Completed: {3}'.format(
        Q.size(),
        Q.size(waiting=True),
        Q.size(working=True),
        Q.size(completed=True)
    )
    print(msg)


with ezpq.Queue() as Q:
    for x in range(10):
        Q.put(random_sleep, args=[x, 0, 2])

    while Q.size(waiting=True, working=True) > 0:
        print_sizes(Q)
        Q.wait(n=1)

    print_sizes(Q)

#%%

with ezpq.Queue() as Q:
    for x in range(10):
        Q.put(random_sleep, args=[x, 0, 2])

    n_remaining = Q.wait(timeout=1)
    while n_remaining > 0:
        print('Timeout reached; {0} jobs remain.'.format(n_remaining))
        n_remaining = Q.wait(timeout=1)

    print('{0} jobs remain.'.format(n_remaining))


#%%

with ezpq.Queue() as Q:
    for x in range(10):
        Q.put(random_sleep, args=[x, 0, 2])

    while Q.size(waiting=True, working=True) > 0:
        print_sizes(Q)
        Q.wait(n=1)

    print_sizes(Q)

#%%

with ezpq.Queue() as Q:
    # enqueue jobs
    for x in range(10):
        Q.put(random_sleep, args=[x, 0, 2])

    # repeatedly `get()` jobs until Q.size() == 0.
    output = list()

    while Q.size() > 0:
        job = Q.get(timeout=0.5)
        if job is None:
            print('Timeout reached; no output available; {0} jobs remain.'.format(Q.size()))
        else:
            output.append(job)

    print('{0} jobs remain in queue.'.format(Q.size()))
    print('Output list size: {0}'.format(len(output)))


#%%

with ezpq.Queue(n_workers=5) as Q:
    for x in range(10):
        Q.put(random_sleep, args=[x, 0, 2])

    print('Queue size before: {0}'.format(Q.size()))

    Q.wait()
    output = Q.collect()

    print('Queue size after: {0}'.format(Q.size()))
    print('Output size: {0}'.format(len(output)))

#%%

logger.setLevel(log.DEBUG)

Q = ezpq.Queue()

for x in range(10):
    Q.put(random_sleep, args=[x, 0, 2])

Q.wait()

Q.dispose()

#%%

def pandas_df_to_markdown_table(df):
    from IPython.display import Markdown, display
    fmt = ['---' for i in range(len(df.columns))]
    df_fmt = pd.DataFrame([fmt], columns=df.columns)
    df_formatted = pd.concat([df_fmt, df])
    md_string = df_formatted.to_csv(sep="|", index=False)
    # display(Markdown(md_string))
    return md_string

df = pd.DataFrame(output)[['name', 'output', 'runtime']]

pandas_df_to_markdown_table(df.head())



#%%

plt = ezpq.Plot(output).build()
plt.save('./imgs/quickplot.png')
plt

#%%

with ezpq.Queue() as Q:
    # enqueue with priority
    for x in range(100):
        Q.put(random_sleep, args=[x, 0, 2],
            priority=int((x+1) % 5 != 0)) # 0 is higher priority than 1.

    # wait and collect
    Q.wait()
    output = Q.collect()

plt = ezpq.Plot(output, title='Lower priority takes precedence.', color_by='priority').build()

plt

#%%

n_inputs = 100


#%% simple

with EZPQ.Queue(n_workers=10, poll=.05) as Q: # create an EZPQ.Queue() with 10 workers.
    for i in range(n_inputs): # create 50 jobs, each sleeping for 1 second.
        Q.put( EZPQ.Job(function=time.sleep, args=1) )
    Q.wait_all() # wait for all jobs to complete.
    results = Q.collect() # collect completed jobs from queue.

plt = EZPQ.Plot.plot_jobs(results, title='100 inputs, 1s job time, 10 workers (parallel).')

plt.save('imgs/100x1x10.png')

#%% simple; 1 worker

with EZPQ.Queue(n_workers=1) as Q:

    for i in range(n_inputs):
        Q.put( EZPQ.Job(function=time.sleep, args=1) )

    Q.wait_all()

    results = Q.collect()

plt = EZPQ.Plot.plot_jobs(results, title='100 inputs, 1s job time, 1 worker (serial).')

plt.save('imgs/100x1x1.png')

#%% prioritized

with EZPQ.Queue(n_workers=10, auto_start=False) as Q:

    for i in range(n_inputs):
        job = EZPQ.Job(function=time.sleep, args=1,
                        priority=(i % 3)+1)
        Q.put(job)

    Q.start()

    Q.wait_all()

    results = Q.collect()

plt = EZPQ.Plot.plot_jobs(results, color_by='priority',
                    title='100 inputs, 1s job time, 10 workers (prioritized)')

plt.save('imgs/100x1x10_priority.png')

#%% simple w/ random job time

with EZPQ.Queue(n_workers=10) as Q:

    for i in range(n_inputs):
        sleep_sec = random.randint(1, 2000) / 1000
        job = EZPQ.Job(function=time.sleep, args=sleep_sec)
        Q.put(job)

    Q.wait_all()

    results = Q.collect()

plt = EZPQ.Plot.plot_jobs(results, title='100 inputs, 0-2s job time, 10 workers.')

plt.save('imgs/100xNx10.png')

#%% reverse order

# with EZPQ.Queue(n_workers=10) as Q:
#
#     for i in range(n_inputs):
#         sleep_sec = random.randint(1, 2000) / 1000
#         job = EZPQ.Job(function=time.sleep, args=sleep_sec,
#                        priority=-i) # either 0 or 1. 0 is higher priority.
#         Q.put(job)
#
#     Q.wait_all()
#
#     results = Q.collect()
#
# EZPQ.Plot.plot_jobs(results, color_by='exitcode')
#
# #%% reverse order; suppress auto_start
#
# with EZPQ.Queue(n_workers=10, auto_start=False) as Q:
#
#     for i in range(n_inputs):
#         sleep_sec = random.randint(1, 2000) / 1000
#         job = EZPQ.Job(function=time.sleep, args=sleep_sec,
#                        priority=-i) # either 0 or 1. 0 is higher priority.
#         Q.put(job)
#
#     Q.start()
#
#     Q.wait_all()
#
#     results = Q.collect()
#
# EZPQ.Plot.plot_jobs(results, color_by='exitcode')


#%% timeout

with EZPQ.Queue(n_workers=10) as Q:

    for i in range(n_inputs):
        sleep_sec = random.randint(0, 2000) / 1000 # sleep between 0s and 2s.

        job = EZPQ.Job(function=time.sleep, args=sleep_sec,
                       timeout=1.5) # 1.5s < 2s, timeouts will occur on roughly 25% of jobs.
        Q.put(job)

    Q.wait_all()

    results = Q.collect()

plt = EZPQ.Plot.plot_jobs(results, color_by='cancelled',
                            title='100 inputs, 0-2s job time, 10 workers (timeout=1.5s)')

plt.save('imgs/100xNx10_timeouts.png')

#%% thread

# 'Thread' loses the ability to use timeout, to kill jobs, or to obtain exit codes.

# with EZPQ.Queue(n_workers=10, job_runner=Thread) as Q:
#
#     for i in range(n_inputs):
#         sleep_sec = random.randint(1, 2000) / 1000
#
#         job = EZPQ.Job(function=time.sleep, args=sleep_sec,
#                        timeout=0)
#         Q.put(job)
#
#     Q.wait_all()
#
#     results = Q.collect()
#
# EZPQ.Plot.plot_jobs(results, color_by='priority')

#%% Count files example

import os, fnmatch
def count_files(path, pattern):
    '''https://stackoverflow.com/a/1724723'''
    n = 0
    for root, dirs, files in os.walk(path, followlinks=True):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                n += 1
    return n

root_dir = os.environ['HOME']

include_dirs = [os.path.join(root_dir, x)
                for x in ['Desktop', 'Documents', 'Downloads', 'Music', 'Pictures']]

#%%% serial

# start = time.time()
#
# ###
#
# n_files = 0
#
# for x in include_dirs:
#     n_files += count_files(path=x, pattern='*')
#
# log.info(n_files)
#
# ###
#
# end = time.time()
#
# log.info(end - start)

#%%% EZPQ serial

with EZPQ.Queue(n_workers=1) as Q:

    for path in include_dirs:
        Q.put( EZPQ.Job(name=path, function=count_files,
                       kwargs={'path': path, 'pattern': '*'}) )

    Q.wait_all()

    results = Q.collect()

n_files = sum([x['output'] for x in results])

plt = EZPQ.Plot.plot_jobs(results, color_by='name', bar_width=5)

plt.save('imgs/enum_dirs_sync.png')

#%%% parallel

with EZPQ.Queue(n_workers = 5) as Q:

    for path in include_dirs:
        Q.put( EZPQ.Job(name=path, function=count_files,
                       kwargs={'path': path, 'pattern': '*'}) )

    Q.wait_all()

    results = Q.collect()

n_files = sum([x['output'] for x in results])

total_runtime = sum([(x['processed'] - x['started']).total_seconds()
                    for x in results])

plt = EZPQ.Plot.plot_jobs(results, color_by='name', bar_width=5)

plt.save('imgs/enum_dirs_async.png')


#%%% parallel w/ logging

with EZPQ.Queue(callback=EZPQ.Queue.log_csv) as Q:

    for path in include_dirs:
        Q.put( EZPQ.Job(name=path, function=count_files,
                       kwargs={'path': path, 'pattern': '*'}) )

    Q.wait_all()

    results = Q.collect()

EZPQ.Plot.plot_jobs(results, color_by='name', width=5)

#%% sim_job

logger.setLevel(log.INFO)

def sim_job(min_time=0, max_time=10000):
    job_time = random.randint(min_time, max_time) / 1000
    time.sleep(job_time)

#%% server (overwhelmed)

stop_at = 50

with EZPQ.Queue(n_workers = 3) as Q:

    while Q.n_submitted() < stop_at:
        time.sleep( random.randint(0, 2000) / 1000 ) # wait for job to "arrive"

        priority = random.randint(1, 3) # assign random priority (less is more)

        Q.put( EZPQ.Job(function=sim_job, kwargs={'max_time': 11000},
                        timeout=10, priority=priority) ) # enqueue job

    Q.wait_all()
    results = Q.collect()

EZPQ.Plot.plot_jobs(results, color_by='exitcode')

EZPQ.Plot.plot_jobs(results, color_by='priority')

#%% server w/ callback

with EZPQ.Queue(n_workers = 10, callback=EZPQ.Queue.log_csv, auto_remove=True) as Q:
    while True:
        time.sleep( random.randint(0, 2000) / 1000 ) # wait for job to "arrive"

        Q.put( EZPQ.Job(function=sim_job, kwargs={'max_time':6000}, timeout=5) ) # enqueue job

#%%





#%%

# def measure_queue_perf(queue_mgr, reps, **kwargs):
#     times = [None] * reps

#     for r in range(reps):
#         start = time.time()

#         output = queue_mgr(**kwargs)

#         times[r] = {
#             'Runtime': time.time() - start,
#             'Output': output,
#             'Timestamp': datetime.now()
#         }

#     return(times)

#     # return({'Repetitions':len(times),
#     #         'Maximum':times.max(),
#     #         'Minimum':times.min(),
#     #         'Average':times.mean(),
#     #         'SD':times.std(),
#     #         'Output':output})


# #%%

# def serial_process(script, items, **kwargs):

#     results = [None] * len(items)

#     for i,x in enumerate(items):
#         results[i] = script(x, **kwargs)

#     return(results)


# def parallel_process(script, items, n_workers, rest_ms=0, **kwargs):
#     job_list = list()
#     output = mp.Manager().list() # must sync across processes
#     output.extend([None] * len(items))

#     for i,x in enumerate(items):
#         p = mp.Process(name=i, target=script, kwargs={'seed': x, 'output': output, **kwargs})
#         p.start()
#         job_list.append(p)
#         while len(job_list) >= n_workers:
#             if rest_ms > 0:
#                 time.sleep(rest_ms / 1000)
#             receive_jobs(job_list, output, auto_remove=True)

#     while len(job_list) > 0:
#         if rest_ms > 0:
#             time.sleep(rest_ms / 1000)
#         if len(job_list) == 1:
#             job_list[0].join()
#         receive_jobs(job_list, output, auto_remove=True)

#     # if sleep_ms is 0, the job could exit too fast to capture output;
#     # replace all empty output with 0 to help future calcs.
#     for i in range(len(output)):
#         if output[i] is None:
#             output[i] = 0

#     return(list(output))


# def parallel_thread(script, items, n_workers, rest_ms=0, **kwargs):

#     job_list = list()
#     output = [None] * len(items)

#     for i,x in enumerate(items):
#         p = thread.Thread(name=i, target=script, kwargs={'seed': x, 'output': output, **kwargs})
#         p.start()
#         job_list.append(p)
#         while len(job_list) >= n_workers:
#             if rest_ms > 0:
#                 time.sleep(rest_ms / 1000)
#             receive_jobs(job_list, output, auto_remove=True)

#     while len(job_list) > 0:
#         if rest_ms > 0:
#             time.sleep(rest_ms / 1000)

#         if len(job_list) == 1:
#             job_list[0].join()

#         receive_jobs(job_list, output, auto_remove=True)

#     return(output)

# #%%

# import sys

# #%%

# ver = sys.version[0:3]
# pwd = sys.path[0]
# pc_name = socket.gethostname().lower().replace('.', '_')
# csv_path = '{0}/runtimes_{1}.csv'.format(pwd, pc_name)

# n_items = None
# n_workers = None
# job_time_ms = None
# n_reps = 1

# if len(sys.argv) >= 4: # test data
#     n_items = [int(sys.argv[1])]
#     n_workers = [int(sys.argv[2])]
#     job_time_ms = [1000*float(sys.argv[3])]
#     if len(sys.argv) == 5:
#         out_csv = bool(int(sys.argv[4]))
#         if not out_csv:
#             csv_path = None
# else: # train data
#     n_items = [30, 45, 60, 75, 100, 500, 1000]
#     n_workers = [3, 7, 11, 15, 30]
#     job_time_ms = [x*1000 for x in [1, 2, 3, 4, 5]]
#     n_reps = 1

# rest_ms_ints = [0]

# benchmarks = [
#     # {'fun':serial_process, 'reps':3},
#     # {'queue_mgr':parallel_process, 'reps':n_reps}, #, 'n_workers':n_workers}, #, 'rest_ms':sleep_ms/10},
#     {'queue_mgr':parallel_thread, 'reps':n_reps} #, 'n_workers':n_workers} #, 'rest_ms':sleep_ms/10}
# ]

# # all_times = [None] * (len(benchmarks) * len(job_time_ms) * n_reps)
# # i = 0

# #%%

# for test in benchmarks:
#     for n in n_items:
#         test['items'] = list(range(n))
#         for n_work in n_workers:
#             test['n_workers'] = min([n, n_work])

#             for rest_ms in rest_ms_ints:
#                 test['rest_ms'] = rest_ms

#                 test['min_sleep'] = 0
#                 test['max_sleep'] = 0
#                 overhead = sum([x['Runtime']
#                                 for x in measure_queue_perf(script=script, **test)]) / test['reps'] / len(test['items'])

#                 for sleep_ms in job_time_ms:
#                     test['min_sleep'] = sleep_ms
#                     test['max_sleep'] = sleep_ms

#                     times = measure_queue_perf(script=script, **test)

#                     for t in times:

#                         output = t.pop('Output')

#                         t.update({
#                             'Method':'Python {0}: {1}'.format(ver, test['queue_mgr'].__name__),
#                             #'Repetitions':test['reps'],
#                             'Input Size':len(test['items']),
#                             'Queue Size':test['n_workers'],
#                             'Overhead': overhead,
#                             'AvgJobTime':sum(output) / len(output) / 1000,
#                             'MinJobTime':min(output) / 1000,
#                             'MaxJobTime':max(output) / 1000,
#                             'SumJobTime':sum(output) / 1000
#                         })

#                     if csv_path is None:
#                         print(times)
#                     else:
#                         write_csv(path=csv_path, data=times, append=True,
#                                 header=['Timestamp', 'Method', 'Runtime', 'Input Size', 'Queue Size',
#                                         'Overhead', 'AvgJobTime', 'MinJobTime', 'MaxJobTime', 'SumJobTime'])

#                     # time.sleep(0.1)


# #%%

# # from plotnine import *

# # ggplot(aes(x='AvgResult', y='Average', color='Method'), data=df) + \
# #       geom_line() + \
# #       labs(x='job delay (ms)', y='Runtime (seconds)', title='')

# # #%%

# # ggplot(aes(x='sleep_ms', y='Average', color='method'), data=df[df['sleep_ms'] < 50]) + \
# #   geom_line()

# # #%%
# # ggplot(aes(x='sleep_ms', y='Average', color='method'),
# #            data=df[(df['method'] != 'Python: serial') & (df['sleep_ms'] < 500)]) + \
# #       geom_line()
