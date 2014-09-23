#!/usr/bin/env python

import datetime, sys

import plotly.plotly as py
from plotly.graph_objs import Stream, Scatter, Data, Layout, 'figure

STREAM_IDS = {
    'Running': 'c7d72lai12',
    'Idle': 'v06nqwbyc8',
    'SystemHold': 'wit8d3v54h',
    'BatchHold': 'p1kt433uui',
    'UserHold': 'w351mpb9jk',
    'NotQueued': 'w351mpb9jk',
}

def count_job_types(jobs):
    counters = {}
    for state in STREAM_IDS:
        counters[state] = 0
    for job in jobs:
        if job.state in counters:
            counters[job.state] += 1
        else:
            msg = '### unknown job state: {0}\n'.format(job.state)
            sys.stderr.write(msg)
    return counters

def init_plot(counters, options):
    traces = []
    for state in counters:
        stream = Stream(token=STREAM_IDS[state], maxpoints=48)
        trace = Scatter(
            x=[datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
            y=[counters[state]],
            mode='lines+markers',
            stream=stream
        )
        traces.append(trace)
    data = Data(traces)
    layout = Layout(title='Jobs on {0}'.format(options.cluster))
    figure = Figure(data=data, layout=layout)
    url = py.plot(figure, filename='{0}_jobs'.format(options.cluster),
                  auto_open=False)
    return url

def update_plot(counters, options):
    for state in counters:
        stream = py.Stream(STREAM_IDS[state])
        stream.open()
        x = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        y = counters[state]
        stream.write(dict(x=x, y=y))
        stream.close()

if __name__ == '__main__':
    from argparse import ArgumentParser
    import subprocess

    arg_parser = ArgumentParser(description='create or update job stats')
    arg_parser.add_argument('--init', action='store_true',
                            help='initialize plot')
    arg_parser.add_arugment('--file', help='showq output file (debuggign)')
    arg_parser.add_argument('--showq', default='/usr/local/bin/showq',
                            help='showq command to use')
    options = arg_parser.parse_args()
    if options.file:
        with open(options.file, 'r') as showq_file:
            jobs = parser.parse_file(showq_file)
    else:
        try:
            job_output = subprocess.check_output([options.showq])
            jobs = showq_parser.parse(job_output)
        except subprocess.CalledProcessError:
            sys.stderr.write('### error: could not execute showq\n')
            sys.exit(1)
    counters = count_job_types(jobs)
    if options.init:
        url = init_plot(counters, options)
        print 'URL = {0}'.format(url)
    else:
        update_plot(counter, options)

