#!/usr/bin/env python
'''script that sends the number of jobs by state to Plotly'''

import datetime, sys

import plotly.plotly as py
from plotly.graph_objs import Stream, Scatter, Data, Layout, Figure

# list of states to ensure right order in graph
STATE_NAMES = [
    'Running',
    'Idle',
    'SystemHold',
    'Deferred',
    'BatchHold',
    'UserHold',
    'Hold',
    'NotQueued',
]

# stream tokens so that each state goes to its own stream
STREAM_IDS = {
    'Running': 'c7d72lai12',
    'Idle': 'v06nqwbyc8',
    'SystemHold': 'wit8d3v54h',
    'Deferred': 'p1kt433uui',
    'BatchHold': 'vjb6ixgg0p',
    'UserHold': 'w351mpb9jk',
    'Hold': '9txpv9ickx',
    'NotQueued': 'bqn4umpi72',
}

def count_job_types(jobs, regex):
    '''create a map from job state to number of jobs based on a list
       of jbos'''
    counters = {}
    for state in STREAM_IDS:
        counters[state] = 0
    for job_list in jobs.values():
        for job in job_list:
            if regex.match(job.id):
                if job.state in counters:
                    counters[job.state] += 1
                else:
                    msg = '### unknown job state: {0}\n'.format(job.state)
                    sys.stderr.write(msg)
    return counters

def init_plot(counters, options):
    '''initialize Plotly plot based on counters, should be executed only,
       once, unless plot layout is modified, or states are added'''
    time_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    traces = []
    for state in STATE_NAMES:
# if state name list and stream IDs are inconsitent, try to plot as much
# data as possible
        if state not in STREAM_IDS:
            continue
        stream = Stream(token=STREAM_IDS[state], maxpoints=48)
        trace = Scatter(
            x=[time_stamp],
            y=[counters[state]],
            mode='lines+markers',
            name=state,
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
    '''update the plot with current counter values'''
    time_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for state in STATE_NAMES:
# if state name list and stream IDs are inconsitent, try to plot as much
# data as possible
        if state not in STREAM_IDS:
            continue
        stream = py.Stream(STREAM_IDS[state])
        stream.open()
        x = time_stamp
        y = counters[state]
        stream.write(dict(x=x, y=y))
        stream.close()

if __name__ == '__main__':
    from argparse import ArgumentParser
    import re, subprocess
    from vsc.moab.showq import ShowqParser

    arg_parser = ArgumentParser(description='create or update job stats')
    arg_parser.add_argument('--init', action='store_true',
                            help='initialize plot')
    arg_parser.add_argument('--cluster', default='thinking',
                            help='name of cluster')
    arg_parser.add_argument('--pattern', default=r'2\d+',
                            help='pattern used to filter job IDs')
    arg_parser.add_argument('--file', help='showq output file (debuggign)')
    arg_parser.add_argument('--showq', default='/opt/moab/bin/showq',
                            help='showq command to use')
    options = arg_parser.parse_args()
# check consistency of state name list and stream IDs, if inconsisten
# try to continue nevertheless
    if set(STATE_NAMES) != set(STREAM_IDS.keys()):
        msg = '### error: state names inconsistent with stream IDs\n'
        sys.stderr.write(msg)
    showq_parser = ShowqParser()
    if options.file:
# for development and debugging
        with open(options.file, 'r') as showq_file:
            jobs = showq_parser.parse_file(showq_file)
    else:
        try:
            job_output = subprocess.check_output([options.showq])
            jobs = showq_parser.parse(job_output)
        except subprocess.CalledProcessError:
            sys.stderr.write('### error: could not execute showq\n')
            sys.exit(1)
    counters = count_job_types(jobs, re.compile(options.pattern))
    if options.init:
        url = init_plot(counters, options)
        print 'URL = {0}'.format(url)
    else:
        update_plot(counters, options)

