#!/usr/bin/env python

from vsc.plotly_utils import create_annotations

import plotly.plotly as py
from plotly.graph_objs import Bar, Data, Layout, Figure

def plot_queues(running_jobs, running_nodes, queued_jobs, queued_nodes,
                queues, options):
    rj_bar = Bar(
        x=queues,
        y=[running_jobs[queue] for queue in queues],
        name='running jobs'
    )
    rn_bar = Bar(
        x=queues,
        y=[running_nodes[queue] for queue in queues],
        name='running nodes'
    )
    qj_bar = Bar(
        x=queues,
        y=[queued_jobs[queue] for queue in queues],
        name='queued jobs'
    )
    qn_bar = Bar(
        x=queues,
        y=[queued_nodes[queue] for queue in queues],
        name='queued nodes'
    )
    data = Data([rj_bar, rn_bar, qj_bar, qn_bar])
    layout = Layout(
        barmode='group',
        title='{0} queues'.format(options.partition),
        annotations=create_annotations()
    )
    figure = Figure(data=data, layout=layout)
    url = py.plot(figure, filename='{0}_queues'.format(options.partition),
                  auto_open=False)
    return url

def compute_queues(options):
    queues = []
    limits = []
    for qdef in options.queues.split(','):
        qname, limit = qdef.split(':')
        queues.append(qname)
        limit_secs = 3600*int(limit)
        limits.append(limit_secs)
    return queues, limits

def count_jobs(jobs, queues, limits):
    running_nodes = {}
    running_jobs = {}
    queued_nr_nodes = {}
    queued_jobs = {}
    for queue in queues:
        queued_jobs[queue] = 0
        running_nodes[queue] = set()
        running_jobs[queue] = 0
        queued_nr_nodes[queue] = 0
    for job in jobs:
        if job.state == 'R':
            walltime = job.resource_specs['walltime']
            for idx, limit in enumerate(limits):
                if walltime <= limit:
                    for node in job.exec_host.keys():
                        running_nodes[queues[idx]].add(node)
                    running_jobs[queues[idx]] += 1
                    break
        elif job.state == 'Q':
            walltime = job.resource_specs['walltime']
            for idx, limit in enumerate(limits):
                if walltime <= limit:
                    nodect = job.resource_specs['nodect']
                    queued_nr_nodes[queues[idx]] += nodect
                    queued_jobs[queues[idx]] += 1
                    break
    running_nr_nodes = {}
    for queue in queues:
        running_nr_nodes[queue] = len(running_nodes[queue])
    return running_jobs, running_nr_nodes, queued_jobs, queued_nr_nodes

if __name__ == '__main__':
    from argparse import ArgumentParser
    import subprocess
    from vsc.utils import seconds2walltime
    from vsc.pbs.qstat import QstatParser

    arg_parser = ArgumentParser(description='parse qstat output to'
                                            ' compute queue'
                                            ' distributions')
    arg_parser.add_argument('--partition', default='thinking',
                            help='partition to display informatino for')
    arg_parser.add_argument('--qstat', default='/usr/local/bin/qstat',
                            help='qstat command to use')
    arg_parser.add_argument('--qstat_options', default='-f',
                            help='qstat command options to use')
    arg_parser.add_argument('--verbose', action='store_true',
                            help='print some feedback information')
    arg_parser.add_argument('--queues',
                            default='q1h:1,q24h:24,q72h:72,q7d:168'
                                    ',q21d:504',
                            help='queue categories, comma separated'
                                 ' list of name:time (h)')
    arg_parser.add_argument('--file', help='file to use as input for'
                                           ' debugging')
    options = arg_parser.parse_args()
    if options.file:
        with open(options.file, 'r') as qstat_file:
            cmd_output = ''.join([line for line in qstat_file])
    else:
        cmd_output = subprocess.check_output([options.qstat,
                                              options.qstat_options])
    queues, limits = compute_queues(options)
    if options.verbose:
        print 'Queue definitions:'
        for idx, queue in enumerate(queues):
            print '\t{0}: {1}'.format(queue, seconds2walltime(limits[idx]))
    qstat_parser = QstatParser()
    jobs = qstat_parser.parse(cmd_output)
    (
        running_jobs, running_nodes,
        queued_jobs, queued_nodes
    ) = count_jobs(jobs, queues, limits)
    if options.verbose:
        print 'Running jobs:'
        for queue in queues:
            print '\t{0}: {1} nodes, {2} jobs'.format(queue,
                                                      running_nodes[queue],
                                                      running_jobs[queue])
        print 'Queued jobs:'
        for queue in queues:
            print '\t{0}: {1} nodes, {2} jobs'.format(queue,
                                                      queued_nodes[queue],
                                                      queued_jobs[queue])
    url = plot_queues(running_jobs, running_nodes,
                      queued_jobs, queued_nodes,
                      queues, options)
    print 'URL: {0}'.format(url)
