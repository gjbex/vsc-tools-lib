#!/usr/bin/env python

from vsc.utils import walltime2seconds, seconds2walltime
from vsc.pbs.qstat import QstatParser

if __name__ == '__main__':
    from argparse import ArgumentParser
    import subprocess

    arg_parser = ArgumentParser(description='parse qstat output to compute queue distributions')
    arg_parser.add_argument('--qstat', default='/usr/local/bin/qstat',
                            help='qstat command to use')
    arg_parser.add_argument('--qstat_options', default='-f',
                            help='qstat command options to use')
    arg_parser.add_argument('--queues', default='q1h:1,q24h:24,q72h:72,q7d:168,q21d:504',
                            help='queue categories, comma separated list of name:time (h)')
    arg_parser.add_argument('--file', help='file to use as input for debugging')
    options = arg_parser.parse_args()
    queues = []
    limits = []
    for qdef in options.queues.split(','):
        qname, limit = qdef.split(':')
        queues.append(qname)
        limit_secs = 3600*int(limit)
        limits.append(limit_secs)
    if options.file:
        with open(options.file, 'r') as qstat_file:
            cmd_output = ''.join([line for line in qstat_file])
    else:
        cmd_output = subprocess.check_output([options.qstat,
                                              options.qstat_options])
    qstat_parser = QstatParser()
    jobs = qstat_parser.parse(cmd_output)
    queue_nodes = {}
    queue_jobs = {}
    for queue in queues:
        queue_jobs[queue] = 0
    total_jobs = 0
    for job in jobs:
        if job.state == 'R':
            total_jobs += 1
            walltime = job.resource_specs['walltime']
            for idx, limit in enumerate(limits):
                if walltime <= limit:
                    if queues[idx] not in queue_nodes:
                        queue_nodes[queues[idx]] = set()
                    for node in job.exec_host.keys():
                        queue_nodes[queues[idx]].add(node)
                    queue_jobs[queues[idx]] += 1
                    break
    for queue in queues:
        if queue in queue_nodes:
            print '{0}: {1:d} nodes, {2:d} jobs'.format(queue,
                                                        len(queue_nodes[queue]),
                                                        queue_jobs[queue])
        else:
            print '{0}: 0 nodes, 0 jobs'.format(queue)
    print 'total: {0:d}'.format(total_jobs)

