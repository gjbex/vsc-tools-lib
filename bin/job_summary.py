#!/usr/bin/env python

from argparse import ArgumentParser
import json
import numpy as np
import subprocess

from vsc.pbs.pbsnodes import PbsnodesParser
from vsc.utils import size2bytes, bytes2size, seconds2walltime

CFG_FILE = '../conf/config.json'


def compute_stats(data):
    return min(data), np.median(np.array(data, dtype=np.float)), max(data)


def compute_memory(node):
    totmem = size2bytes(node.status['totmem'])
    availmem = size2bytes(node.status['availmem'])
    return totmem - availmem


def get_cpu_time(node, curr_job_id):
    for job_id, job_info in node.status['job_info'].iteritems():
        if job_id.startswith(curr_job_id):
            return job_info['cput']
    return None


if __name__ == '__main__':
    arg_parser = ArgumentParser(description='print summary statistics '
                                            'for a running job')
    arg_parser.add_argument('job_id', help='ID of job to summarize')
    arg_parser.add_argument('--verbose', action='store_true',
                            help='show warnings')
    options = arg_parser.parse_args()
    with open(CFG_FILE, 'r') as config_file:
        config = json.load(config_file)
    pbsnodes_output = subprocess.check_output(config['pbsnodes_cmd'])
    pbsnodes_parser = PbsnodesParser(options.verbose)
    nodes = pbsnodes_parser.parse(pbsnodes_output)
    loadaves = []
    mems = []
    netloads = []
    cput = None
    for node in nodes:
        if options.job_id in node.job_ids:
            loadaves.append(float(node.status['loadave']))
            mems.append(compute_memory(node))
            netloads.append(float(node.status['netload']))
            cpu_time = get_cpu_time(node, options.job_id)
            if cpu_time:
                cput = cpu_time
    if loadaves:
        print 'nodes = {0}'.format(len(loadaves))
        print 'cpu time: {0}'.format(seconds2walltime(cput))
        print 'loadave:'
        stats = compute_stats(loadaves)
        print('  min: {0:.1f}, '
              'median: {1:.1f}, '
              'max: {2:.1f}').format(*stats)
        print 'mem:'
        stats = map(lambda x: float(bytes2size(x, 'gb', no_unit=True,
                                               fraction=True)),
                    compute_stats(mems))
        print('  min: {0:.1f} GB, '
              'median: {1:.1f} GB, '
              'max: {2:.1f} GB').format(*stats)
        print 'netload:'
        stats = map(lambda x: float(bytes2size(x, 'gb', no_unit=True,
                                               fraction=True)),
                    netloads)
        print('  min: {0:.1f} GB, '
              'median: {1:.1f} GB, '
              'max: {2:.1f} GB').format(*stats)
    else:
        print 'job {0} is not running'.format(options.job_id)
