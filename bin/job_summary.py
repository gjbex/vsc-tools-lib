#!/usr/bin/env python

from argparse import ArgumentParser
import json
import numpy as np
import re
import subprocess
import sys

from vsc.pbs.pbsnodes import PbsnodesParser
from vsc.utils import size2bytes, bytes2size, seconds2walltime

CFG_FILE = '../conf/config.json'


def get_numeric_job_id(job_id):
    match = re.match(r'(\d+)(?:\..+)?$', job_id)
    if match:
        return match.group(1)
    else:
        msg = "### error: job ID '{0}' seems malformed"
        sys.stderr.write(msg.format(job_id))
        sys.exit(1)


def compute_stats(data):
    array = np.array(data, dtype=np.float)
    return np.min(array), np.median(array), np.max(array)


def compute_memory(node):
    totmem = size2bytes(node.status['totmem'])
    availmem = size2bytes(node.status['availmem'])
    return totmem - availmem


def get_cpu_time(node, curr_job_id):
    for job_id, job_info in node.status['job_info'].iteritems():
        if job_id.startswith(curr_job_id):
            return job_info['cput']
    return None


def get_walltime(node, curr_job_id):
    for job_id, job_info in node.status['job_info'].iteritems():
        if job_id.startswith(curr_job_id):
            return job_info['walltime']
    return None

if __name__ == '__main__':
    arg_parser = ArgumentParser(description='print summary statistics '
                                            'for a running job')
    arg_parser.add_argument('job_id', help='ID of job to summarize')
    arg_parser.add_argument('--conf', default=CFG_FILE,
                            help='configuration file to use')
    arg_parser.add_argument('--nodes', action='store_true',
                            help='show comma-separated list of '
                                 'host names the job is running on')
    arg_parser.add_argument('--verbose', action='store_true',
                            help='show warnings')
    options = arg_parser.parse_args()
    job_id = get_numeric_job_id(options.job_id)
    with open(options.conf, 'r') as config_file:
        config = json.load(config_file)
    pbsnodes_output = subprocess.check_output(config['pbsnodes_cmd'])
    pbsnodes_parser = PbsnodesParser(options.verbose)
    nodes = pbsnodes_parser.parse(pbsnodes_output)
    loadaves = []
    mems = []
    netloads = []
    cput = None
    walltime = None
    node_names = []
    for node in nodes:
        if job_id in node.job_ids:
            loadaves.append(float(node.status['loadave']))
            mems.append(compute_memory(node))
            netloads.append(float(node.status['netload']))
            cpu_time = get_cpu_time(node, job_id)
            wall_time = get_walltime(node, job_id)
            if cpu_time:
                cput = cpu_time
            if wall_time:
                walltime = wall_time
            node_names.append(node.hostname)
    if loadaves:
        print 'nodes = {0}'.format(len(loadaves))
        if cput:
            print 'cpu time: {0}'.format(seconds2walltime(cput))
        if walltime:
            print 'walltime: {0}'.format(seconds2walltime(walltime))
        print 'loadave:'
        stats = compute_stats(loadaves)
        fmt_str = '  min: {0:.1f}, median: {1:.1f}, max: {2:.1f}'
        print(fmt_str.format(*stats))
        print 'mem:'
        stats = map(lambda x: float(bytes2size(x, 'gb', no_unit=True,
                                               fraction=True)),
                    compute_stats(mems))
        fmt_str = '  min: {0:.1f} GB, median: {1:.1f} GB, max: {2:.1f} GB'
        print(fmt_str.format(*stats))
        print 'netload:'
        stats = map(lambda x: float(bytes2size(x, 'gb', no_unit=True,
                                               fraction=True)),
                    compute_stats(netloads))
        fmt_str = '  min: {0:.1f} GB, median: {1:.1f} GB, max: {2:.1f} GB'
        print(fmt_str.format(*stats))
        if options.nodes:
            print('nodes:')
            print('  ' + ','.join(node_names))
    else:
        print 'job {0} is not running'.format(options.job_id)
