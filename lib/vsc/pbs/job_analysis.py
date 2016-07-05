'''Module implementing utilities to analyze PBS torque job log files'''

import numpy as np
from operator import itemgetter
import pandas as pd

from vsc.utils import seconds2walltime, bytes2size


default_job_columns = [
    'time', 'job_id', 'user', 'state', 'partition',
    'used_mem', 'used_walltime', 'spec_walltime', 'nodes', 'ppn',
    'hosts', 'exit_status',
]
time_fmt = '%Y-%m-%d %H:%M:%S'
default_host_columns = ['job_id', 'host', 'cores']

def job_to_tuple(job):
    nodes = job.resource_spec('nodes')[0]['nodes']
    if 'ppn' in job.resource_spec('nodes')[0]:
        ppn = job.resource_spec('nodes')[0]['ppn']
    else:
        ppn = None
    if job.resource_used('mem'):
        mem_used = float(bytes2size(job.resource_used('mem'), 'gb',
                                    no_unit=True, fraction=True))
    else:
        mem_used = None
    return (
        job.events[-1].time_stamp.strftime(time_fmt),
        job.job_id,
        job.user,
        job.state,
        job.partition,
        mem_used,
        (seconds2walltime(job.resource_used('walltime'))
             if job.resource_used('walltime') else None),
        seconds2walltime(job.resource_spec('walltime')),
        job.resource_spec('nodect'),
        ppn,
        ' '.join(job.exec_host.keys()) if job.exec_host else None,
        job.exit_status,
    )

def exec_host_to_tuples(job):
    tuples = []
    if job.exec_host:
        for host in sorted(job.exec_host.iterkeys()):
            tuples.append((job.job_id, host, job.exec_host[host]))
    return tuples

def jobs_to_dataframes(jobs):
    '''create a pandas DataFrame out of a dictionary of jobs'''
    job_data = []
    host_data = []
    for job_id, job in jobs.iteritems():
        if job.has_end_event() or job.has_start_event():
            job_data.append(job_to_tuple(job))
            host_data.extend(exec_host_to_tuples(job))
    df_jobs = pd.DataFrame(sorted(job_data, key=itemgetter(0)),
                           columns=default_job_columns)
    def time_conv(time):
        return pd.datetime.strptime(time, time_fmt)
    df_jobs['time'] = df_jobs['time'].map(time_conv)
    df_jobs.ppn = df_jobs.ppn.fillna(-1.0).astype(int)
    df_jobs.exit_status = df_jobs.exit_status.fillna(-1024.0).astype(int)
    df_hosts = pd.DataFrame(host_data, columns=default_host_columns)
    return df_jobs, df_hosts
