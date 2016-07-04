'''Module implementing utilities to analyze PBS torque job log files'''

import pandas as pd

from vsc.utils import seconds2walltime


default_columns = [
    'time', 'job_id', 'user', 'state', 'partition',
    'used_mem', 'used_walltime', 'spec_walltime', 'nodes', 'ppn',
    'hosts',
]
default_dtype = [
]

def job_to_tuple(job):
    parts = job.resource_spec('nodes').split(':')
    if len(parts) >= 2 and '=' in parts[1]:
        _, ppn = parts[1].split('=')
    else:
        ppn = None
    return (
        job.events[-1].time_stamp,
        job.job_id,
        job.user,
        job.state,
        job.partition,
        job.resource_used('mem'),
        seconds2walltime(job.resource_used('walltime')) if job.resource_used('walltime') else None,
        seconds2walltime(job.resource_spec('walltime')),
        job.resource_spec('nodect'),
        ppn,
        ' '.join(job.exec_host.keys()) if job.exec_host else None,
    )

def jobs_to_dataframes(jobs, columns=None, column_map=None):
    '''create a pandas DataFrame out of a dictionary of jobs'''
    data = []
    for job_id, job in jobs.iteritems():
        if job.has_end_event() or job.has_start_event():
            data.append(job_to_tuple(job))
    return pd.DataFrame(data, columns=default_columns)
