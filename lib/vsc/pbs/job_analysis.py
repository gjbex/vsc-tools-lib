'''Module implementing utilities to analyze PBS torque job log files'''

import json
import numpy as np
from operator import itemgetter
import pandas as pd

from vsc.pbs.log import PbsLogParser
from vsc.utils import seconds2walltime, bytes2size

class PbsLogAnalysis(object):
    '''class that wraps log analysis functionality'''

    default_job_columns = [
        'time', 'job_id', 'user', 'state', 'partition',
        'used_mem', 'used_walltime', 'spec_walltime', 'nodes', 'ppn',
        'hosts', 'exit_status',
    ]
    time_fmt = '%Y-%m-%d %H:%M:%S'
    default_host_columns = ['job_id', 'host', 'cores']

    def __init__(self, cfg_file_name):
        '''Constructor that takes the name of a JSON configuration file'''
        with open(cfg_file_name, 'r') as json_file:
            self._config = json.load(json_file)
        self._jobs = None

    def prepare(self, start=None, end=None):
        '''Prepare analysis by parsing log files and acquiring
        information'''
        pbs_parser = PbsLogParser(self._config)
        if not end:
            end_date = date.today()
            end = end_date.strftime('%Y%m%d')
        if not start:
            start = self._config.log_start
        pbs_parser.parse(start, end)
        self._jobs = pbs_parser.jobs
        self._df_jobs, self._df_hosts = self._jobs_to_dataframes()

    @property
    def jobs(self):
        '''return jobs in the analysis'''
        return self._jobs

    @property
    def jobs_df(self):
        '''return jobs pandas data frame'''
        return self._df_jobs
    
    @property
    def hosts_df(self):
        '''return hosts pandas data frame'''
        return self._df_hosts
    
    @staticmethod
    def _job_to_tuple(job):
        '''returns a tuple with the job information that will be inserted
        into a pandas data frame'''
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
        events = []
        last_event = (
            job.events[-1].time_stamp.strftime(PbsLogAnalysis.time_fmt),
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
        events.append(last_event)
        if last_event[3] == 'E':
            event = (
                job.events[-2].time_stamp.strftime(PbsLogAnalysis.time_fmt),
                job.job_id,
                job.user,
                'S',
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
            events.append(event)
        return events

    @staticmethod
    def _exec_host_to_tuples(job):
        '''returns a tuple with the exec_host information that will be
        inserted into a pandas data frame'''
        tuples = []
        if job.exec_host:
            for host in sorted(job.exec_host.iterkeys()):
                tuples.append((job.job_id, host, job.exec_host[host]))
        return tuples

    def _jobs_to_dataframes(self):
        '''create pandas DataFrames out of a dictionary of jobs, the first
        contains the job information, the second keeps track of job-node
        associations'''
        job_data = []
        host_data = []
        for job_id, job in self._jobs.iteritems():
            if job.has_end_event() or job.has_start_event():
                job_data.extend(PbsLogAnalysis._job_to_tuple(job))
                host_data.extend(PbsLogAnalysis._exec_host_to_tuples(job))
        df_jobs = pd.DataFrame(sorted(job_data, key=itemgetter(0)),
                               columns=PbsLogAnalysis.default_job_columns)
        def time_conv(time):
            return pd.datetime.strptime(time, PbsLogAnalysis.time_fmt)
        df_jobs['time'] = df_jobs['time'].map(time_conv)
        df_jobs.ppn = df_jobs.ppn.fillna(-1.0).astype(int)
        df_jobs.exit_status = df_jobs.exit_status.fillna(-1024.0).astype(int)
        df_hosts = pd.DataFrame(host_data, columns=PbsLogAnalysis.default_host_columns)
        return df_jobs, df_hosts

    def running_jobs(self, start_time, end_time):
        '''returns a pandas DataFrame containing the jobs that were
        running within the time interval specified by start and end
        time'''
        jobs = self.jobs_df
        started_jobs = jobs[(jobs.state == 'S') &
                                  (jobs.time < end_time)]
        ended_jobs = jobs[(jobs.state == 'E') &
                                (start_time < jobs.time)]
        running_jobs = pd.merge(started_jobs, ended_jobs,
                                how='inner', on='job_id')
        running_jobs.rename(columns={'time_x': 'start',
                                     'time_y': 'end'},
                            inplace=True)
        for column in started_jobs.columns:
            if column != 'time' and column != 'job_id':
                del running_jobs[column + '_x']
        renaming = {}
        for column in ended_jobs.columns:
            if column != 'time' and column != 'job_id':
                renaming[column + '_y'] = column
        running_jobs.rename(columns=renaming, inplace=True)
        return running_jobs
