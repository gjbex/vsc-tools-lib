'''Module implementing utilities to analyze PBS torque job log files'''

from collections import namedtuple
from datetime import date, datetime
import json
from operator import itemgetter
import pandas as pd

from vsc.pbs.log import PbsLogParser
from vsc.utils import seconds2walltime, bytes2size


class AnalysisError(Exception):
    '''Signals a problem in the analysis'''

    def __init__(self, msg):
        self._msg = msg

    def __str__(self):
        return self._msg


class PbsLogAnalysis(object):
    '''class that wraps log analysis functionality'''

    default_job_columns = [
        'start', 'end', 'job_id', 'user', 'state', 'partition',
        'used_mem', 'used_walltime', 'spec_walltime', 'nodes', 'ppn',
        'hosts', 'exit_status',
    ]
    JobTuple = namedtuple('JobTuple', default_job_columns)
    default_host_columns = ['job_id', 'host', 'cores']
    HostTuple = namedtuple('HostTuple', default_host_columns)
    time_fmt = '%Y-%m-%d %H:%M:%S'

    def __init__(self, cfg_file_name):
        '''Constructor that takes the name of a JSON configuration file'''
        with open(cfg_file_name, 'r') as json_file:
            self._config = json.load(json_file)
        self._jobs = None
        self._df_jobs = None
        self._df_hosts = None

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
        if job.start:
            start_time = job.start.strftime(PbsLogAnalysis.time_fmt)
        else:
            start_time = '1970-01-01 00:00:00'
        if job.end:
            end_time = job.end.strftime(PbsLogAnalysis.time_fmt)
        else:
            end_time = datetime.now().strftime(PbsLogAnalysis.time_fmt)
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
        if job.resource_used('walltime'):
            walltime = seconds2walltime(job.resource_used('walltime'))
        else:
            walltime = None
        spec_walltime = seconds2walltime(job.resource_spec('walltime'))
        if job.exec_host:
            hosts = ' '.join(list(job.exec_host.keys()))
        else:
            hosts = None
        return PbsLogAnalysis.JobTuple(
                start=start_time,
                end=end_time,
                job_id=job.job_id,
                user=job.user,
                state=job.state,
                partition=job.partition,
                used_mem=mem_used,
                used_walltime=walltime,
                spec_walltime=spec_walltime,
                nodes=nodes,
                ppn=ppn,
                hosts=hosts,
                exit_status=job.exit_status
                )

    @staticmethod
    def _exec_host_to_tuples(job):
        '''returns a tuple with the exec_host information that will be
        inserted into a pandas data frame'''
        tuples = []
        if job.exec_host:
            for host in sorted(job.exec_host.keys()):
                tuples.append((job.job_id, host, job.exec_host[host]))
        return tuples

    def _jobs_to_dataframes(self):
        '''create pandas DataFrames out of a dictionary of jobs, the first
        contains the job information, the second keeps track of job-node
        associations'''
        job_data = []
        host_data = []
        for job_id, job in self._jobs.items():
            if job.has_end_event() or job.has_start_event():
                job_data.append(PbsLogAnalysis._job_to_tuple(job))
                host_data.extend(PbsLogAnalysis._exec_host_to_tuples(job))
        df_jobs = pd.DataFrame(sorted(job_data, key=itemgetter(2)),
                               columns=PbsLogAnalysis.default_job_columns)

        def time_conv(time):
            return pd.datetime.strptime(time, PbsLogAnalysis.time_fmt)
        df_jobs['start'] = df_jobs['start'].map(time_conv)
        df_jobs['end'] = df_jobs['end'].map(time_conv)
        df_jobs.ppn = df_jobs.ppn.fillna(-1.0).astype(int)
        df_jobs.exit_status = df_jobs.exit_status.fillna(-1024.0).astype(int)
        df_hosts = pd.DataFrame(host_data,
                                columns=PbsLogAnalysis.default_host_columns)
        return df_jobs, df_hosts

    def running_jobs(self, start_time=None, end_time=None, at_time=None):
        '''returns a pandas DataFrame containing the jobs that were
        running within the time interval specified by start and end
        time'''
        jobs = self.jobs_df
        if at_time:
            return jobs[(jobs.start < at_time) & (at_time < jobs.end)]
        elif start_time and end_time:
            return jobs[(jobs.start < end_time) & (jobs.end > start_time)]
        else:
            raise AnalysisError('either specify interval, or time at')
