'''Module to parse torque logs'''

from datetime import datetime, timedelta
import os

from vsc.pbs.job import PbsJob
from vsc.pbs.job_event import PbsJobEvent


class PbsLogParser(object):
    '''Implements torque log parser'''

    def __init__(self, config):
        '''Constructor'''
        self._config = config
        self._jobs = dict()

    def parse(self, start_date, end_date):
        '''Parse log information from the given start date to the end
        date, inclusive'''
        start_date = datetime.strptime(start_date, '%Y%m%d')
        end_date = datetime.strptime(end_date, '%Y%m%d')
        date = start_date
        delta_time = timedelta(days=1)
        while date <= end_date:
            file_name = os.path.join(self._config['log_dir'],
                                     date.strftime('%Y%m%d'))
            self.parse_file(file_name)
            date += delta_time

    def parse_file(self, file_name):
        '''Parse the specified log'''
        with open(file_name, 'r') as pbs_file:
            for line in pbs_file:
                line = line.rstrip()
                if not line:
                    continue
                time_stamp, event_type, job_id, info_str = line.split(';')
                if job_id not in self._jobs:
                    self._jobs[job_id] = PbsJob(self._config, job_id)
                event = PbsJobEvent(time_stamp, event_type, info_str)
                self._jobs[job_id].add_event(event)

    @property
    def jobs(self):
        '''return the jobs that were parsed as a list'''
        return self._jobs
