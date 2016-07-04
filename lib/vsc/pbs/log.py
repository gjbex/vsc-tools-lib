'''Module to parse torque logs'''

import os

from vsc.pbs import PbsJob

class PbsLogParser(object):
    '''Implements torque log parser'''

    def __init__(self, config):
        '''Constructor'''
        self._config = config
        self._jobs = dict()

    def parse(self, start_date, end_date):
        '''Parse log information from the given start date to the end
        date, inclusive'''
        pass

    def parse_file(self, file_name):
        '''Parse the specified log'''
        with open(file_name, 'r') as pbs_file:
            for line in pbs_file:
                line = line.rstrip()
                if not line:
                    continue
                time_stamp, event_type, job_id, info = line.split(';') 
                if job_id not in self._jobs:
                    self._jobs[job_id] = PbsJob(self._config, job_id)
                self._set_info(job_id, time_stamp, event_type, info)

    def _set_info(self, job_id, time_stamp, event_type, info):
        '''Enrich the PbsJob object for the given jobID with the
        given event information'''
