#!/usr/bin/env python
'''Utilities to deal with PBS torque qstat'''

from vsc.utils import walltime2seconds
from vsc.pbs.job import PbsJob

class QstatParser(object):
    '''Parser for full PBS torque qstat output'''

    def __init__(self):
        '''constructor'''
        self._jobs = {}

    def _get_value(self, line):
        '''extract value from line'''
        _, value = line.split('=', 1)
        return value.strip()

    def parse_record(self, record):
        '''parse an individual job record'''
        job = None
        resource_specs = {}
        resources_used = {}
        for line in record.split('\n'):
            line = line.strip()
            if line.startswith('Job Id:'):
                _, job_id = line.split(':', 1)
                job = PbsJob(job_id.strip())
            elif line.startswith('Job_Name ='):
                job.name = self._get_value(line)
            elif line.startwith('euser ='):
                job.user = self._get_value(line)
            elif line.startwith('job_state = '):
                job.state = self._get_value(line)
            elif line.startswith('queue ='):
                job.queue = self._get_value(line)
            elif line.startswith('Account_Name ='):
                job.project = self._get_value(line)
            elif line.startswith('resources_used.walltime ='):
                walltime = self._get_value(line)
                resources_used['walltime'] = walltime2seconds(walltime)
            elif line.startswith('Resources_List.walltime ='):
                walltime = self._get_value(line)
                resource_specs['walltime'] = walltime2seconds(walltime)
            elif line.startswith('exec_host ='):
                host_str = self._get_value(line)
                hosts = {}
                for host in host_str.split('+'):
                    node, core = host.split('/')
                    if node not in hosts:
                        hosts[node] = []
                    hosts[node].append(core)
                job.exec_host = hosts
        job.add_resource_specs(resource_specs)
        job.add_resources_used(resources_used)
        return job

    def parse(self, qstat_str):
        '''parse PBS torque qstat full output, and return list of jobs'''
        jobs = []
        job_str = None
        for line in qstat_str.split('\n'):
            if line.startwith('Job Id:'):
                if job_str:
                    jobs.append(self.parse_record(job_str))
                job_str = line
            elif line.strip():
                job_str += '\n' + line
        if job_str:
            jobs.append(self.parse_record(job_str))
        return jobs

