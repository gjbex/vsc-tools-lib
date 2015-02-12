#!/usr/bin/env python
'''Utilities to parse optput of Adaptive Moab showq output'''

import re

from vsc.moab.job import JobStatus

class ShowqParser(object):
    '''Parser for Moab showq output'''

    def __init__(self):
        '''Moab showq output parser constructor'''
        pass

    def parse(self, showq_output):
        '''Returns map of jobs for given showq output'''
        jobs = {
            'active': [],
            'eligible': [],
            'blocked': []
        }
        parser_state = 'init'
        for line in showq_output.split('\n'):
            if len(line) == 0:
                pass
            elif line.startswith('active'):
                parser_state = 'active'
            elif line.startswith('eligible'):
                parser_state = 'eligible'
            elif line.startswith('blocked'):
                parser_state = 'blocked'
            elif re.match(r'\d+\s+(active|eligible|blocked)', line):
                pass
            elif line[0].isdigit():
                line = re.sub(r'\s+', ' ', line)
                jobid, username, state, procs, time, date = line.split(' ', 5)
                job = JobStatus(id=jobid, username=username, state=state,
                                procs=procs, time=time, date=date)
                jobs[parser_state].append(job)
        return jobs

    def parse_file(self, showq_file):
        '''Returns map of jobs for given showq output as file'''
        showq_output = ''.join(showq_file.readlines())
        return self.parse(showq_output)

