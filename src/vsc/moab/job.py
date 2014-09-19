#!/usr/bin/env python

class InconsistantAttributesError(Exception):
    '''Exception indicating inconsistant attributes in the job status
       constructor'''

    def __init__(self, msg):
        '''Constructor'''
        super(InconsistantAttributesError, self).__init__()
        self.message = msg


class JobStatus(object):
    '''Moab job status'''

    def __init__(self, id, username, state, procs, time, date):
        '''Job status constructor'''
        self._id = id
        self._username = username
        self._state = state
        self._procs = procs
        self._time = time
        self._date = date

    @property
    def id(self):
        '''Returns job ID'''
        return self._id

    @property
    def username(self):
        '''Returns user name'''
        return self._username

    @property
    def state(self):
        '''Returns job state'''
        return self._state

    @property
    def procs(self):
        '''Returns number of processes'''
        return self._procs

    @property
    def remaining(self):
        '''Returns time remaining for a running job'''
        if self._state == 'Running':
            return self._time
        else:
            raise InconsistantAttributesError('non-running job has no remaining time')
        
    @property
    def starttime(self):
        '''Returns start time of a running job'''
        if self._state == 'Running':
            return self._date
        else:
            raise InconsistantAttributesError('non-running job has no starting time')
        
    @property
    def wclimit(self):
        '''Returns walltime limit of a non-running job'''
        if self._state != 'Running':
            return self._time
        else:
            raise InconsistantAttributesError('running job has no wall clock limit')
        
    @property
    def queuetime(self):
        '''Returns time non-running job was queued'''
        if self._state != 'Running':
            return self._date
        else:
            raise InconsistantAttributesError('running job has no queue time')

    def __str__(self):
        '''Returns string representation of a job status'''
        job_str = 'ID {0}:'.format(self.id)
        job_str += '\n\tuser: {0}'.format(self.username)
        job_str += '\n\tstate: {0}'.format(self.state)
        job_str += '\n\tprocs: {0}'.format(self.state)
        if self.state == 'Running':
            job_str += '\n\tremaining: {0}'.format(self.remaining)
        else:
            job_str += '\n\twclimit: {0}'.format(self.wclimit)
        if self.state == 'Running':
            job_str += '\n\tstarted: {0}'.format(self.starttime)
        else:
            job_str += '\n\tqueued: {0}'.format(self.queuetime)
        return job_str


import re

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

