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
        self._account = None
        self._holds = []

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

    @property
    def account(self):
        '''Return the account specified for charging, if any'''
        return self._account

    @account.setter
    def account(self, account):
        '''Set the account specified for charging'''
        self._account = account

    @property
    def holds(self):
        '''Returns list of holds in place on job, may be empty if no
           holds are in place'''
        return self._holds

    def add_hold(self, hold):
        '''Add a hold to the job status'''
        self._holds.append(hold)

    def __str__(self):
        '''Returns string representation of a job status'''
        job_str = 'ID {0}:'.format(self.id)
        job_str += '\n\tuser: {0}'.format(self.username)
        job_str += '\n\tstate: {0}'.format(self.state)
        job_str += '\n\tprocs: {0}'.format(self.procs)
        if self.state == 'Running':
            job_str += '\n\tremaining: {0}'.format(self.remaining)
        else:
            job_str += '\n\twclimit: {0}'.format(self.wclimit)
        if self.state == 'Running':
            job_str += '\n\tstarted: {0}'.format(self.starttime)
        else:
            job_str += '\n\tqueued: {0}'.format(self.queuetime)
        return job_str
