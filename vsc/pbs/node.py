'''Module to represent cluster nodes'''

import re

import vsc.utils

class NodeStatus(object):
    '''Class representing node status as reported by pbsnodes'''

    def __init__(self, hostname):
        '''Constructor'''
        self._hostname = hostname
        self._state = None
        self._np = None
        self._properties = None
        self._ntype = None
        self._jobs = None
        self._status = None
        self._note = None
        self._memory = 0
        self._gpus = 0
        self._gpu_status = list()

    @property
    def hostname(self):
        '''returns node's hostname'''
        return self._hostname

    @property
    def memory(self):
        '''returns the node's memory in bytes as integer'''
        if self._memory == 0 and self._status:
            self._memory = vsc.utils.size2bytes(self._status['physmem'])
        return self._memory

    @property
    def cpuload(self):
        '''Returns load average, None if no status information was
           available'''
        if self._status and self._status['loadave']:
            loadave = float(self._status['loadave'])
            ncpus = int(self._status['ncpus'])
            return loadave/ncpus
        return None

    @property
    def memload(self):
        '''Returns the memory load, None if no status information was
           available'''
        if (self._status and self._status['availmem'] and
                self._status['physmem']):
            physmem = vsc.utils.size2bytes(self._status['physmem'])
            availmem = vsc.utils.size2bytes(self._status['availmem'])
            return 1.0 - float(availmem)/float(physmem)
        return None

    @property
    def state(self):
        '''returns node's state'''
        return self._state

    @property
    def np(self):
        '''returns node's np'''
        return self._np

    @property
    def properties(self):
        '''returns node's properties'''
        return self._properties

    def has_property(self, prop):
        '''returns True if the NodeStatus has the given property'''
        if self._properties:
            return prop in self._properties
        else:
            return False

    @property
    def ntype(self):
        '''returns node's ntype'''
        return self._ntype

    @property
    def jobs(self):
        '''returns node's jobs'''
        return self._jobs

    @property
    def job_ids(self):
        '''returns a set of jobs IDs, empty if node is free'''
        job_ids = set()
        if self.jobs:
            for job_id in self.jobs.values():
                match = re.match(r'(\d+)', job_id)
                if match:
                    job_ids.add(match.group(1))
        return job_ids

    @property
    def status(self):
        '''returns node's status'''
        return self._status

    @property
    def note(self):
        '''returns node's note'''
        return self._note

    @state.setter
    def state(self, state):
        '''set node's state'''
        self._state = state

    @np.setter
    def np(self, np):
        '''set node's np'''
        self._np = int(np)

    @properties.setter
    def properties(self, properties):
        '''set node's properties'''
        self._properties = properties

    @ntype.setter
    def ntype(self, ntype):
        '''set node's ntype'''
        self._ntype = ntype

    @jobs.setter
    def jobs(self, jobs):
        '''set node's jobs'''
        self._jobs = jobs

    @status.setter
    def status(self, status):
        '''set node's status'''
        self._status = status

    @note.setter
    def note(self, note):
        '''set node's note'''
        self._note = note

    @property
    def gpus(self):
        '''returns number of GPUs, 0 for non-GPU nodes'''
        return self._gpus

    @property
    def gpu_status(self):
        '''return GPU info, list of dicts, one per GPU, empty list if no GPUs
        in node'''
        return self._gpu_status

    def add_gpu_status(self, gpu_status):
        '''add a gpu_status dictionary to the Node object'''
        self._gpu_status.insert(0, gpu_status)

    @property
    def gpu_states(self):
        '''returns a list of GPU states, empty if the system has none'''
        states = list()
        for status in self._gpu_status:
            states.append(status['gpu_state'])
        return states

    def __str__(self):
        '''returns string representation for node status'''
        node_str = self.hostname
        node_str += '\n\t{0} = {1}'.format('state', self.state)
        node_str += '\n\t{0} = {1}'.format('np', self.np)
        properties_str = ','.join(self.properties)
        node_str += '\n\t{0} = {1}'.format('properties', properties_str)
        node_str += '\n\t{0} = {1}'.format('ntype', self.ntype)
        if self.jobs:
            jobs_str = ','.join(
                ['{0}={1}'.format(k, v) for k, v in self.jobs.items()]
            )
            node_str += '\n\t{0} = {1}'.format('jobs', jobs_str)
        if self.status:
            status_str = ','.join(
                ['{0}={1}'.format(k, v) for k, v in self.status.items()]
            )
            node_str += '\n\t{0} = {1}'.format('status', status_str)
        if self.note:
            node_str += '\n\t{0} = {1}'.format('note', self.note)
        return node_str

