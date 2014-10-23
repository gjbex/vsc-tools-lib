'''module to represent and manipulate PBS jobs'''

import os

class PbsJob(object):
    '''Class representing a PBS job'''

    def __init__(self, job_id=None):
        '''Constructor for a PBS job object'''
        self._id = job_id
        self._name = None
        self._user = None
        self._state = None
        self._resource_specs = {}
        self._resources_used = {}
        self._exec_hosts = None
        self._partition = None
        self._queue = None
        self._project = None
        _, host, _, _, _ = os.uname()
        cwd = os.getcwd()
        self._io_specs = {
            'keep': 'oe',
            'join': 'n',
            'error': {'host': host, 'path': cwd},
            'output': {'host': host, 'path': cwd}
        }
        self._mail_specs = {
            'events': None,
            'addresses': [os.getlogin()]
        }
        self._shebang = None
        self._script = []

    @property
    def job_id(self):
        '''Returns job ID, None for jobs that are not queued'''
        return  self._id

    @property
    def name(self):
        '''returns the job's name, None if not set'''
        return self._name

    @name.setter
    def name(self, name):
        '''Set the job's name'''
        self._name = name

    @property
    def user(self):
        '''Returs the user ID of the job's woner, if any'''
        return self._user

    @user.setter
    def user(self, user):
        '''Sets the job's owner's user ID'''
        self._user = user

    @property
    def state(self):
        '''Returs the job's state'''
        return self._state

    @state.setter
    def state(self, state):
        '''Sets the job's state'''
        self._state = state

    @property
    def partition(self):
        '''Returs the job's partition'''
        return self._partition

    @partition.setter
    def partition(self, partition):
        '''Sets the job's partition'''
        self._partition = partition

    @property
    def exec_host(self):
        '''Returns a map with exec hosts for a running job'''
        return self._exec_hosts

    @exec_host.setter
    def exec_host(self, exec_host):
        '''Sets the hosts a job is executing on'''
        self._exec_hosts = exec_host

    @property
    def project(self):
        '''Returns the job's project name, None if not set'''
        return self._project

    @project.setter
    def project(self, name):
        '''Set the job's project name'''
        self._project = name

    @property
    def queue(self):
        '''returns the job's queue name, None if not set'''
        return self._queue

    @queue.setter
    def queue(self, name):
        '''Set the job's queue name'''
        self._queue = name

    @property
    def resource_specs(self):
        '''returns the job's resource specifications'''
        return self._resource_specs

    def add_resource_specs(self, resource_specs):
        '''Add resources to specification'''
        for key, value in resource_specs.items():
            self._resource_specs[key] = value

    @property
    def resources_used(self):
        '''returns the job's resource usage'''
        return self._resources_used

    def add_resources_used(self, resources_used):
        '''Add resources to used list'''
        for key, value in resources_used.items():
            self._resources_used[key] = value

    @property
    def mail_events(self):
        '''return mail events'''
        return self._mail_specs['events']

    @mail_events.setter
    def mail_events(self, events):
        '''set mail events for job'''
        self._mail_specs['events'] = events

    @property
    def mail_addresses(self):
        '''return mail addresses'''
        return self._mail_specs['addresses']

    @mail_addresses.setter
    def mail_addresses(self, addresses):
        '''set mail address(es) for job mail events'''
        if type(addresses) == list:
            self._mail_specs['addresses'] = addresses
        else:
            self._mail_specs['addresses'] = [addresses]

    @property
    def join(self):
        '''return I/O join'''
        return self._io_specs['join']

    @join.setter
    def join(self, join):
        '''set I/O join for job'''
        self._io_specs['join'] = join

    @property
    def keep(self):
        '''return I/O keep'''
        return self._io_specs['keep']

    @keep.setter
    def keep(self, keep):
        '''set I/O keep for job'''
        self._io_specs['keep'] = keep

    @property
    def error(self):
        '''return path for job's standard error'''
        return (self._io_specs['error']['host'],
                self._io_specs['error']['path'])

    def set_error(self, path, host=None):
        '''set path for job's standard error'''
        if not os.path.isabs(path):
            path = os.path.join(self._io_specs['error']['path'], path)
        self._io_specs['error']['path'] = path
        if host:
            self._io_specs['error']['host'] = host

    @property
    def output(self):
        '''return path for job's standard output'''
        return (self._io_specs['output']['host'],
                self._io_specs['output']['path'])

    def set_output(self, path, host=None):
        '''set path for job's standard output'''
        if not os.path.isabs(path):
            path = os.path.join(self._io_specs['output']['path'], path)
        self._io_specs['output']['path'] = path
        if host:
            self._io_specs['output']['host'] = host

    @property
    def shebang(self):
        '''returns the job's shebang, None if not set'''
        return self._shebang

    @shebang.setter
    def shebang(self, shebang):
        '''Set the job's shebang'''
        self._shebang = shebang

    @property
    def script(self):
        '''returns the actual job script, i.e., the command to be executed'''
        return '\n'.join([line[1] for line in self._script])

    def add_script_line(self, line_nr, line):
        '''adds a line to the script, retains line number information'''
        self._script.append((line_nr, line))

    def attrs_to_str(self):
        '''return job attributes as a string, mainly for debug purposes'''
        attr_str = ''
        attr_str += "name = '{0}'".format(self.name)
        attr_str += "\nproject = '{0}'".format(self.project)
        attr_str += "\nresources:"
        for resource_name, resource_spec in self.resource_specs.items():
            if resource_name == 'nodes':
                for node_spec in resource_spec:
                    for f_name, f_val in node_spec.items():
                        attr_str += "\n\t{0} = '{1}'".format(f_name, f_val)
            else:
                attr_str += "\n\t{0} = '{1}'".format(resource_name,
                                                     resource_spec)
        attr_str += "\nqueue = {0}".format(self.queue)
        attr_str += '\nI/O:'
        attr_str += "\n\tjoin = '{0}'".format(self.join)
        attr_str += "\n\tkeep = '{0}'".format(self.keep)
        attr_str += "\n\terror = '{0}'".format(self.error)
        attr_str += "\n\toutput = '{0}'".format(self.output)
        attr_str += "\nmail:"
        attr_str += "\n\tevents = '{0}'".format(self.mail_events)
        address_str = ','.join(self.mail_addresses)
        attr_str += "\n\taddresses = '{0}'".format(address_str)
        return attr_str

