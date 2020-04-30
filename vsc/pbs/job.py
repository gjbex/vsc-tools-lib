'''module to represent and manipulate PBS jobs'''

from datetime import datetime
import operator
import os


class PbsJob(object):
    '''Class representing a PBS job'''

    def __init__(self, config, job_id=None):
        '''Constructor for a PBS job object'''
        self._id = job_id
        self._name = None
        self._user = None
        self._state = None
        self._resources_used = {}
        self._exec_hosts = None
        self._partition = None
        self._resource_specs = {
            'pmem': config['default_pmem'],
            'partition': config['default_partition'],
            'qos': config['default_qos'],
            'nodes': [{'nodes': config['default_nodes'],
                       'ppn': config['default_ppn'],
                       'properties': [], }],
            'walltime': config['default_walltime'],
            'features': [],
        }
        self._has_default_pmem = True
        self._queue = config['default_queue']
        self._exit_status = None
        self._project = None
        _, host, _, _, _ = os.uname()
        cwd = os.getcwd()
        self._io_specs = {
            'keep': config['default_keep'],
            'join': config['default_join'],
            'error': {'host': host, 'path': cwd},
            'output': {'host': host, 'path': cwd},
        }
        try:
            default_mail_addr = os.getlogin()
        except OSError:
            default_mail_addr = ''
        self._mail_specs = {
            'events': config['default_mail_events'],
            'addresses': [default_mail_addr]
        }
        self._shebang = None
        self._script = []
        self._is_time_limit_set = False
        self._events = []

    @property
    def job_id(self):
        '''Returns job ID, None for jobs that are not queued'''
        return self._id

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
    def exit_status(self):
        '''returns the job's exit status, or None'''
        return self._exit_status

    @exit_status.setter
    def exit_status(self, value):
        '''Set the job's exit status'''
        self._exit_status = value

    @property
    def resource_specs(self):
        '''returns the job's resource specifications'''
        return self._resource_specs

    def resource_spec(self, spec):
        '''returns the resource specification specified, None if that
           was not specified'''
        if spec in self._resource_specs:
            return self._resource_specs[spec]
        else:
            return None

    def add_resource_spec(self, key, value):
        self._resource_specs[key] = value

    def add_resource_specs(self, resource_specs):
        '''Add resources to specification'''
        for key, value in list(resource_specs.items()):
            self.add_resource_spec(key, value)

    @property
    def resources_used(self):
        '''returns the job's resource usage'''
        return self._resources_used

    def resource_used(self, key):
        if key in self._resources_used:
            return self._resources_used[key]
        else:
            return None

    def add_resources_used(self, resources_used):
        '''Add resources to used list'''
        for key, value in list(resources_used.items()):
            self.add_resource_used(key, value)

    def add_resource_used(self, key, value):
        self._resources_used[key] = value

    @property
    def has_default_pmem(self):
        '''returns True if the default value for pmem was not changed'''
        return self._has_default_pmem

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

    @property
    def events(self):
        '''return event list for this job'''
        return sorted(self._events, key=operator.attrgetter('time_stamp'))

    def has_start_event(self):
        '''returns True if this job has a start event'''
        return any(event.is_start() for event in self._events)

    def has_end_event(self):
        '''returns True if this job has a end event'''
        return any(event.is_end() for event in self._events)

    def add_event(self, event):
        '''add event to the job'''
        self._events.append(event)
        event.update_job_info(self)

    @property
    def end_event(self):
        if self.has_end_event():
            return self.events[-1]
        else:
            return None

    @property
    def start(self):
        '''return the start datetime of the job, None if not started'''
        if self.has_start_event():
            for event in self.events[::-1]:
                if event.type == 'S':
                    return event.time_stamp
        else:
            return None

    @property
    def end(self):
        '''return the end datetime of the job, None if not started'''
        if self.has_end_event():
            return self.events[-1].time_stamp
        else:
            return None

    @property
    def queue_time(self):
        '''return queue time as reported by qstat'''
        try:
            return self._queue_time
        except:
            return None

    @queue_time.setter
    def queue_time(self, value):
        '''set queue time as reported by qstat'''
        if type(value) == str:
            self._queue_time = datetime.strptime(value, '%c')
        else:
            self._queue_time = value

    @property
    def start_time(self):
        '''return start time as reported by qstat'''
        try:
            return self._start_time
        except:
            return None
    @start_time.setter
    def start_time(self, value):
        '''set start time as reported by qstat'''
        if type(value) == str:
            self._start_time = datetime.strptime(value, '%c')
        else:
            self._start_time = value

    @property
    def time_in_queue(self):
        if self.state == 'Q':
            return (datetime.now() - self.queue_time).total_seconds()
        else:
            return (self.start_time - self.queue_time).total_seconds()

    @property
    def walltime_used(self):
        '''return the walltime already used by the job, 0 if queued'''
        if self.state == 'Q':
            return 0
        else:
            return self.resource_used('walltime')

    @property
    def walltime_remaining(self):
        ''' return walltime remaining for a job, requested walltime if queued'''
        if self.state == 'Q':
            return self.resource_spec('walltime')
        else:
            delta = self.resource_spec('walltime') - self.resource_used('walltime')
            return delta if delta >= 0 else 0

    def attrs_to_str(self):
        '''return job attributes as a string, mainly for debug purposes'''
        attr_str = ''
        attr_str += "name = '{0}'".format(self.name)
        attr_str += "\nproject = '{0}'".format(self.project)
        attr_str += "\nresources:"
        for resource_name, resource_spec in list(self.resource_specs.items()):
            if resource_name == 'nodes':
                for node_spec in resource_spec:
                    for f_name, f_val in list(node_spec.items()):
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
