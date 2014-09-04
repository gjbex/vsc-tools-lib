'''module to represent and manipulate PBS jobs'''

class PbsJob(object):
    '''Class representing a PBS job'''

    def __init__(self):
        '''Constructor for a PBS job object'''
        self._name = None
        self._resource_specs = {}
        self._mail_specs = {'events': None, 'addresses': []}
        self._queue = None
        self._project = None
        self._join = None
        self._keep = None
        self._error = None
        self._output = None
        self._shebang = None
        self._script = []

    @property
    def name(self):
        '''returns the job's name, None if not set'''
        return self._name

    @name.setter
    def name(self, name):
        '''Set the job's name'''
        self._name = name

    @property
    def project(self):
        '''returns the job's project name, None if not set'''
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
        return self._join

    @join.setter
    def join(self, join):
        '''set I/O join for job'''
        self._join = join

    @property
    def keep(self):
        '''return I/O keep'''
        return self._keep

    @keep.setter
    def keep(self, keep):
        '''set I/O keep for job'''
        self._keep = keep

    @property
    def error(self):
        '''return path for job's standard error'''
        return self._error

    @error.setter
    def error(self, error):
        '''set path for job's standard error'''
        self._error = error

    @property
    def output(self):
        '''return path for job's standard output'''
        return self._output

    @output.setter
    def output(self, output):
        '''set path for job's standard output'''
        self._output = output

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
        attr_str += "\njoin = '{0}'".format(self.join)
        attr_str += "\nkeep = '{0}'".format(self.keep)
        attr_str += "\nerror = '{0}'".format(self.error)
        attr_str += "\noutput = '{0}'".format(self.output)
        attr_str += "\nmail:"
        attr_str += "\n\tevents = '{0}'".format(self.mail_events)
        attr_str += "\n\taddresses = '{0}'".format(','.join(self.mail_addresses))
        return attr_str

