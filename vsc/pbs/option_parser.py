#!/usr/bin/env python
'''class for parsing PBS options'''

from argparse import ArgumentParser
import os
import re
import validate_email

from vsc.event_logger import EventLogger
from vsc.utils import walltime2seconds, size2bytes
from vsc.utils import InvalidWalltimeError


class PbsOptionParser(EventLogger):
    '''Parser for PBS options, either command line or directives'''

    def __init__(self, config, event_defs, job):
        '''constructor'''
        super(PbsOptionParser, self).__init__(event_defs, 'global')
        self._config = config
        self._job = job
        self._arg_parser = ArgumentParser()
        self._arg_parser.add_argument('-a')
        self._arg_parser.add_argument('-A')
        self._arg_parser.add_argument('-e')
        self._arg_parser.add_argument('-j')
        self._arg_parser.add_argument('-k')
        self._arg_parser.add_argument('-l', action='append')
        self._arg_parser.add_argument('-m')
        self._arg_parser.add_argument('-M')
        self._arg_parser.add_argument('-N')
        self._arg_parser.add_argument('-o')
        self._arg_parser.add_argument('-q')

    def parse_args(self, option_line):
        '''parse options string'''
        self._events = []
        args = option_line.split()
        options, rest = self._arg_parser.parse_known_args(args)
        for option, value in options.__dict__.items():
            if value:
                self.handle_option(option, value)
        if self._job.queue and not self._job._is_time_limit_set:
            walltime_limit = self.get_queue_limit(self._job.queue)
            if walltime_limit:
                self._job._resource_specs['walltime'] = walltime_limit

    def handle_option(self, option, value):
        '''option dispatch method'''
        if option == 'a':
            self.check_a(value.strip())
        elif option == 'A':
            self.check_A(value.strip())
        elif option == 'e' or option == 'o':
            self.check_oe(value.strip(), option)
        elif option == 'j':
            self.check_j(value.strip())
        elif option == 'k':
            self.check_k(value.strip())
        elif option == 'l':
            self.check_l(value)
        elif option == 'm':
            self.check_m(value.strip())
        elif option == 'M':
            self.check_M(value.strip())
        elif option == 'N':
            self.check_N(value.strip())
        elif option == 'q':
            self.check_q(value.strip())

    def is_valid_datetime(self, dt_str):
        regex = r'^\s*(?:(?:(?:(?:(?:\d{2})?\d{2})?\d{2})?\d{2})?\d{2})?\d{4}(?:\.\d{2})?\s*$'
        match = re.match(regex, dt_str)
        return match is not None

    def check_a(self, val):
        '''check whether a valid datetime string was specified'''
        if self.is_valid_datetime(val):
            pass
        else:
            self.reg_event('invalid_datetime', {'val': val})

    def check_A(self, val):
        '''check whether a valid project name was specified'''
        if re.match(r'[A-Za-z]\w*$', val):
            self._job.project = val
        else:
            self.reg_event('invalid_project_name', {'val': val})

    def get_queue_limit(self, queue_name):
        '''get the maximum walltime for the queue specified'''
        for queue_def in self._config['queue_definitions']:
            if queue_def['name'] == queue_name:
                return int(queue_def['walltime_limit'])
        return None

    def check_q(self, val):
        '''check whether a valid queue name was specified'''
        if re.match(r'[A-Za-z]\w*$', val):
            self._job.queue = val
        else:
            self.reg_event('invalid_queue_name', {'val': val})

    def check_j(self, val):
        '''check -j option, vals can be oe, eo, n'''
        if val == 'oe' or val == 'eo' or val == 'n':
            self._job.join = val
        else:
            self.reg_event('invalid_join', {'val': val})

    def check_k(self, val):
        '''check -k option, val can be e, o, oe, eo, or n'''
        if re.match(r'^[eo]+$', val) or val == 'n':
            self._job.keep = val
        else:
            self.reg_event('invalid_keep', {'val': val})

    def check_m(self, val):
        '''check -m option, val can be any combination of b, e, a, or n'''
        if re.match(r'^[bea]+$', val) or val == 'n':
            self._job.mail_events = val
        else:
            self.reg_event('invalid_mail_event', {'val': val})

    def check_M(self, val):
        '''check -M option'''
        self._job.mail_addresses = val.split(',')
        uid = os.getlogin()
        for address in self._job.mail_addresses:
            if (not validate_email.validate_email(address) and
                    address != uid):
                self.reg_event('invalid_mail_address', {'address': address})

    def check_N(self, val):
        '''check -N is a valid job name'''
        if re.match(r'[A-Za-z]\w{,14}$', val):
            self._job.name = val
        else:
            self.reg_event('invalid_job_name', {'val': val})

    def check_time_res(self, val, resource_spec):
        '''check a time resource'''
        attr_name, attr_value = val.split('=')
        try:
            seconds = walltime2seconds(attr_value)
            resource_spec[attr_name] = seconds
        except InvalidWalltimeError:
            self.reg_event('invalid_{0}_format'.format(attr_name),
                           {'time': attr_value})

    def check_generic_res(self, val, resource_spec):
        '''check a generic resource'''
        attr_name, attr_value = val.split('=')
        if attr_name == 'feature':
            resource_spec[attr_name] = attr_value.split(':')
        else:
            resource_spec[attr_name] = attr_value

    def check_mem_res(self, val, resource_spec):
        '''check memory resource'''
        attr_name, attr_value = val.split('=')
        match = re.match(r'(\d+)([kmgt])?[bw]', attr_value)
        if match:
            amount = int(match.group(1))
            order = match.group(2)
            resource_spec[attr_name] = size2bytes(amount, order)
        else:
            self.reg_event('invalid_{0}_format'.format(attr_name),
                           {'size': attr_value})

    def check_nodes_res(self, val, resource_spec):
        '''check nodes resource'''
        _, attr_value = val.split('=', 1)
        node_specs = PbsOptionParser.parse_node_spec_str(attr_value, self)
        resource_spec['nodes'] = node_specs

    def check_procs_res(self, val, resource_spec):
        '''check procs resource specification'''
        attr_name, attr_value = val.split('=')
        if attr_name in resource_spec:
            self.reg_event('multiple_procs_specs')
        if not attr_value.isdigit():
            self.reg_event('non_integer_procs', {'procs': attr_value})
        resource_spec[attr_name] = int(attr_value)

    def check_l(self, vals):
        '''check and handle resource options'''
        resource_spec = {}
        has_default_pmem = True
        # there can be multiple -l options on one line or on command line
        for val_str in (x.strip() for x in vals):
            # values can be combined by using ','
            for val in (x.strip() for x in val_str.split(',')):
                if (val.startswith('walltime=') or
                        val.startswith('cput=') or
                        val.startswith('pcput=')):
                    self.check_time_res(val, resource_spec)
                    self._job._is_time_limit_set = True
                elif (val.startswith('mem=') or val.startswith('pmem=') or
                      val.startswith('vmem=') or val.startswith('pvmem=')):
                    self.check_mem_res(val, resource_spec)
                    if val.startswith('pmem='):
                        has_default_pmem = False
                elif val.startswith('nodes='):
                    self.check_nodes_res(val, resource_spec)
                elif val.startswith('procs='):
                    self.check_procs_res(val, resource_spec)
                elif (val.startswith('partition=') or
                      val.startswith('feature') or
                      val.startswith('qos')):
                    self.check_generic_res(val, resource_spec)
                else:
                    self.reg_event('unknown_resource_spec', {'spec': val})
        self._job.add_resource_specs(resource_spec)
        self._job._has_default_pmem = has_default_pmem

    def check_oe(self, val, option):
        '''check for valid -o or -e paths'''
        if ':' in val:
            host, path = val.split(':', 1)
        else:
            host = None
            path = val
        if option == 'e':
            self._job.set_error(path, host)
        else:
            self._job.set_output(path, host)

    @staticmethod
    def parse_node_spec_str(attr_value, parser=None):
# if present, multiple node specifications are separated by '+'
        node_spec_strs = attr_value.split('+')
        node_specs = []
        for node_spec_str in node_spec_strs:
            node_spec = {'properties': []}
            spec_strs = node_spec_str.split(':')
# if a node spec starts with a number, that's the number of nodes,
# otherwise it can be a hostname or a feature, but number of nodes is 1
            if spec_strs[0].isdigit():
                node_spec['nodes'] = int(spec_strs[0])
            else:
                node_spec['nodes'] = 1
# note that this might be wrong, it may actually be a feature, but
# that is a semantic check, not syntax
                node_spec['host'] = spec_strs[0]
# now deal with the remaining specifications, ppn, gpus and properties
            for spec_str in spec_strs[1:]:
                if (spec_str.startswith('ppn=') or
                        spec_str.startswith('gpus=')):
                    key, value = spec_str.split('=')
                    if value.isdigit():
                        node_spec[key] = int(value)
                    elif parser:
                        parser.reg_event('{0}_no_number'.format(key),
                                         {'number': value})
                else:
                    node_spec['properties'].append(spec_str)
            node_specs.append(node_spec)
        return node_specs
