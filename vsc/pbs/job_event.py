'''Module to handle PBS job events'''

from datetime import datetime
import re

from vsc.pbs.option_parser import PbsOptionParser
from vsc.utils import walltime2seconds, size2bytes


class PbsJobEvent(object):
    '''Implements job events'''

    _int_keys = [
        'qtime', 'ctime', 'etime', 'start', 'end',
        'Resource_List.nodect',
        'total_execution_slots', 'unique_node_count', 'Exit_status',
        'resources_used.cput', 'resources_used.walltime',
    ]
    _time_keys = [
        'Resource_List.walltime',
    ]
    _mem_keys = [
        'Resource_List.mem', 'Resource_List.pmem', 'Resource_List.vmem',
        'resources_used.mem', 'resources_used.vmem',
    ]

    _key_map = {
        'jobname': 'name',
        'user': 'user',
        'Resource_List.partition': 'partition',
        'account': 'project',
        'queue': 'queue',
        'Exit_status': 'exit_status',
        'exec_host': 'exec_host',
    }

    def __init__(self, time_stamp, event_type, info_str):
        '''Constructor'''
        self._time_stamp = datetime.strptime(time_stamp,
                                             '%m/%d/%Y %H:%M:%S')
        self._type = event_type
        self._info = PbsJobEvent._parse_info(info_str)

    @property
    def time_stamp(self):
        '''returns time stamp for the event as a datetime object'''
        return self._time_stamp

    @property
    def type(self):
        '''returns event type'''
        return self._type

    def is_queue(self):
        '''returns True if this is a queue event'''
        return self.type == 'Q'

    def is_start(self):
        '''returns True if this is a start event'''
        return self.type == 'S'

    def is_end(self):
        '''returns True if this is a end event'''
        return self.type == 'E'

    def is_delete(self):
        '''returns True if this is a delete event'''
        return self.type == 'D'

    def has_info(self, key):
        '''return True if the event information has the specified key'''
        return key in self._info

    def info(self, key):
        '''return value in the event information for the specified key'''
        if self.has_info(key):
            return self._info[key]
        else:
            return None

    def __str__(self):
        '''returns string representation of the event'''
        time_stamp = self.time_stamp.strftime('%Y-%m-%d %H:%M:%S')
        info_strs = ['  {0}: {1}'.format(key, self._info[key])
                     for key in self._info]
        return '{0}: {1}\n{2}'.format(self.type, time_stamp,
                                      '\n'.join(info_strs))

    def update_job_info(self, job):
        '''update the job info based on this event'''
        job.state = self.type
        for key, attr_name in PbsJobEvent._key_map.items():
            if self.has_info(key):
                job.__setattr__(attr_name, self.info(key))
        prefix = 'Resource_List.'
        for key in self._info:
            if key.startswith(prefix):
                resource = key[len(prefix):]
                if resource == 'nodes':
                    node_specs = PbsOptionParser.parse_node_spec_str(self.info(key))
                    job.add_resource_spec(resource, node_specs)
                elif resource == 'feature':
                    job.add_resource_spec('features', [self.info(key)])
                else:
                    job.add_resource_spec(resource, self.info(key))
        prefix = 'resources_used.'
        for key in self._info:
            if key.startswith(prefix):
                resource = key[len(prefix):]
                job.add_resource_used(resource, self.info(key))

    @staticmethod
    def _parse_info(info_str):
        '''parse a job event info string into a dictionary'''
        key_value_strs = re.split(r'\s+', info_str)
        info = dict()
        for key_value_str in key_value_strs:
            if key_value_str:
                key, value = key_value_str.split('=', 1)
                info[key] = value
        PbsJobEvent._format_info(info)
        return info

    @staticmethod
    def _format_info(info):
        '''format values in info dictionary'''
        for key in PbsJobEvent._int_keys:
            if key in info:
                info[key] = int(info[key])
        for key in PbsJobEvent._time_keys:
            if key in info:
                info[key] = walltime2seconds(info[key])
        for key in PbsJobEvent._mem_keys:
            if key in info:
                info[key] = size2bytes(info[key])
        key = 'exec_host'
        if key in info:
            exec_host = dict()
            for exec_host_str in info[key].split('+'):
                host, cores = exec_host_str.split('/')
                exec_host[host] = cores
            info[key] = exec_host
