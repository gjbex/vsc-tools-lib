'''Module to parse PBS torque's pbsnodes command output'''

import re, sys

from vsc.pbs.node import NodeStatus

class PbsnodesParser:
    '''Implements a parser for pbsnodes output'''

    def __init__(self, is_verbose=False):
        '''Constructor'''
        self._is_verbose = is_verbose

    def parse(self, node_output):
        '''parse output as produced by pbsnodes, returns a list of nodes'''
        nodes = []
        state = 'init'
        for line in node_output.split('\n'):
            if state == 'init' and re.match(r'^\w', line):
                node_str = line
                state = 'in_node'
            elif state == 'in_node':
                if len(line.strip()):
                    node_str += '\n' + line
                else:
                    try:
                        nodes.append(self.parse_node(node_str.strip()))
                    except Exception:
                        print("problem with node\n'{0}'".format(node_str))
                    state = 'init'
        return nodes

    def parse_file(self, node_file):
        '''parse a file that contains pbsnodes output'''
        node_output = ''.join(node_file.readlines())
        return self.parse(node_output)

    def parse_jobs(self, jobs_str):
        job_info = {}
        job_strs = jobs_str.split(' ')
        for job_str in job_strs:
            if '(' in job_str:
                job_id, info_str = job_str.split('(')
                job_info[job_id] = {}
                info_strs = info_str[:-2]
                for info_str in info_strs.split(','):
                    try:
                        key, value = info_str.split('=')
                        job_info[job_id][key] = value
                    except ValueError:
                        if self._is_verbose:
                            msg = '### warnig: no value for {0}\n'
                            sys.write(msg.format(key))
                        job_info[job_id][info_str] = None
        return job_info
        
    def parse_status(self, node_status, status_str):
        pos = status_str.find(',')
        while pos >= 0:
            if status_str.startswith('jobs='):
                last_pos = status_str.rfind(')')
                if last_pos == -1:
                    jobs = {}
                    status_str = status_str[pos + 1:]
                else:
                    start_pos = status_str.find('=') + 1
                    jobs_str = status_str[start_pos:last_pos + 1]
                    jobs = self.parse_jobs(jobs_str)
                    status_str = status_str[last_pos + 2:]
                node_status.status['job_info'] = jobs
                pos = status_str.find(',')

            else:
                try:
                    key, value = status_str[:pos].split('=')
                    node_status.status[key] = value
                except ValueError:
                    if self._is_verbose:
                        msg = '### warning: no value for {0} on {1}\n'
                        sys.stderr.write(msg.format(key,
                                                    node_status.hostname))
                    node_status.status[status_str[:pos]] = None
                status_str = status_str[pos + 1:]
                pos = status_str.find(',')

    def parse_node(self, node_str):
        '''parse a string containing pbsnodes information of single node'''
        lines = node_str.split('\n')
        hostname = lines.pop(0).strip()
        node_status = NodeStatus(hostname)
        for line in (l.strip() for l in lines):
            if line.startswith('np = '):
                _, np_str = line.split(' = ')
                node_status.np = int(np_str)
            elif line.startswith('properties = '):
                _, properties_str = line.split(' = ')
                node_status.properties = properties_str.split(',')
            elif line.startswith('status = '):
                _, status_str = line.split(' = ')
                node_status.status = {}
                if 'message=' in status_str:
                    match = re.search(r'message=(.*?)(?:,\w+=|$)',
                                      status_str)
                    message = match.group(1)
                    node_status.status['message'] = message
                    repl = 'message={0},'.format(message)
                    status_str = status_str.replace(repl, '')
                    if self._is_verbose:
                        msg = '### warning: message {0} on node {1}\n'
                        sys.stderr.write(msg.format(message, hostname))
                self.parse_status(node_status, status_str)
            elif line.startswith('ntype = '):
                _, node_status.ntype = line.split(' = ')
            elif line.startswith('state = '):
                _, node_status.state = line.split(' = ')
            elif line.startswith('note = '):
                _, node_status.note = line.split(' = ')
            elif line.startswith('jobs = '):
                _, job_str = line.split(' = ')
                node_status.jobs = {}
                for job_item in job_str.split(','):
                    core, job = job_item.split('/')
                    node_status.jobs[core] = job
        return node_status
