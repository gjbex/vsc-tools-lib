'''Module to parse PBS torque's pbsnodes command output'''

import re, sys

from vsc.pbs.node import NodeStatus

class PbsnodesParser(object):
    '''Implements a parser for pbsnodes output'''

    def __init__(self):
        '''Constructor'''
        pass

    def parse(self, node_output):
        '''parse output as produced by pbsnodes'''
        nodes = []
        state = 'init'
        for line in node_output.split('\n'):
            if state == 'init' and re.match(r'^\w', line):
                node_str = line
                state = 'in_node'
            elif state == 'in_node' and len(line.strip()):
                node_str += '\n' + line
            else:
                state = 'init'
                nodes.append(self.parse_node(node_str.strip()))
        return nodes

    def parse_file(self, node_file):
        '''parse a file that contains pbsnodes output'''
        node_output = ''.join(node_file.readlines())
        return self.parse(node_output)

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
                    match = re.search(r'message=(.*)(?:,\w+=|$)',
                                      status_str)
                    message = match.group(1)
                    node_status.status['message'] = message
                    repl = 'message={0},'.format(message)
                    status_str = status_str.replace(repl, '')
                    msg = '### warning: message {0} on node {1}\n'
                    sys.stderr.write(msg.format(message, hostname))
                for status_item in status_str.split(','):
                    try:
                        key, value = status_item.split('=')
                        node_status.status[key] = value
                    except ValueError:
                        msg = '### warning: {0} has no value on {1}\n'
                        sys.stderr.write(msg.format(status_item, hostname))
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


