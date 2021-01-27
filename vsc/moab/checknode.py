'''module for dealing with Moab's checknode output'''
import sys, re
from xml.dom import minidom

class ChecknodeParser(object):
    '''Parser class for Moab checknode ouptut'''

    def __init__(self, debug=False):
        """Constructor"""
        self._debug = debug

        self._blocks = list() # see parse_ascii()

        self.regex_inventory()

    def regex_inventory(self):
        """Inventory of the regular expressions needed"""
        self._reg_node = re.compile(r'^node\s+(?P<hostname>\w+)\n?')
        self._reg_state = re.compile(r'^.*\s+State:\s+(?P<state>\w+)\s+.*')
        self._reg_conf_res = re.compile(r'[\w\W]+Configured\s+Resources:\s+(?P<conf_res>[\w\W]+)\sUtilized[\w\W]+')
        self._reg_util_res = re.compile(r'[\w\W]+Utilized\s+Resources:\s+(?P<util_res>[\w\W]+)\sDedicated[\w\W]+')
        self._reg_dedi_res = re.compile(r'[\w\W]+Dedicated\s+Resources:\s+(?P<dedi_res>[\w\W]+)\s+Attributes[\w\W]+')
        self._reg_attrs = re.compile(r'[\w\W]+Attributes:\s+(?P<attrs>[\w\W]+)\s+Opsys[\w\W]+')
        # self._reg_cpuload = re.compile(r'[\w\W]+Speed:\s+.*CPULoad:\s+(?P<cpuload>[\w\W]+)\s+Partition[\w\W]+')
        self._reg_cpuload = re.compile(r'[\w\W]+Speed:.*CPULoad:\s+(?P<cpuload>[\w\W]+)\s+Partition[\w\W]+')
        # self._reg_cpuload = re.compile(r'[\w\W]+Speed:\s+(?P<speed>[\w\W]+)\s+CPULoad:\s+(?P<cpuload>[\w\W]+)\s+Partition[\w\W]+')
        self._reg_partition = re.compile(r'[\w\W]+Partition:\s+(?P<partition>[\w\W]+)\s+Rack/Slot[\w\W]+')
        self._reg_features = re.compile(r'[\w\W]+Features:\s+(?P<features>[\w\W]+)\s+NodeType[\w\W]+')
        self._reg_classes = re.compile(r'[\w\W]+Classes:\s+(?P<classes>[\w\W]+)\s+RM[\w\W]+')
        self._reg_policy = re.compile(r'[\w\W]+EffNodeAccessPolicy:\s+(?P<policy>[\w\W]+)\s+Successive[\w\W]+')
        self._reg_resrv = re.compile(r'[\w\W]+Reservations:\s+(?P<resrv>[\w\W]+)\s+Jobs[\w\W]+')
        self._reg_jobs = re.compile(r'^Jobs:\s+(?P<jobs>.*)$')

    def parse_file(self, filename):
        """Parse a file that contains checknode output

            Inputs:
                filename: str; full path to ascii file

            Returns:
                result of calling parse() method
        """
        with open(filename, 'r') as f:
            lines = ''.join(f.readlines())
            return self.parse_ascii(lines)

    def parse_ascii(self, output):
        """Parse output of checknode, and return a list of
        instances of BaseNode class"""
        # a block is checknode output for one node
        nodes = list()
        block = ''

        for line in output.split('\n'):
            if not line:
                continue
            else:
                line = line.strip()

            match_node = self._reg_node.match(line)
            match_jobs = self._reg_jobs.match(line)

            if match_node:
                block = line
            elif match_jobs:
                block += '\n' + line
                self._blocks.append(block)
                nodes.append( self.parse_one(block) )
            else:
                block += '\n' + line

    def parse_one(self, block=''):
        """Parse checknode for one node"""
        # replace \n, \t and \r with a space
        block.replace('\n', ' ')
        block.replace('\r', ' ')
        block.replace('\t', ' ')

        try:
            m_hostname = self._reg_node.match(block)
            self._hostname = m_hostname.group('hostname')

            m_state = self._reg_state.match(block)
            self._state = m_state.group('state')

            m_conf_res = self._reg_conf_res.match(block)
            self._conf_res = m_conf_res.group('conf_res')
            self._parse_resources(self._conf_res)

            m_util_res = self._reg_util_res.match(block)
            self._util_res = m_util_res.group('util_res')
            self._parse_resources(self._util_res)

            m_dedi_res = self._reg_dedi_res.match(block)
            self._dedi_res = m_dedi_res.group('dedi_res')
            self._parse_resources(self._dedi_res)

            m_attrs = self._reg_attrs.match(block)
            self._attrs = m_attrs.group('attrs')

            m_cpuload = self._reg_cpuload.match(block)
            self._cpuload = float(m_attrs.group('cpuload'))

            m_partition = self._reg_partition.match(block)
            self._partition = m_partition.group('partition')

            m_features = self._reg_features.match(block)
            self._features = m_features.group('features')

            m_classes = self._reg_classes.match(block)
            self._classes = m_classes.group('classes')

            m_policy = self._reg_policy.match(block)
            self._policy = m_policy.group('policy')

            m_resrv = self._reg_resrv.match(block)
            self._reservations = m_resrv.group('resrv')

            # m_jobs = self._reg_jobs.match(block)
            # self._jobs = m_jobs.group('jobs')

            # print(hostname, state)
        except Exception as err:
            print('Failed to capture fields: {0}'.format(err))
            print('so far: ', self._hostname, self._state, self._dedi_res)

    def _parse_resources(self, s):
        """Parse the string resources, like:
        PROCS: 36  MEM: 184G  SWAP: 185G  DISK: 1M  socket: 2  numanode: 2  core: 36  thread: 36
        """
        regex = r'^(PROCS:)?(?p<procs>\s+\d+)'


    def parse(self, checknode_xml):
        '''parse checknode XML output, and return the list of features'''
        dom = minidom.parseString(checknode_xml)
        node = dom.getElementsByTagName('node')[0]
        feature_str = node.getAttribute('FEATURES')
        return feature_str.split(',')

    def parse_xml_file(self, node_file):
        '''parse a file that contains checknode output'''
        node_output = ''.join(node_file.readlines())
        return self.parse(node_output)
