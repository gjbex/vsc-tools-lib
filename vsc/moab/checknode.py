'''module for dealing with Moab's checknode output

Example:
>>> # to parse a single block from a file:
>>> from vsc.moab.checknode import ChecknodeParser
>>> parser = ChecknodeParser(debug=False)
>>> checknode = parser.parse_ascii('/path/to/checknode/file.txt')
>>> # or
>>> block = parser.parse_one('/path/to/checknode/file.txt')

>>> # to parse the whole output of `checknode ALL` as a list of blocks:
>>> parser = ChecknodeParser(debug=False)
>>> parser.parse_file('/path/to/checknode_ALL.txt')
>>> blocks = parser.blocks
'''

import sys, re
from xml.dom import minidom

# generic return codes
_success = 0
_fail = 1

class InputError(Exception):
    """
    User defined exception for unexpected inputs

    InputError exception is raised when an input to a function is
    not in the anticipated format/type/size, etc.

    Inputs
    ------
    expression : str
        input expression in which the error occured

    message : str
        description of the error
    """

    def __init__(self, expression, message):
        """Constructor"""

        self.expression = expression
        self.message = message


class ChecknodeBlock(object):
    '''
    ChecknodeBlock represents one block of `checknode ALL` output for a single node

    For brevity (and avoiding to write many setter/getters), all class attributes are public
    '''

    # block-specific attributes are all public
    hostname: str
    state: str
    conf_resrcs: dict
    util_resrcs: dict
    dedi_resrcs: dict
    cpuload: float
    partition: str
    nodetype: str
    access_policy: str
    eff_policy: str
    reservations: tuple
    jobs: list
    alert: str

    def __init__(self) -> None:
        '''Constructor

        Parameters
        ----------
        None
            No input expected when instantiating
        '''

        self.hostname = str()
        self.state = str()
        self.conf_resrcs = dict()
        self.util_resrcs = dict()
        self.dedi_resrcs = dict()
        self.cpuload = 0.0
        self.partition = str()
        self.nodetype = str()
        self.access_policy = str()
        self.eff_policy = str()
        self.reservations = tuple()
        self.jobs = list()
        self.alert = str()

class ChecknodeParser(object):
    '''Parser class for Moab checknode ouptut'''

    _debug: bool
    _lines: list
    _blocks_str: str
    _dic_blocks: dict
    _blocks: list
    _nodes: list

    def __init__(self, debug=False):
        """Constructor

        Parameters
        ----------
        debug : bool, optional
            flag to turn debugging mode for extra info forwarded to STDOUT
            (default = False)
        """

        # attributes
        self._debug = debug
        # call these methods at init phase
        self._regex_inventory()
        self._parser_inventory()

    @property
    def n_nodes(self) -> int:
        """
        Getter

        Returns
        ----------
        n : int
            number of nodes returned by checknode command
        """

        return len(self._nodes)

    @property
    def blocks(self) -> list:
        """
        Getter

        Returns
        -------
        blocks : list
            a list of ChecknodeBlock instances are returned, one instance per node
        """
        return self._nodes

    @property
    def dic_blocks(self) -> dict:
        '''
        Return a dictionary of all nodes/blocks, where the key is the hostname

        Returns
        -------
        dic_blocks : dict
            the value for each key/hostname is a block as a string
        '''
        return self._dic_blocks

    def get_block_by_hostname(self, hostname):
        """
        Retrieve an instance of ChecknodeBlock from the list of all blocks, based on hostname

        Parameters
        ----------
        hostname : str
            hostname of the desired node/block to retrieve

        Returns
        -------
        node : ChecknodeBlock | None
            return an instance of ChecknodeBlock when the hostname matches; else None
        """
        if hostname in self._dic_blocks:
            _block_str = 'node {0} {1}'.format(hostname, self._dic_blocks[hostname])
            return self.parse_one(_block_str)

        return None

    def _regex_inventory(self) -> None:
        """Inventory of the regular expressions needed"""

        # regex for one checknode block
        self._reg_host = re.compile(r'(\bnode\s)?(?P<hostname>[\w\-]+).*')
        self._reg_resrscs = re.compile(r'\w+:\s+[\d\w]+')
        self._reg_rsrv = re.compile(r'[\w\W]+Reservations:\s+(?P<reservations>[\w\W]+)')
        self._reg_stnd_resrv = re.compile(r'Blocked Resources')
        self._reg_user_resrv = re.compile(r'(?P<jobid>[\w]{8})x(?P<ppn>[\d]+)\s+'
                                          r'Job:(?P<job>[\w]+)\s+'
                                          r'(?P<remaining>[\-\d\:]+)\s+->\s+'
                                          r'(?P<elapsed>[\d\:]+)\s+'
                                          r'\((?P<walltime>[\d\:]+)\)')
        self._reg_alert = re.compile(r'([\w\W]+ALERT:(?P<alert>[\w\W]+))?')
        # regex for parsers
        self._reg_first = re.compile(r'(?P<first>[\w]+)[\w\W]+')
        self._reg_load  = re.compile(r'[\w\W]+\s+CPULoad:\s+(?P<cpuload>[\d\.]+)')

    def _parser_inventory(self) -> None:
        """
        Inventory of the parsers

        For a single node, the checknode output is split into key: value pairs. Then, the
        values for few interesting keys are parsed using dedicated parsers. This parser inventory
        couples those interesting fields with their corresponding parsers

        This private method has no return, but it sets the private attribute (dict) self._dic_parsers
        """
        self._dic_parsers = dict()
        self._dic_parsers['state'] = self._parse_first_trash_rest
        self._dic_parsers['conf_resrcs'] = self._parse_resources
        self._dic_parsers['util_resrcs'] = self._parse_resources
        self._dic_parsers['dedi_resrcs'] = self._parse_resources
        self._dic_parsers['cpuload'] = self._parse_cpuload
        self._dic_parsers['partition'] = self._parse_first_trash_rest
        self._dic_parsers['reservations'] = self._parse_reservations

    def parse_file(self, filename) -> None:
        """
        Parse a file that contains checknode output

        Notes
        -----
        - the self._lines attribute is allocated as a list of strings (lines)
        - the self._blocks_str attribute is allocated as a (concatanated) string

        Parameters
        ----------
        filename : str
            full path to `checknode ALL` output in ascii format

        Returns
        -------
        None
        """

        with open(filename, 'r') as f:
            self._lines = f.readlines()
            self._blocks_str = ''.join(self._lines)
            self.parse_ascii(self._blocks_str)

    def _split_blocks_str(self):
        '''
        Split the self._blocks_str string into blocks

        Notes
        -----
        - blocks are stored as key/value pairs in self._dic_blocks dictionary, where
          the key is the hostname, and value is the rest of the string for each block
        - self._blocks is also filled in

        Parameters
        ----------
        None

        Returns
        -------
        None
        '''
        if not self._lines:
            if self._debug:
                sys.stderr.write('Error: _split_blocks_str: type(self._blocks_str)={}\n'.format(type(self._blocks_str)))
            raise InputError('Expecting non-empty input; something has gone wrong')

        self._dic_blocks = dict()
        for _line in self._lines:
            _match = re.match(r'^node\s+(?P<hostname>[\w\-]+)\s+.*', _line)
            if _match is not None:
                _hostname = _match.group('hostname')
                self._dic_blocks[_hostname] = ''
            elif _match is None:
                self._dic_blocks[_hostname] += _line
            else:
                print('Error: self._split_blocks_str: line: {0}'.format(_line), file=sys.stderr)

        self._blocks = ['node {0} {1}'.format(_k, _v) for _k, _v in self._dic_blocks.items()]

    def parse_ascii(self, output) -> None:
        """Parse output of checknode, and return a list of
        instances of BaseNode class"""
        # a block is checknode output for one node
        self._nodes = list()

        self._split_blocks_str()
        for _block in self._blocks:
            if not _block: continue
            _node = self.parse_one(_block)
            if _node:
                self._nodes.append(_node)

    def parse_one(self, block='') -> ChecknodeBlock:
        """Parse checknode for one node

           Checknode ALL

           Parameters
           ----------
           block : str, default=''
               the output of checknode for a single node

           Returns
           -------
           blk : ChecknodeBlock
               this instance of ChecknodeBlock contains all fields which are parsed by
               the regex (self._reg_block); the respective names of the fields defined
               in the regex matches the class attribute names
        """
        if self._debug: print(block)

        _dic = dict()
        _exclude = ['Reservations', 'ALERT']

        # extract single-line key: value pairs
        for line in block.split('\n'):
            kv = line.split(':', 1)

            if isinstance(kv, list) and len(kv) == 2:
                key, val = kv
                if key in _exclude: continue
                if 'RM[pbs]' in key: continue
                _dic[key] = val.strip()

        # extract hostname, and multi-line fields
        try:
            _host = re.match(self._reg_host, block)
            assert _host is not None
            _dic['hostname'] = _host.group('hostname')
        except AssertionError:
            sys.stderr.write('Error: regex for "hostname" field failed\n')
            sys.stderr.write('block is: {0}\n'.format(block))
            sys.exit(_fail)

        try:
            _trimmed_block = list()
            _lines = block.split('\n')
            for _line in _lines:
                if not _line: continue
                _break = _line.startswith('Jobs:') or _line.startswith('RM[pbs] ') or _line.startswith('ALERT')
                if _break:
                    break
                else:
                    _trimmed_block.append(_line)

            _trimmed_block = ''.join(_trimmed_block)

            _rsrv = re.match(self._reg_rsrv, _trimmed_block)
            assert _rsrv is not None
            _dic['reservations'] = _rsrv.group('reservations')
        except AssertionError:
            sys.stderr.write('Error: regex for "Reservations" field failed; host={0}\n'.format(_dic['hostname']))
            sys.exit(_fail)

        try:
            _alert = re.match(self._reg_alert, block)
            assert _alert is not None
            if _alert.group('alert') is None:
                _dic['alert'] = list()
            else:
                _alerts = _alert.group('alert').split('\n')
                _dic['alert'] = [_alert.strip() for _alert in _alerts if _alert]
        except AssertionError:
            sys.stderr.write('Error: regex for "ALERT" field failed; host={0}\n'.format(_dic['hostname']))
            sys.exit(_fail)

        if self._debug:
            for key, val in _dic.items():
                print('{0} : {1}'.format(key, val))

        # extract the values for each group, and set them as attributes of
        # an instance of ChecknodeBlock class
        try:
            blk = ChecknodeBlock()
            blk.hostname = _dic['hostname']
            blk.state = self._dic_parsers['state'](_dic['State'])
            blk.conf_resrcs = self._dic_parsers['conf_resrcs'](_dic['Configured Resources'])
            blk.util_resrcs = self._dic_parsers['util_resrcs'](_dic['Utilized   Resources'])
            blk.dedi_resrcs = self._dic_parsers['dedi_resrcs'](_dic['Dedicated  Resources'])
            blk.cpuload = self._dic_parsers['cpuload'](_dic['Speed'])
            blk.partition = self._dic_parsers['partition'](_dic['Partition'])
            blk.nodetype = _dic['NodeType']
            if 'NodeAccessPolicy' in _dic:
                blk.access_policy = _dic['NodeAccessPolicy']
            blk.eff_policy = _dic['EffNodeAccessPolicy']
            blk.reservations = self._dic_parsers['reservations'](_dic['reservations'])
            if 'Jobs' in _dic:
                blk.jobs  = _dic['Jobs'].split(',')
            if 'alert' in _dic:
                blk.alert = _dic['alert']
        except Exception:
            raise NotImplementedError('Error: parse_one: _dic to ChecknodeBlock instance failed; host={0}'.format(_dic['hostname']))

        return blk

    def _parse_resources(self, resrcs) -> dict:
        """
        Parse the string for resources

        The resources string can look like:
        PROCS: 36  MEM: 184G socket: 2  numanode: 2  core: 36  thread: 36
        This string is split into key value pairs. If the value is integral,
        the type of the value is int, else str

        Parameters
        ----------
        resrcs : str
            string representing (configured, utilized or dedicated) resources

        Returns
        -------
        output : dict | None
            keys and values are extracted from pairs that resemble "key: value".
            If the string cannot be matched, return None
        """
        _matches = re.findall(self._reg_resrscs, resrcs)
        dic = dict()
        if _matches:
            for _match in _matches:
                key, val = _match.split(':')
                val = int(val) if val.strip().isdigit() else val.strip()
                dic[key.strip()] = val
            return dic
        else:
            return None

    def _parse_reservations(self, resrv) -> tuple:
        """
        Parse the string for reservations

        The string for resources can contain standing reservations and running jobs, e.g.:
        Reservations:
        dedicated_nodes_16204.14548x1  User    -720days ->   INFINITY (  INFINITY)
            Blocked Resources@  -720days  Procs: 36/36 (100.00%)  Mem: 0/188493 (0.00%)  Swap: 0/189517 (0.00%)  Disk: 0/1 (0.00%)
        50818310x3  Job:Running  -10:38:12 -> 4:21:48 (15:00:00)
        50820317x4  Job:Running  -3:38:04 -> 6:21:56 (10:00:00)
        50820321x6  Job:Running  -3:35:16 -> 2:20:23:44 (2:23:59:00)
        50820582x1  Job:Running  -1:40:22 -> 2:22:18:38 (2:23:59:00)

        For the running jobs, the first column has the format <JobID>x<ppn>, so, we can capture/aggregate
        the total cores used on a node by parsing the "Reservations" block

        Parameters
        ----------
        resrv : str
            reservation string block

        Returns
        -------
        output : tuple (bool, list_of_dict)
            the first item in the tuple (bool) specifies whether there is a
            standing reservation on the node; the second item is a list of
            dictionaries for the jobs running on the node, a dictionary per
            job. Each dict contains the following key/value items:
                jobid : str
                ppn : int
                job : str
                remaining_time : str
                elapsed_time : str
                walltime : str
        """
        resrv = resrv.strip()
        dics = list()

        _matches = re.findall(self._reg_user_resrv, resrv)
        if _matches:
            for _match in _matches:
                dic = dict()
                dic['jobid'] = _match[0]
                dic['ppn'] = int(_match[1])
                dic['job'] = _match[2]
                dic['remaining_time'] = _match[3]
                dic['elapsed_time'] = _match[4]
                dic['walltime'] = _match[5]
                dics.append(dic)

        _stnd_resrv = re.search(self._reg_stnd_resrv, resrv)
        if _stnd_resrv:
            return (True, dics)
        else:
            return (False, dics)

    def _parse_first_trash_rest(self, line) -> str:
        """
        Parse a line where we only care about the first word, and the rest is ignored

        Parameters
        ----------
        line : str
            The line to be parsed

        Returns
        ------
        word : str
            The parsed/retrieved word
        """
        try:
            _match = re.match(self._reg_first, line.strip())
            assert _match is not None
            return _match.group('first')
        except AssertionError:
            sys.stderr.write('_parse_first_trash_rest failed on: {0}\n'.format(line))
            sys.exit(_fail)

    def _parse_cpuload(self, line) -> float:
        """
        Parse a line and retrieve the CPULoad

        Parameters
        ----------
        line : str
            The line to be parsed

        Returns
        -------
        cpuload : float
            CPULoad (in decimals)
        """
        try:
            _match = re.match(self._reg_load, line)
            assert _match is not None
            return float(_match.group('cpuload'))
        except AssertionError:
            sys.stderr.write('_parse_cpuload failed on: {0}\n'.format(line))
            sys.exit(_fail)

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
