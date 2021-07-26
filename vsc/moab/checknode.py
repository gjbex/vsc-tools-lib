'''module for dealing with Moab's checknode output

Example:
>>> from vsc.moab.checknode import ChecknodeParser
>>> parser = ChecknodeParser
>>> checknode = parser.parse_ascii('/path/to/checknode/file.txt')
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
    state: list
    conf_resrcs: list
    util_resrcs: list
    dedi_resrcs: list
    cpuload: float
    partition: str
    features: str
    nodetype: str
    access_policy: str
    eff_policy: str
    n_job_fail: int
    times: list
    reservations: list
    jobs: list
    alert: list

    def __init__(self) -> None:
        '''Constructor

        Parameters
        ----------
        None
            No input expected when instantiating
        '''

        self.hostname = str()
        self.state = list()
        self.conf_resrcs = list()
        self.util_resrcs = list()
        self.dedi_resrcs = list()
        self.cpuload = 0.0
        self.partition = str()
        self.features = str()
        self.nodetype = str()
        self.access_policy = str()
        self.eff_policy = str()
        self.n_job_fail = 0
        self.times = list()
        self.reservations = list()
        self.jobs = list()
        self.alert = list()


class ChecknodeParser(object):
    '''Parser class for Moab checknode ouptut'''

    _debug: bool
    _nodes_str: str
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

    @property
    def n_nodes(self):
        """
        Getter

        Returns
        ----------
        n : int
            number of nodes returned by checknode command
        """

        return len(self._nodes)

    def _regex_inventory(self):
        """Inventory of the regular expressions needed"""

        self._reg_split = re.compile(r'\bnode\sr')
        self._reg_block = re.compile(
                r"\bnode\s(?P<hostname>[\w\W]+)\s[\w\W]+"
                r"\bState:\s+(?P<state>[\w\W]+)\s+\(.*\)[\w\W]+"
                r"\bConfigured Resources:\s(?P<conf_resrcs>[\w\W]+)[\w\W]+"
                r"\bUtilized\s+Resources:\s(?P<util_resrcs>[\w\W]+)[\w\W]+"
                r"\bDedicated\s+Resources:\s(?P<dedi_resrcs>[\w\W]+)[\w\W]+"
                r"\bAttributes:\s+[\w\W]+"
                r"\bOpsys:[\w\W]+"
                r"\bSpeed:\s[\w\W]+CPULoad:\s(?P<cpuload>[\w\W]+)[\w\W]+"
                r"\bPartition:\s+(?P<partition>[\w\W]+)\s+Rack[\w\W]+"
                r"\bFeatures:\s+(?P<features>[\w\W]+)[\w\W]+"
                r"\bNodeType:\s+(?P<nodetype>[\w\W]+)"
                r"\bClasses:\s*(?P<classes>[\w\W]+)[\w\W]+"
                r"\bRM\[.*\]\*:[\w\W]+"
                r"\bNodeAccessPolicy:\s+(?P<access_policy>[\w\W]+)[\w\W]+"
                r"\bEffNodeAccessPolicy:\s+(?P<eff_policy>SHARED|SINGLEUSER|SINGLEJOB)[\w\W]+"
                r"\bSuccessive Job Failures:\s+(?P<n_job_fail>[\d]+)[\w\W]+"
                r"\bTotal\sTime:\s+(?P<times>[\w\W]+)[\w\W]+"
                r"\bReservations:\s+(?P<reservations>[\w\W]+)[\w\W]+"
                r"\bJobs:\s+(?P<jobs>[\w\W,]+)[\w\W]+\s?[\w\W]+\b\n"
                r"(\bALERT:\s+(?P<alert>[\w\W]+))?"
                )
        self._reg_resrscs = re.compile(r'\w+:\s+[\d\w]+')

    def parse_file(self, filename):
        """
        Parse a file that contains checknode output

        Parameters
        ----------
        filename : str
            full path to "checknode ALL" output in ascii format

        Returns
        -------
        None
        """

        with open(filename, 'r') as f:
            lines = ' '.join(f.readlines())
            # lines = lines.replace('\n', ' ')
            self._nodes_str = lines
            self.parse_ascii(self._nodes_str)

    def _split_nodes_str(self):
        """
        Split the self._nodes_str string into blocks, one block per node

        This routine uses the re.split() method to split the checknode output
        (in long string format) into blocks, where each block starts with a
        phrase like "node r". This private method uses self._nodes_str and fills
        in the self._blocks list, so no input is needed, and no output is returned.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        if self._nodes_str:
            _blocks  = re.split(self._nodes_str, self._reg_split)
            self._blocks = [_ for _ in _blocks if _]
        else:
            raise InputError('Expecting non-empty input; something has gone wrong')
            if self._debug:
                sys.stderr.write('Error: _split_nodes_str: type(self._nodes_str)={}\n'.format(type(self._nodes_str)))
            sys.exit(_fail)

    def parse_ascii(self, output):
        """Parse output of checknode, and return a list of
        instances of BaseNode class"""
        # a block is checknode output for one node
        self._nodes = list()

        self._split_nodes_str()
        for _block in self._blocks:
            _node = self.parse_one(_block)
            if _node:
                self._nodes.append(_node)

    def parse_one(self, block=''):
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

        try:
            _match = self._reg_block.match(block)
            if self._debug:
                print('%'*40)
                print('Info: regex for block succeeded')
                print(_match.group('hostname'))
                print(_match.group('state'))
                print(_match.group('conf_resrcs'))
                print(_match.group('util_resrcs'))
                print(_match.group('dedi_resrcs'))
                print(_match.group('cpuload'))
                print(_match.group('partition'))
                print(_match.group('features'))
                print(_match.group('nodetype'))
                print(_match.group('access_policy'))
                print(_match.group('eff_policy'))
                print(_match.group('n_job_fail'))
                print(_match.group('times'))
                print(_match.group('reservations'))
                print(_match.group('jobs'))
                print(_match.group('alert'))
        except Exception as err:
            print('Error: regex for block failed')

        # extract the values for each group, and set them as attributes of
        # an instance of ChecknodeBlock class
        blk = ChecknodeBlock()
        blk.hostname = _match.group('hostname').strip()
        blk.state = _match.group('state').strip()
        blk.conf_resrcs = _match.group('conf_resrcs').strip()
        blk.util_resrcs = _match.group('util_resrcs').strip()
        blk.dedi_resrcs = _match.group('dedi_resrcs').strip()
        blk.cpuload = float(_match.group(cpuload))
        blk.partition = _match.group('partition').strip()
        blk.features = _match.group('features').strip()
        blk.nodetype = _match.group('nodetype').strip()
        blk.access_policy = _match.group('access_policy').strip()
        blk.eff_policy = _match.group('eff_policy').strip()
        blk.n_job_fail = int(_match.group('n_job_fail'))
        blk.times = _match.group('times').strip()
        blk.reservations = _match.group('reservations').strip()
        blk.jobs = _match.group('jobs').strip()
        blk.alert = _match.group('alert').strip()

        return blk


    def _parse_resources(self, resrcs):
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
                val = int(val) if val.isdigit() else val.strip()
                dic[key.strip()] = val
            return dic
        else:
            return None

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
