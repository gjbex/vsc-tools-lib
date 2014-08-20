#!/usr/bin/env python
'''class for parsing PBS files'''

import re
from pbs_option_parser import InvalidPbsDirectiveError, PbsOptionParser

class PbsParser(object):
    '''Parser for PBS torque job files'''

    def __init__(self, pbs_directive='#PBS'):
        self._pbs_directive = '#{0}'.format(pbs_directive)
        self._pbs_extract = re.compile(r'{0}\s+(.+)')
        self._line_nr = 0
        self._shebang = None
        self._pbs = []
        self._script = []
        self._warnings = []
        self._errors = []

    @property
    def shebang(self):
        '''return shebang to use for file'''
        return self._shebang

    @property
    def warnins(self):
        '''return warnings generated during parsing'''
        return self._warnings

    @property
    def errors(self):
        '''return errors generated during parsing'''
        return self._errors

    def parse(self, file_name):
        '''parses a PBS file specified by name'''
        with open(file_name, 'r') as pbs_file:
            self.parse_file(pbs_file)

    def check_encoding(self, line):
        '''checks ASCII encoding and line endings'''
        try:
            line.decode('ascii')
        except UnicodeDecodeError as error:
            self.error(error.messsage)
        if line.endswith('\r\n') or line.endswith('\r'):
            self.error('non-Unix line ending')

    def is_shebang(self, line):
        '''returns True if the line is a shebang'''
        return line.startswith('#!')

    def is_pbs(self, line):
        '''returns True if the line is a PBS directive'''
        return re.match(self._pbs_directive, line)

    def warning(self, msg):
        '''register a warning'''
        message = 'line {0}: {1}'.format(self._line_nr, msg)
        self._warnings.append(message)

    def error(self, msg):
        '''register a error'''
        message = 'line {0}: {1}'.format(self._line_nr, msg)
        self._errors.append(message)

    def parse_file(self, pbs_file):
        '''parse a PBS file'''
        state = 'start'
        self._line_nr = 0
        for line in pbs_file:
            self._line_nr += 1
            self.check_encoding(line)
            if state == 'start':
                if self.is_shebang(line):
                    self._shebang = line.strip()
                else:
                    self.warning('missing shebang')
                state = 'pbs'
            elif state == 'pbs':
                if self.is_shebang(line):
                    self.warning('shebang out of place')
                if self.is_pbs(line):
                    match = self._pbs_extract.match(line)
                    if match:
                        try:
                            self.parse_pbs(match.group(1))
                        except InvalidPbsDirectiveError as error:
                            self.error(error.message)
                    else:
                        self.error('malformed PBS directive')
                else:
                    self._script.append(line)
                    state = 'script'
            else:
                if self.is_shebang(line):
                    self.warning('shebang out of place')
                if self.is_pbs(line):
                    self.warning('PBS directive out of place')
                self._script.append(line)
        if state == 'start':
            self.error('no PBS direcives or script in file')
        elif state == 'pbs':
            self.error('no script in file')

    def parse_pbs(self, directive):
        '''parses a PBS option and stores it'''
        pass

