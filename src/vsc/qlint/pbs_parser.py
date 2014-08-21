#!/usr/bin/env python
'''class for parsing PBS files'''

import re
from vsc.qlint.pbs_option_parser import (InvalidPbsDirectiveError,
                                         PbsOptionParser)

class PbsParser(object):
    '''Parser for PBS torque job files'''

    def __init__(self, pbs_directive='#PBS'):
        self._pbs_directive = pbs_directive
        regex = r'\s*{0}\s+(.+)$'.format(pbs_directive)
        self._pbs_extract = re.compile(regex)
        self._pbs_option_parser = PbsOptionParser()
        self._state = None
        self._line_nr = 0
        self._shebang = None
        self._pbs = []
        self._script = []
        self._events = []

    @property
    def shebang(self):
        '''return shebang to use for file'''
        return self._shebang

    @property
    def events(self):
        '''return events generated during parsing'''
        return self._events

    def parse(self, file_name):
        '''parses a PBS file specified by name'''
        with open(file_name, 'r') as pbs_file:
            self.parse_file(pbs_file)

    def check_encoding(self, line):
        '''checks ASCII encoding and line endings'''
        try:
            line.decode('ascii')
        except UnicodeDecodeError as error:
            self.reg_event('non_ascii')
        if line.endswith('\r\n'):
            self.reg_event('dos_format')
        if line.endswith('\r'):
            self.reg_event('mac_format')

    def is_comment(self, line):
        '''returns True if the line is a comment'''
        return (re.match(r'\s*#', line) and
                not (self.is_shebang(line) or
                     self.is_spaced_pbs(line) or
                     self.is_pbs(line)))

    def is_shebang(self, line):
        '''returns True if the line is a shebang'''
        return line.startswith('#!')

    def is_spaced_pbs(self, line):
        '''checks whether an extra space is added between hash and text
           in PBS directive, only for default directive string'''
        return self._pbs_directive == '#PBS' and re.match(r'\s*#\s+PBS\s+',
                                                          line)

    def is_pbs(self, line):
        '''returns True if the line is a PBS directive'''
        return re.match(self._pbs_directive, line)

    def reg_event(self, event, extra={}):
        '''register a event'''
        self._events.append({'event': event,
                             'line': self._line_nr,
                             'extra': extra})

    def parse_shebang(self, line):
        '''parse shebang part of PBS file'''
        if self.is_shebang(line):
            self._shebang = line.strip()
            if self._line_nr > 1:
                self.reg_event('misplaced_shebang')
            self._state = 'pbs'
        else:
            self.reg_event('missing_shebang')
            self._state = 'pbs'
            self.parse_pbs(line)

    def parse_pbs(self, line):
        '''parse PBS directives part of a PBS file'''
        if self.is_shebang(line):
            self.reg_event('misplaced_shebang')
        elif self.is_spaced_pbs(line):
            self.reg_event('space_in_pbs_dir')
        elif self.is_pbs(line):
            match = self._pbs_extract.match(line)
            if match:
                try:
                    self.parse_pbs_options(match.group(1))
                except InvalidPbsDirectiveError as error:
                    self.reg_event('invalid_pbs_dir',
                                   {'msg': error.message})
            else:
                self.reg_event('malformed_pbs_dir')
        else:
            self._state = 'script'
            self.parse_script(line)
    
    def parse_script(self, line):
        '''parse shell script part of a PBS file'''
        if self.is_shebang(line):
            self.reg_event('misplaced_shebang')
        if self.is_pbs(line):
            self.reg_event('misplace_pbs_dir')
        self._script.append(line)

    def parse_file(self, pbs_file):
        '''parse a PBS file'''
        self._state = 'start'
        self._line_nr = 0
        for line in pbs_file:
            self._line_nr += 1
            self.check_encoding(line)
            if self.is_comment(line):
                continue
            if len(line.strip()) == 0:
                continue
            if self._state == 'start':
                self.parse_shebang(line)
            elif self._state == 'pbs':
                self.parse_pbs(line)
            else:
                self.parse_script(line)
        if self._state == 'start':
            self.reg_event('no_script')
        elif self._state == 'pbs':
            self.reg_event('no_script')

    def parse_pbs_options(self, directive):
        '''parses a PBS option and stores it'''
        self._pbs_option_parser.parse_args(directive)

