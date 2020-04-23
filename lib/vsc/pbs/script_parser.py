#!/usr/bin/env python
'''class for parsing PBS files'''

import os
import re
from vsc.event_logger import EventLogger
from vsc.pbs.job import PbsJob
from vsc.pbs.option_parser import PbsOptionParser


class PbsScriptParser(EventLogger):
    '''Parser for PBS torque job files'''

    def __init__(self, config, event_defs, pbs_directive='#PBS'):
        '''Constructor'''
        super(PbsScriptParser, self).__init__(event_defs)
        self._config = config
        self._job = PbsJob(self._config)
        self._pbs_directive = pbs_directive
        regex = r'\s*{0}\s+(.+)$'.format(pbs_directive)
        self._pbs_re = re.compile(regex)
        regex = r'\s+{0}\s+(.+)$'.format(pbs_directive)
        self._pbs_indented_re = re.compile(regex)
        regex = r'\s*{0}\s+(.+)$'.format(pbs_directive)
        self._pbs_extract_re = re.compile(regex)
        regex = r'\s*-[A-Za-z]'
        self._pbs_option_re = re.compile(regex)
        self._pbs_option_parser = PbsOptionParser(self._config, event_defs,
                                                  self._job)
        self._state = None
        self._line_nr = 0
        self._pbs = []
        self._script_first_line_nr = None

    @property
    def script_first_line_nr(self):
        '''return the first line number of the Bash script part'''
        return self._script_first_line_nr

    def parse_file(self, pbs_file):
        '''parse a PBS file'''
        self._job.name = os.path.basename(pbs_file.name)
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

    @property
    def job(self):
        '''returns a PbsJob object representing the job script'''
        return self._job

    def check_encoding(self, line):
        '''checks ASCII encoding and line endings'''
        try:
            line.encode('ascii')
        except UnicodeDecodeError:
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
                     self.is_indented_pbs(line) or
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
        return self._pbs_re.match(line)

    def is_indented_pbs(self, line):
        '''returns True if the line contains an indented PBS directive'''
        return self._pbs_indented_re.match(line)

    def parse_shebang(self, line):
        '''parse shebang part of PBS file'''
        if self.is_shebang(line):
            self._job.shebang = line.strip()
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
            if self.is_indented_pbs(line):
                self.reg_event('indented_pbs_dir')
            match = self._pbs_extract_re.match(line)
            if match:
                option = match.group(1)
                if self._pbs_option_re.match(option):
                    self._pbs_option_parser.parse_args(option)
                    self.merge_events(self._pbs_option_parser.events)
                else:
                    self.reg_event('malformed_pbs_dir')
            else:
                self.reg_event('malformed_pbs_dir')
        else:
            self._state = 'script'
            if not self._script_first_line_nr:
                self._script_first_line_nr = self._line_nr
            self.parse_script(line)

    def parse_script(self, line):
        '''parse shell script part of a PBS file'''
        if self.is_shebang(line):
            self.reg_event('misplaced_shebang')
        if self.is_pbs(line):
            self.reg_event('misplace_pbs_dir')
        self._job.add_script_line(self._line_nr, line)
