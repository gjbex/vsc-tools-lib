#!/usr/bin/env python
'''class for parsing PBS options'''

import re

class InvalidPbsDirectiveError(Exception):
    '''Error indicating an invalid PBS directive'''

    def __init__(self, msg):
        super(InvalidPbsDirectiveError, self).__init__(self)
        self.message = msg


class PbsOptionParser(object):
    '''Parser for PBS options, either command line or directives'''

    def __init__(self):
        '''constructor'''
        self._options = {}
        self._option_re = re.compile(r'^\s*-([A-Za-z])\s*(.+)\s*$')

    def parse(self, option_str):
        '''parse options string'''
        match = self._option_re(option_str)
        if match:
            self.handle_option(match.group(1), match.group(2))
        else:
            msg = "option '{0}' does not parse".format(option_str)
            raise InvalidPbsDirectiveError(msg)

    def handle_option(self, option, value):
        if option == 'l':
            pass
        elif option == 'N':
            self.check_N(val)
        elif option == 'q':
            pass
        elif option == 'A':
            pass
        elif option == 'j':
            self.check_j(value)
        elif option == 'm':
            self.check_m(value)
        else:
            self._options[option].append(value)

    def check_j(self, val):
        '''check -j option, vals can be oe, eo, e, o, n'''
        if re.match(r'[oe]+|n$', val):
            msg = "'{0}' is not a valid join value".format(val)
            raise InvalidPbsDirectiveError(msg)

    def check_m(self, val):
        '''check -m option, vals can be any combination of b, e, a, or n'''
        if not re.match(r'[bea]{1,3}|n$', val):
            msg = "'{0}' is not a valid mail event value".format(val)
            raise InvalidPbsDirectiveError(msg)

    def check_N(self, val):
        '''check -N is a valid job name'''
        if not re.match(r'[A-Za-z]\w{,14}$', val):
            msg = "'{0}' is not a valid job name".format(val)
            raise InvalidPbsDirectiveError(msg)

