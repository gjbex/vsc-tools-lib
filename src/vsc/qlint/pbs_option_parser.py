#!/usr/bin/env python
'''class for parsing PBS options'''

class InvalidPbsDirectiveError(Exception):
    '''Error indicating an invalid PBS directive'''

    def __init__(self, msg):
        super(InvalidPbsDirectiveError, self).__init__(self)
        self.message = msg


from argparse import ArgumentParser
import re

class PbsOptionParser(object):
    '''Parser for PBS options, either command line or directives'''

    def __init__(self):
        '''constructor'''
        self._options = {}
        self._arg_parser = ArgumentParser()
        self._arg_parser.add_argument('-l', action='append')
        self._arg_parser.add_argument('-j')
        self._arg_parser.add_argument('-m')
        self._arg_parser.add_argument('-N')

    def parse_args(self, option_line):
        '''parse options string'''
        options, rest = self._arg_parser.parse_known_args(optoins_line)

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

