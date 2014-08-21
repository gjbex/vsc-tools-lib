#!/usr/bin/env python
'''class for parsing PBS options'''

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
        self._events = []

    @property
    def events(self):
        '''returns events collected during parse'''
        return self._events

    def reg_event(self, event, extra={}):
        '''register an event when it occurs'''
        self._events.append({'event': event,
                             'extra': extra})

    def parse_args(self, option_line):
        '''parse options string'''
        self._events = []
        args = self._arg_parser.convert_arg_line_to_args(option_line)
        options, rest = self._arg_parser.parse_known_args(args)
        for option, value in options.__dict__.items():
            if value:
                self.handle_option(option, value)

    def handle_option(self, option, value):
        '''option dispatch method'''
        if option == 'l' and value is not None:
            pass
        elif option == 'N' and value is not None:
            self.check_N(value.strip())
        elif option == 'q' and value is not None:
            pass
        elif option == 'A' and value is not None:
            pass
        elif option == 'j' and value is not None:
            self.check_j(value.strip())
        elif option == 'm' and value is not None:
            self.check_m(value.strip())
        if option not in self._options:
            self._options[option] = []
        self._options[option].append(value)

    def check_j(self, val):
        '''check -j option, vals can be oe, eo, e, o, n'''
        if not re.match(r'^[oe]+|n$', val):
            self.reg_event('invalid_join', {'val': val})

    def check_m(self, val):
        '''check -m option, vals can be any combination of b, e, a, or n'''
        if not re.match(r'[bea]{1,3}|n$', val):
            self.reg_event('invalid_mail_event', {'val': val})

    def check_N(self, val):
        '''check -N is a valid job name'''
        if not re.match(r'[A-Za-z]\w{,14}$', val):
            self.reg_event('invalid_name', {'val': val})

