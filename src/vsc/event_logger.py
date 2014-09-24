#!/usr/bin/env python
'''module for dealing with events'''

class EventLogger(object):
    '''base class for classes that need to register events'''

    def __init__(self):
        '''Constructor, to be called by dervied classes'''
        super(EventLogger, self).__init__()
        self._events = []

    @property
    def events(self):
        '''return events generated during parsing'''
        return self._events

    def reg_event(self, event, extra={}):
        '''register a event'''
        self._events.append({'id': event,
                             'line': self._line_nr,
                             'extra': extra})

