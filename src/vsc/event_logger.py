#!/usr/bin/env python
'''module for dealing with events'''

class EventLogger(object):
    '''base class for classes that need to register events'''

    def __init__(self, context='file'):
        '''Constructor, to be called by dervied classes'''
        super(EventLogger, self).__init__()
        self._events = []
        self._context = context

    @property
    def events(self):
        '''return events generated during parsing'''
        return self._events

    def reg_event(self, event, extra={}):
        '''register a event'''
        if self._context == 'file':
            self._events.append({'id': event,
                                 'line': self._line_nr,
                                 'extra': extra})
        else:
            self._events.append({'id': event,
                                 'line': None,
                                 'extra': extra})

    def merge_events(self, events):
        '''merge events'''
        for event in events:
            self.reg_event(event['id'], event['extra'])

