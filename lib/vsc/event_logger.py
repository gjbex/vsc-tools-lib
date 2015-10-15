#!/usr/bin/env python
'''module for dealing with events'''

class UndefinedEventError(Exception):

    def __init__(self, msg):
        super(UndefinedEventError, self).__init__(msg)


class EventLogger(object):
    '''base class for classes that need to register events'''

    def __init__(self, event_defs, context='file'):
        '''Constructor, to be called by dervied classes'''
        super(EventLogger, self).__init__()
        self._event_defs = event_defs
        self._events = []
        self._context = context

    @property
    def context(self):
        '''return context the EventLogger is operating in'''
        return self._context

    @context.setter
    def context(self, context):
        '''set the context for the EventLogger'''
        self._context = context

    @property
    def events(self):
        '''return events generated during parsing'''
        return self._events

    def reg_event(self, event, extra={}, line_nr=None):
        '''register a event'''
        if event not in self._event_defs:
            msg = "event '{0}' is undefined".format(event)
            raise UndefinedEventError(msg)
        if self.context == 'file':
            line_nr = line_nr if line_nr else self._line_nr
            self._events.append({'id': event,
                                 'line': line_nr,
                                 'extra': extra})
        else:
            self._events.append({'id': event,
                                 'line': None,
                                 'extra': extra})

    def merge_events(self, events):
        '''merge events'''
        for event in events:
            self.reg_event(event['id'], event['extra'], event['line'])

    @property
    def nr_errors(self):
        '''return number of errors logged so far'''
        nr_errors = 0
        for event in self._events:
            if self._event_defs[event['id']]['category'] == 'error':
                nr_errors += 1
        return nr_errors

    @property
    def nr_warnings(self):
        '''return number of warnings logged so far'''
        nr_warnings = 0
        for event in self._events:
            if self._event_defs[event['id']]['category'] == 'warning':
                nr_warnings += 1
        return nr_warnings
