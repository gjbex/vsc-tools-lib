'''Module to handle PBS job events'''

class PbsJobEvent(object):
    '''Implements job events'''

    def __init__(self, time_stamp, event_type, info):
        '''Constructor'''
        self._time_stamp = time_stamp
        self._type = event_type
        self._set_info(into)

    @property
    def time_stamp(self):
        '''returns time stamp for the event'''
        return self._time_stamp

    @property
    def type(self):
        '''retruns event type'''
        return self._type


class PbsJobStartEvent(PbsJobEvent):
    '''Implements a PBS job start event'''

    def _set_info(self, info):
        '''sets the information for a PBS job start event'''
        pass
