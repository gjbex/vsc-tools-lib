'''Module containing a number of useful functions'''

class InvalidWalltimeError(Exception):
    '''thrown when an invalid walltime format is used'''

    def __init__(self, msg):
        super(InvalidWalltimeError, self).__init__()
        self.message = msg


import re

def walltime2seconds(time_str):
    '''converts walltime [[[DD:]HHH?:]MM:]SS+ to seconds'''
    time_str = time_str.strip()
    match = re.match(r'^(\d+)$', time_str)
    if match:
        return int(match.group(1))
    match = re.match(r'^(\d+):(\d{2})$', time_str)
    if match and int(match.group(2)) < 60:
        return 60*int(match.group(1)) + int(match.group(2))
    match = re.match(r'^(\d+):(\d{2}):(\d{2})$', time_str)
    if match and int(match.group(2)) < 60 and int(match.group(3)) < 60:
        return (3600*int(match.group(1)) + 60*int(match.group(2)) +
                int(match.group(3)))
    match = re.match(r'^(\d+):(\d{2}):(\d{2}):(\d{2})$', time_str)
    if match and (int(match.group(2)) < 24 and
                  int(match.group(3)) < 60 and
                  int(match.group(4)) < 60):
        return (24*3600*int(match.group(1)) + 3600*int(match.group(2)) +
                60*int(match.group(3)) + int(match.group(4)))
    raise InvalidWalltimeError(time_str)

def seconds2walltime(seconds):
    '''convert a time in seconds to [[HHH:[MM:]SS'''
    secs = seconds % 60
    seconds = seconds//60
    mins = seconds % 60
    hours = seconds//60
    return '{h:d}:{m:02d}:{s:02d}'.format(h=hours, m=mins, s=secs)
