#!/usr/bin/env python
'''Module containing a number of useful functions'''

class InvalidWalltimeError(Exception):
    '''thrown when an invalid walltime format is used'''

    def __init__(self, msg):
        super(InvalidWalltimeError, self).__init__()
        self.message = msg


class InvalidSizeError(Exception):
    '''thrown when an invalid size format is used'''

    def __init__(self, msg):
        super(InvalidSizeError, self).__init__()
        self.message = msg


import re

def walltime2seconds(time_str):
    '''converts walltime [[[DD:]HHH?:]MM:]SS+ to seconds

    >>> walltime2seconds('1234')
    1234
    >>> walltime2seconds('13:12')
    792
    >>> walltime2seconds('3:02:45')
    10965
    >>> walltime2seconds('3:01:02:03')
    262923
    >>> walltime2seconds('1:2:3')
    Traceback (most recent call last):
        ...
    InvalidWalltimeError
    >>> walltime2seconds('1-02-03')
    Traceback (most recent call last):
        ...
    InvalidWalltimeError
    '''
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
    raise InvalidWalltimeError("'{0}' is invalid".format(time_str))

def seconds2walltime(seconds):
    '''convert a time in seconds to [[HHH:[MM:]SS

    >>> seconds2walltime(12)
    '00:00:12'
    >>> seconds2walltime(123)
    '00:02:03'
    >>> seconds2walltime(1234)
    '00:20:34'
    >>> seconds2walltime(12345)
    '03:25:45'
    '''
    seconds = int(seconds)
    secs = seconds % 60
    seconds = seconds//60
    mins = seconds % 60
    hours = seconds//60
    return '{h:02d}:{m:02d}:{s:02d}'.format(h=hours, m=mins, s=secs)

def size2bytes(amount, order=None):
    '''given a number and and order, compute size
    >>> size2bytes(12, 'k')
    12288
    >>> size2bytes(12, 't')
    13194139533312L
    >>> size2bytes(12, None)
    12
    >>> size2bytes(12, 'q')
    Traceback (most recent call last):
        ...
    InvalidSizeError
    >>> size2bytes('size', 't')
    Traceback (most recent call last):
        ...
    InvalidSizeError
    >>> size2bytes('12kb')
    12288
    >>> size2bytes('12 tw')
    13194139533312L
    '''
    conversion = {
        'k': 1024,
        'm': 1024**2,
        'g': 1024**3,
        't': 1024**4,
        None: 1,
    }
    if (type(amount) == str and ('b' in amount or 'w' in amount) and
        order is None):
        match = re.match(r'(\d+)\s*([kmgt]?)(?:b|w)$', amount)
        if match:
            amount = match.group(1)
            order = match.group(2)
        else:
            raise InvalidSizeError("'{0}' is not an integer".format(amount))
    try:
        return int(amount)*conversion[order]
    except ValueError:
        raise InvalidSizeError("'{0}' is not an integer".format(amount))
    except KeyError:
        raise InvalidSizeError("'{0}' is not a valid order "
                               "of magnitude".format(order))

def hostname2rackinfo(hostname):
    '''Determine rack number, IRU and node number from hostname'''
    match = re.match(r'r(\d+)i(\d+)n(\d+)', hostname)
    if match:
        return int(match.group(1)), int(match.group(2)), int(match.group(3))
    else:
        return None, None, None

if __name__ == '__main__':
    import doctest
    doctest.testmod()

