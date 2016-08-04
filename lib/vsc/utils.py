#!/usr/bin/env python
'''Module containing a number of useful functions'''

class InvalidWalltimeError(Exception):
    '''thrown when an invalid walltime format is used'''

    def __init__(self, msg):
        super(InvalidWalltimeError, self).__init__()
        self.message = msg

    def __str__(self):
        return self.message


class InvalidSizeError(Exception):
    '''thrown when an invalid size format is used'''

    def __init__(self, msg):
        super(InvalidSizeError, self).__init__()
        self.message = msg

    def __str__(self):
        return self.message


import math, re

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
    InvalidWalltimeError: '1:2:3' is invalid
    >>> walltime2seconds('1-02-03')
    Traceback (most recent call last):
        ...
    InvalidWalltimeError: '1-02-03' is invalid
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
    13194139533312
    >>> size2bytes(12, None)
    12
    >>> size2bytes(12, 'q')
    Traceback (most recent call last):
        ...
    InvalidSizeError: 'q' is not a valid order of magnitude
    >>> size2bytes('size', 't')
    Traceback (most recent call last):
        ...
    InvalidSizeError: 'size' is not an integer
    >>> size2bytes('12kb')
    12288
    >>> size2bytes('12 tw')
    13194139533312
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
        if order:
            return int(amount)*conversion[order]
        else:
            return int(amount)
    except ValueError:
        raise InvalidSizeError("'{0}' is not an integer".format(amount))
    except KeyError:
        raise InvalidSizeError("'{0}' is not a valid order "
                               "of magnitude".format(order))

def bytes2size(bytes, unit, no_unit=False, fraction=False):
    '''Conbert a number of bytes to the given unit (kb, mb, gb, tb)
       >>> bytes2size(34320, 'kb')
       '34kb'
       >>> bytes2size(12884463294, 'GB')
       '12GB'
       >>> bytes2size(12884463294, 'GB', no_unit=True)
       '12'
       >>> bytes2size(1297, 'kb', fraction=True)
       '1.3kb'
    '''
    conversion = {
        'b': 1.0,
        'kb': 1024.0,
        'mb': 1024.0**2,
        'gb': 1024.0**3,
        'tb': 1024.0**4,
    }
    if unit.lower() in conversion:
        mem = bytes/conversion[unit.lower()]
        if  fraction:
            return '{0:.1f}{1}'.format(mem, '' if no_unit else unit)
        else:
            mem = int(math.ceil(mem))
            return '{0:d}{1}'.format(mem, '' if no_unit else unit)
    else:
        raise InvalidSizeError("'{0}' is not a valid unit".format(unit))
    

def hostname2rackinfo(hostname):
    '''Determine rack number, IRU and node number from hostname'''
    match = re.match(r'r(\d+)i(\d+)n(\d+)', hostname)
    if match:
        return int(match.group(1)), int(match.group(2)), int(match.group(3))
    else:
        return None, None, None


def core_specs2count(core_spec_str):
    '''Determine the number of cores from a speciciatoin such as, e.g.,
    0-5,7,9-11

    >>> core_specs2count('0-5')
    6
    >>> core_specs2count('1-9,15-18')
    13
    >>> core_specs2count('5')
    1
    >>> core_specs2count('0-4,7,9-14,3')
    13
    '''
    core_count = 0
    for core_spec in core_spec_str.split(','):
        if core_spec.isdigit():
            core_count += 1
        elif '-' in core_spec:
            low, high = core_spec.split('-')
            core_count += int(high) - int(low) + 1
    return core_count

if __name__ == '__main__':
    import doctest
    doctest.testmod()

