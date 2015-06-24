'''module for dealing with Moab's checkjob output'''

import re


class CheckjobParser(object):
    '''Parser class for Moab checkjob ouptut'''

    def __init__(self):
        self._account_re = re.compile(r'account:(\S+)')

    def parse(self, job, checkjob_str):
        '''parse checkjob output, and return relevant status'''
        for line in checkjob_str.split('\n'):
            match = self._account_re.search(line)
            if match:
                job.account = match.group(1)
                continue
            if line.startswith('Holds:'):
                _, hold_str = line.strip().split(' ', 1)
                hold_strs = hold_str.strip().split(',')
                for hold in hold_strs:
                    job.add_hold(hold)
