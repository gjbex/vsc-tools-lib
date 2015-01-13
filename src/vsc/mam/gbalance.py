'''utilities for interacting with Adaptive MAM's gbalance'''

import re, sys

class GbalanceParser(object):
    '''parser for gbalance output'''

    def __init__(self, cmd):
        '''constructor'''
        self._cmd = cmd
        self._acc_re = re.compile(r'Account=([^,]+),?')
        self._old_re = re.compile(r'(\w+)')

    def parse(self, gbalance_output):
        '''parse the output of gbalance and return account information'''
        accounts = {}
        for line in gbalance_output.split('\n'):
            if len(line.strip()) == 0:
                continue
            if line.startswith('Id'):
                field_names = [x.strip() for x in line.split()]
                continue
            if line.startswith('--'):
                field_widths = [len(x) for x in line.split()]
                continue
            account = {}
            offset = 0
            for i, field_width in enumerate(field_widths):
                value = line[offset:offset + field_width].strip()
                account[field_names[i]] = value
                offset += field_width + 1
            if account['Name']:
                match = self._acc_re.match(account['Name'])
                if not match:
                    match = self._old_re.match(account['Name'])
                if not match:
                    msg = "can not parse account name '{0}'\n"
                    sys.stderr.write(msg.format(account['Name']))
                else:
                    account['Name'] = match.group(1)
            else:
                account['Name'] = 'default_project'
            accounts[account['Id']] = account
        return accounts
