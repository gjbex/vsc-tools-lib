'''utilities for interacting with Adaptive MAM's gbalance'''

import re, sys

from vsc.mam.account import MamAccount

class GbalanceParser(object):
    '''parser for gbalance output'''

    def __init__(self):
        '''constructor'''
        self._acc_re = re.compile(r'Account=([^,]+),?')
        self._old_re = re.compile(r'(\w+)')

    def parse_file(self, gbalance_file):
        '''parse a file that gbalance pbsnodes output'''
        gbalance_output = ''.join(gbalance_file.readlines())
        return self.parse(gbalance_output)

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
            account_info = {}
            offset = 0
            for field_width, field_name in zip(field_widths, field_names):
                value = line[offset:offset + field_width].strip()
                account_info[field_name] = value
                offset += field_width + 1
            if account_info['Name']:
                match = self._acc_re.match(account_info['Name'])
                if not match:
                    match = self._old_re.match(account_info['Name'])
                if not match:
                    msg = "can not parse account_info name '{0}'\n"
                    sys.stderr.write(msg.format(account_info['Name']))
                else:
                    account_info['Name'] = match.group(1)
            else:
                account_info['Name'] = 'default_project'
            account = MamAccount(acc_id=account_info['Id'],
                                 name=account_info['Name'])
            account.available_credits = account_info['Available']
            account.allocated_credits = account_info['Allocated']
            accounts[account.account_id] = account
        return accounts

