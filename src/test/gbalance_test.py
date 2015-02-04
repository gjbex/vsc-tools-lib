#!/usr/bin/env python
'''module to test the vsc.mam.gbalance.GbalanceParser parser'''

import unittest
from vsc.mam.gbalance import GbalanceParser

class GbalanceTest(unittest.TestCase):
    '''Tests for the gbalance output parser'''

    def test_parsing(self):
        file_name = 'data/gbalance_old.txt'
        nr_accounts = 4
        first_id = '120'
        last_id = '1211'
        test_id = '714'
        test_name = 'lp_h_biology'
        test_available = 11696
        test_allocated = 15000
        parser = GbalanceParser()
        default_id = '120'
        default_name = 'default_project'
        default_available = 836
        default_allocated = 1000
        with open(file_name, 'r') as gbalance_file:
            accounts = parser.parse_file(gbalance_file)
            self.assertEquals(nr_accounts, len(accounts))
            self.assertTrue(first_id in accounts)
            self.assertTrue(last_id in accounts)
            self.assertEquals(test_name, accounts[test_id].name)
            self.assertEquals(test_available,
                              accounts[test_id].available_credits)
            self.assertEquals(test_allocated,
                              accounts[test_id].allocated_credits)
            self.assertEquals(default_name, accounts[default_id].name)
            self.assertEquals(default_available,
                              accounts[default_id].available_credits)
            self.assertEquals(default_allocated,
                              accounts[default_id].allocated_credits)
