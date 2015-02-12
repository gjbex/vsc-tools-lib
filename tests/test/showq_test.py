#!/usr/bin/env python
'''module to test the vsc.moab.showq.ShowqParser parser'''

import unittest
from vsc.moab.showq import ShowqParser

class QstatTest(unittest.TestCase):
    '''Tests for the showq output parser'''

    def test_parsing(self):
        file_name = 'data/showq.txt'
        nr_categories = 3
        nr_active_jobs = 62
        nr_eligible_jobs = 60
        nr_blocked_jobs = 6
        parser = ShowqParser()
        with open(file_name, 'r') as showq_file:
            jobs = parser.parse_file(showq_file)
        self.assertEquals(nr_categories, len(jobs))
        self.assertEquals(nr_active_jobs, len(jobs['active']))
        self.assertEquals(nr_eligible_jobs, len(jobs['eligible']))
        self.assertEquals(nr_blocked_jobs, len(jobs['blocked']))
