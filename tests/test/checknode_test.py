#!/usr/bin/env python
'''module to test the vsc.moab.checknode.Checknode parser'''

import sys, unittest
from vsc.moab.checknode import ChecknodeParser

class ChecknodeParserTest(unittest.TestCase):
    '''Tests for the checknode output parser'''

    def test_parsing(self):
        file_name = 'data/checknode.xml'
        expected_features = ['ivybridge', 'r1', 'tencore', 'thinking',
                             'type_ivybridge', 'r1i1', 'mem64']
        parser = ChecknodeParser()
        with open(file_name, 'r') as checknode_file:
            features = parser.parse_file(checknode_file)
        self.assertListEqual(expected_features, features)
