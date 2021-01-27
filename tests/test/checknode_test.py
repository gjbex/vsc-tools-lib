#!/usr/bin/env python
'''module to test the vsc.moab.checknode.Checknode parser'''

import sys, unittest
from vsc.moab.checknode import ChecknodeParser

class ChecknodeParserTest(unittest.TestCase):
    """Tests for checknode ascii output parser"""

    def setUp(self):
        self.filename_r27i13n24 = 'data/checknode_r27i13n24.txt'
        self.filename_genius = 'data/checknode_genius.txt'

    def tearDown(self):
        pass

    def test_parse_one(self):
        with open(self.filename_r27i13n24, 'r') as f:
            lines = f.readlines()
            block = ''.join(lines)

        parser = ChecknodeParser()
        parser.parse_one(block)

class ChecknodeParserXMLTest(unittest.TestCase):
    '''Tests for the checknode output parser'''

    def test_xml_parsing(self):
        # file_name = 'tests/test/data/checknode.xml'
        file_name = 'data/checknode.xml'
        expected_features = ['ivybridge', 'r1', 'tencore', 'thinking',
                             'type_ivybridge', 'r1i1', 'mem64']
        parser = ChecknodeParser()
        with open(file_name, 'r') as checknode_file:
            features = parser.parse_xml_file(checknode_file)
        self.assertListEqual(expected_features, features)
