#!/usr/bin/env python
'''module to test the vsc.moab.checknode.Checknode parser'''

import sys, unittest
from vsc.moab.checknode import ChecknodeParser, ChecknodeBlock

class ChecknodeParserTest(unittest.TestCase):
    """Tests for checknode ascii output parser"""

    def setUp(self):
        self.filename_r27i13n24 = 'data/checknode_r27i13n24.txt'
        self.filename_r23i13n23 = 'data/checknode_r23i13n23.txt'
        self.filename_genius = 'data/checknode_genius.txt'

    def tearDown(self):
        pass

    def test_parse_one_r27i13n24(self):
        with open(self.filename_r27i13n24, 'r') as f:
            lines = f.readlines()
            block = ''.join(lines)

        parser = ChecknodeParser(debug=False)
        chkblock = parser.parse_one(block)

        self.assertIsInstance(chkblock, ChecknodeBlock)
        self.assertEqual(chkblock.hostname, 'r27i13n24')
        self.assertEqual(chkblock.state, 'Busy')
        self.assertIsInstance(chkblock.conf_resrcs, dict)
        self.assertEqual(chkblock.conf_resrcs['PROCS'], '36')
        self.assertEqual(chkblock.conf_resrcs['MEM'], '184G')
        self.assertIsInstance(chkblock.util_resrcs, dict)
        self.assertEqual(chkblock.util_resrcs['SWAP'], '12G')
        self.assertEqual(chkblock.cpuload, 4.890)
        self.assertEqual(chkblock.partition, 'pbs')
        self.assertEqual(chkblock.nodetype, 'cascadelake')
        self.assertEqual(chkblock.access_policy, 'SHARED')
        self.assertEqual(chkblock.eff_policy, 'SINGLEJOB')
        self.assertEqual(chkblock.n_job_fail, 1)

        self.assertIsInstance(chkblock.reservations, tuple)
        self.assertEqual(len(chkblock.reservations), 2)
        self.assertIsInstance(chkblock.reservations[0], bool)
        self.assertIsInstance(chkblock.reservations[1], list)
        self.assertEqual(len(chkblock.reservations[1]), 1)
        self.assertIsInstance(chkblock.reservations[1][0], dict)
        self.assertEqual(chkblock.reservations[1][0]['jobid'], '50622561')
        self.assertEqual(chkblock.reservations[1][0]['ppn'], 1)
        self.assertEqual(chkblock.reservations[1][0]['job'], 'Running')
        self.assertEqual(chkblock.reservations[1][0]['remaining_time'], '-20:11:26')
        self.assertEqual(chkblock.reservations[1][0]['elapsed_time'], '6:03:47:34')
        self.assertEqual(chkblock.reservations[1][0]['walltime'], '6:23:59:00')

        self.assertEqual(chkblock.jobs, '50622561')
        self.assertEqual(chkblock.alert, 'node is in state Busy but load is low (4.890)')

    # def test_parse_one_r23i13n23(self):
    #     with open(self.filename_r23i13n23, 'r') as f:
    #         lines = f.readlines()
    #         block = ''.join(lines)

    #     parser = ChecknodeParser(debug=False)
    #     chkblock = parser.parse_one(block)

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
