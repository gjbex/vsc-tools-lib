#!/usr/bin/env python
'''module to test the vsc.moab.checknode.Checknode parser'''

from logging import debug
from subprocess import check_call
import sys, re, unittest
from vsc.moab.checknode import ChecknodeParser, ChecknodeBlock

class ChecknodeParserTest(unittest.TestCase):
    """Tests for checknode ascii output parser"""

    def setUp(self):
        self.filename_r27i13n24 = 'data/checknode_r27i13n24.txt'
        self.filename_r23i13n23 = 'data/checknode_r23i13n23.txt'
        self.filename_r24g05    = 'data/checknode_r24g05.txt'
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
        self.assertEqual(chkblock.conf_resrcs['PROCS'], 36)
        self.assertEqual(chkblock.conf_resrcs['MEM'], '184G')
        self.assertIsInstance(chkblock.util_resrcs, dict)
        self.assertEqual(chkblock.util_resrcs['SWAP'], '12G')
        self.assertEqual(chkblock.cpuload, 4.890)
        self.assertEqual(chkblock.partition, 'pbs')
        self.assertEqual(chkblock.nodetype, 'cascadelake')
        self.assertEqual(chkblock.access_policy, 'SHARED')
        self.assertEqual(chkblock.eff_policy, 'SINGLEJOB')

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
        job_cores = [dic['ppn'] for dic in chkblock.reservations[1]]
        self.assertNotEqual(chkblock.util_resrcs['PROCS'], sum(job_cores))

        self.assertIsInstance(chkblock.jobs, list)
        self.assertEqual(len(chkblock.jobs), 1)
        self.assertEqual(chkblock.jobs[0], '50622561')

        self.assertIsInstance(chkblock.alert, list)
        self.assertEqual(len(chkblock.alert), 1)
        self.assertEqual(chkblock.alert[0], 'node is in state Busy but load is low (4.890)')

    def test_parse_one_r23i13n23(self):
        """Complementary tests for ChecknodeBlock.reservations attribute"""
        with open(self.filename_r23i13n23, 'r') as f:
            lines = f.readlines()
            block = ''.join(lines)

        parser = ChecknodeParser(debug=False)
        chkblock = parser.parse_one(block)
        resrv = chkblock.reservations
        stnd_resr, usr_resr = resrv
        ures0 = usr_resr[0]
        ures3 = usr_resr[3]

        self.assertIsInstance(resrv, tuple)
        self.assertEqual(len(resrv), 2)

        self.assertTrue(stnd_resr)

        self.assertIsInstance(usr_resr, list)
        self.assertEqual(len(usr_resr), 4)

        self.assertIsInstance(ures0, dict)
        self.assertIn('jobid', ures0)
        self.assertEqual(ures0['jobid'], '50818310')
        self.assertEqual(ures0['ppn'], 3)
        self.assertEqual(ures0['job'], 'Running')
        self.assertEqual(ures0['remaining_time'], '-11:47:12')
        self.assertEqual(ures0['elapsed_time'], '3:12:48')
        self.assertEqual(ures0['walltime'], '15:00:00')
        self.assertEqual(ures3['jobid'], '50820647')
        self.assertEqual(ures3['ppn'], 4)

        self.assertIsInstance(chkblock.alert, list)
        self.assertEqual(len(chkblock.alert), 4)

    def test_parse_one_r24g05(self):
        """Complementary test for a CascadeLake GPU node"""
        with open(self.filename_r24g05, 'r') as fh:
            lines = fh.readlines()
            block = ''.join(lines)

        parser = ChecknodeParser(debug=False)
        chkblock = parser.parse_one(block)

        conf_resrcs = chkblock.conf_resrcs
        self.assertIn('GPUS', conf_resrcs)
        self.assertEqual(conf_resrcs['GPUS'], 8)

        util_resrcs = chkblock.util_resrcs
        self.assertEqual(util_resrcs['GPUS'], 6)
        self.assertEqual(util_resrcs['PROCS'], 34)

        self.assertEqual(chkblock.partition, 'gpu')
        self.assertIn('gpu', chkblock.nodetype)

        job_cores = [dic['ppn'] for dic in chkblock.reservations[1]]
        self.assertNotEqual(util_resrcs['PROCS'], sum(job_cores))


class checknodeParserFileTest(unittest.TestCase):
    '''Tests for `checknode ALL` command and the whole output'''

    def setUp(self):
        self.filename_all = 'data/checknode_ALL.txt'

    def tearDown(self):
        pass

    def test_all(self):
        parser = ChecknodeParser(debug=False)
        parser.parse_file(self.filename_all)
        _blocks = parser.blocks

        self.assertIsInstance(_blocks, list)
        self.assertEqual(len(_blocks), 267)
        for _block in _blocks:
            self.assertIsInstance(_block, ChecknodeBlock)
        self.assertEqual(_blocks[0].hostname,  'r23i13n23')
        self.assertEqual(_blocks[-1].hostname, 'r27i13n24')

    def test_get_block_by_hostname(self):
        parser = ChecknodeParser(debug=False)
        parser.parse_file(self.filename_all)

        hostname = 'r24g05'
        r24g05 = parser.get_block_by_hostname(hostname)
        self.assertEqual(r24g05.hostname, hostname)

        hostname = 'tier2-p-superdome-1'
        superdome = parser.get_block_by_hostname(hostname)
        self.assertEqual(superdome.hostname, hostname)

    def test_dic_blocks(self):
        parser = ChecknodeParser(debug=False)
        parser.parse_file(self.filename_all)
        _dic_blocks = parser.dic_blocks
        _list_hosts = _dic_blocks.keys()
        _regex_host = re.compile(r'r\d{2}i\d{2}n\d{2}|r\d{2}g\d{2}|tier2-p-superdome-1')

        for _host in _list_hosts:
            _match = re.match(_regex_host, _host)
            self.assertIsNotNone(_match)


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
