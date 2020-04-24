#!/usr/bin/env python
'''module to test the vsc.pbs.pbsnodes.Pbsnodes parser'''

import io, sys, unittest
from vsc.pbs.pbsnodes import PbsnodesParser

class PbsnodesParserTest(unittest.TestCase):
    '''Tests for the pbsnodes output parser'''

    def test_parsing_with_messages(self):
        file_name = 'data/pbsnodes_message.txt'
        nr_nodes = 1
        warning_start = '### warning: message ERROR'
        warning_end = 'cleaned up on node r5i0n6\n'
        hostname = 'r5i0n6'
        loadave = '0.10'
        netload = '250792032533'
        state = 'free'
        parser = PbsnodesParser(is_verbose=True)
        with open(file_name, 'r') as pbsnodes_file:
            os_stderr = sys.stderr
            sys.stderr = io.StringIO()
            node_infos = parser.parse_file(pbsnodes_file)
            warning_msg = sys.stderr.getvalue()
            sys.stderr = os_stderr
        self.assertEqual(nr_nodes, len(node_infos))
        self.assertEqual(warning_start,
                         warning_msg[:len(warning_start)])
        self.assertEqual(warning_end,
                         warning_msg[-len(warning_end):])
        node_info = node_infos[0]
        self.assertEqual(hostname, node_info.hostname)
        self.assertEqual(loadave, node_info.status['loadave'])
        self.assertEqual(netload, node_info.status['netload'])
        self.assertEqual(netload, node_info.status['netload'])
        self.assertEqual(state, node_info.state)

    def test_parsing(self):
        file_name = 'data/pbsnodes.txt'
        nr_nodes = 173
        np = 20
        rack_str = 'r'
        parser = PbsnodesParser()
        with open(file_name, 'r') as pbsnodes_file:
            node_infos = parser.parse_file(pbsnodes_file)
        self.assertEqual(nr_nodes, len(node_infos))
        for node_info in node_infos:
            self.assertTrue(np <= node_info.np)
            self.assertTrue(node_info.hostname.startswith(rack_str))

    def test_parsing_gpu_node(self):
        file_name = 'data/pbsnodes_gpu.txt'
        nr_nodes = 1
        np = 36
        hostname = 'r22g35'
        memory = 192494548*1024
        cpuload = 3.02/36
        nr_jobs = 3
        parser = PbsnodesParser()
        with open(file_name, 'r') as pbsnodes_file:
            node_infos = parser.parse_file(pbsnodes_file)
        self.assertEqual(nr_nodes, len(node_infos))
        node_info = node_infos[0]
        self.assertEqual(np, node_info.np)
        self.assertEqual(node_info.hostname, hostname)
        self.assertEqual(node_info.memory, memory)
        self.assertEqual(node_info.cpuload, cpuload)
        self.assertEqual(len(node_info.jobs), nr_jobs)
