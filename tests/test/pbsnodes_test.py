#!/usr/bin/env python
'''module to test the vsc.pbs.pbsnodes.Pbsnodes parser'''

import io, sys, unittest
from vsc.pbs.pbsnodes import PbsnodesParser

class PbsnodesParserTest(unittest.TestCase):
    '''Tests for the pbsnodes output parser'''

    def test_parsing_with_messages(self):
        file_name = 'tests/test/data/pbsnodes_message.txt'
        nr_nodes = 1
        warning_start = '### warning: message ERROR'
        warning_end = 'cleaned up on node r5i0n6\n'
        hostname = 'r5i0n6'
        loadave = '0.10'
        netload = '250792032533'
        state = 'free'
        nr_gpus = 0
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
        self.assertEqual(nr_gpus, node_info.gpus)
        self.assertEqual(0, len(node_info.gpu_states))

    def test_parsing_genius2(self):
        file_name = 'tests/test/data/pbsnodes_genius_ehsan.txt'
        nr_nodes = 243
        parser = PbsnodesParser(is_verbose=False)
        with open(file_name, 'r') as pbsnodes_file:
            node_infos = parser.parse_file(pbsnodes_file)
        self.assertEqual(nr_nodes, len(node_infos))

    def test_parsing_breniac(self):
        file_name = 'tests/test/data/pbsnodes_breniac.txt'
        nr_nodes = 988
        parser = PbsnodesParser(is_verbose=False)
        with open(file_name, 'r') as pbsnodes_file:
            node_infos = parser.parse_file(pbsnodes_file)
        self.assertEqual(nr_nodes, len(node_infos))

    def test_parsing_thinking(self):
        file_name = 'tests/test/data/pbsnodes_thinking.txt'
        nr_nodes = 149
        parser = PbsnodesParser(is_verbose=False)
        with open(file_name, 'r') as pbsnodes_file:
            node_infos = parser.parse_file(pbsnodes_file)
        self.assertEqual(nr_nodes, len(node_infos))

    def test_parsing_gpu_node(self):
        file_name = 'tests/test/data/pbsnodes_gpu.txt'
        nr_nodes = 1
        np = 36
        hostname = 'r22g35'
        memory = 192494548*1024
        cpuload = 3.02/36
        nr_jobs = 3
        nr_gpus = 4
        gpu_states = ['Exclusive', 'Exclusive', 'Exclusive', 'Unallocated',]
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
        self.assertEqual(4, len(node_info.gpu_status))
        self.assertEqual('38%', node_info.gpu_status[3]['gpu_utilization'])
        self.assertEqual('0%', node_info.gpu_status[0]['gpu_utilization'])
        self.assertEqual(nr_gpus, node_info.gpus)
        self.assertEqual(gpu_states, node_info.gpu_states)
