#!/usr/bin/env python
'''module to test the vsc.pbs.pbsnodes.Pbsnodes parser'''

import io, sys, unittest
from vsc.pbs.pbsnodes import PbsnodesParser

class PbsnodesParserTest(unittest.TestCase):
    '''Tests for the pbsnodes output parser'''

    def test_parsing_nodes_down(self):
        file_name = 'tests/test/data/pbsnodes_down.txt'
        nr_nodes = 2
        hostnames = ['r22g41', 'r24g35', ]
        states = ['down,offline', 'down']
        parser = PbsnodesParser(is_verbose=True)
        with open(file_name, 'r') as pbsnodes_file:
            node_infos = parser.parse_file(pbsnodes_file)
        self.assertEqual(nr_nodes, len(node_infos))
        for node_info, hostname, state in zip(node_infos, hostnames, states):
            self.assertEqual(hostname, node_info.hostname)
            self.assertEqual(state, node_info.state)
