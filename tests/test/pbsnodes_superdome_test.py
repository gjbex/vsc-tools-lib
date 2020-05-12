#!/usr/bin/env python
'''module to test the vsc.pbs.pbsnodes.Pbsnodes parser'''

import io, sys, unittest
from vsc.pbs.pbsnodes import PbsnodesParser

class PbsnodesParserTest(unittest.TestCase):
    '''Tests for the pbsnodes output parser'''

    def test_parsing_superdome(self):
        file_name = 'tests/test/data/pbsnodes_superdome.txt'
        nr_nodes = 1
        hostnames = ['tier2-p-superdome-1', ]
        states = ['job-exclusive', ]
        nps = [112, ]
        parser = PbsnodesParser(is_verbose=False)
        with open(file_name, 'r') as pbsnodes_file:
            node_infos = parser.parse_file(pbsnodes_file)
        self.assertEqual(nr_nodes, len(node_infos))
        for node_info, hostname, state, np in zip(node_infos, hostnames,
                                                  states, nps):
            self.assertEqual(hostname, node_info.hostname)
            self.assertEqual(state, node_info.state)
            self.assertEqual(np, node_info.np)
