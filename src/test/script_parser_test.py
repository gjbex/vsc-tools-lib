#!/usr/bin/env python
'''module to test the vsc.pbs.script_parser.PbsScriptParser parser'''

import json, unittest
from vsc.pbs.script_parser import PbsScriptParser

class PbsScriptParserTest(unittest.TestCase):
    '''Tests for the pbsnodes output parser'''

    def setUp(self):
        config_file_name = '../config.json'
        with open(config_file_name, 'r') as config_file:
            self._config = json.load(config_file)

    def test_parsing(self):
        file_name = 'data/correct.pbs'
        name = 'my_job'
        project = 'lp_qlint'
        nodes = 2
        ppn = 4
        walltime = 72*3600
        qos = 'debugging'
        join = 'oe'
        parser = PbsScriptParser(self._config)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        job = parser.job
        self.assertEquals(name, job.name)
        self.assertEquals(project, job.project)
        nodes_specs = job.resource_specs['nodes'][0]
        self.assertEquals(nodes, nodes_specs['nodes'])
        self.assertEquals(ppn, nodes_specs['ppn'])
        self.assertEquals(walltime, job.resource_specs['walltime'])
        self.assertEquals(qos, job.resource_specs['qos'])
        self.assertEquals(join, job.join)
