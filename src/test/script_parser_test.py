#!/usr/bin/env python
'''module to test the vsc.pbs.script_parser.PbsScriptParser parser'''

import json, unittest
from vsc.pbs.script_parser import PbsScriptParser

class PbsScriptParserTest(unittest.TestCase):
    '''Tests for the pbsnodes output parser'''

    def setUp(self):
        config_file_name = '../config.json'
        event_file_name = '../events.json'
        with open(config_file_name, 'r') as config_file:
            self._config = json.load(config_file)
        with open(event_file_name, 'r') as event_file:
            self._event_defs = json.load(event_file)

    def test_simple(self):
        file_name = 'data/simple.pbs'
        project = 'lp_qlint'
        nodes = 2
        ppn = 4
        walltime = 72*3600
        partition = self._config['default_partition']
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        job = parser.job
        nodes_specs = job.resource_specs['nodes'][0]
        self.assertEquals(nodes, nodes_specs['nodes'])
        self.assertEquals(ppn, nodes_specs['ppn'])
        self.assertEquals(walltime, job.resource_specs['walltime'])
        self.assertEquals(partition, job.resource_specs['partition'])

    def test_parsing(self):
        file_name = 'data/correct.pbs'
        name = 'my_job'
        project = 'lp_qlint'
        nodes = 2
        ppn = 4
        walltime = 72*3600
        qos = 'debugging'
        join = 'oe'
        parser = PbsScriptParser(self._config, self._event_defs)
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

    def test_dos(self):
        file_name = 'data/dos.pbs'
        nr_syntax_events = 4
        event_names = ['dos_format']
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(nr_syntax_events, len(parser.events))
        for event in parser.events:
            self.assertTrue(event['id'] in event_names)
        self.assertEquals(nr_syntax_events, parser.nr_errors)

    def test_nodes_ppn_wrong_spec(self):
        file_name = 'data/nodes_ppn_wrong_spec.pbs'
        nr_syntax_events = 1
        event_names = ['ppn_no_number']
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(nr_syntax_events, len(parser.events))
        for event in parser.events:
            self.assertTrue(event['id'] in event_names)
        self.assertEquals(nr_syntax_events, parser.nr_errors)

    def test_misplaced_shebang(self):
        file_name = 'data/misplaced_shebang.pbs'
        event_names = ['missing_shebang', 'misplaced_shebang']
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(len(event_names), len(parser.events))
        for event in parser.events:
            self.assertTrue(event['id'] in event_names)
        self.assertEquals(len(event_names), parser.nr_warnings)
