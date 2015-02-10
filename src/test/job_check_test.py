#!/usr/bin/env python
'''module to test the vsc.pbs.check.JobChecker logic'''

import json, unittest
from vsc.pbs.script_parser import PbsScriptParser
from vsc.pbs.check import JobChecker

class JObCheckerTest(unittest.TestCase):
    '''Tests for the PBS job checker'''

    def setUp(self):
        conf_file_name = '../config.json'
        event_file_name = '../events.json'
        with open(conf_file_name, 'r') as conf_file:
            self._config = json.load(conf_file)
        self._config['cluster_db'] = 'data/cluster.db'
        self._config['mock_balance'] = 'data/gbalance_new.txt'
        with open(event_file_name, 'r') as event_file:
            self._event_defs = json.load(event_file)

    def test_correct(self):
        file_name = 'data/correct.pbs'
        nr_syntax_events = 0
        nr_semantic_events = 0
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(nr_syntax_events, len(parser.events))
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        self.assertEquals(nr_semantic_events, len(checker.events))

    def test_too_large_ppn(self):
        file_name = 'data/too_large_ppn.pbs'
        nr_syntax_events = 0
        nr_semantic_events = 1
        event_name = 'insufficient_ppn_nodes'
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(nr_syntax_events, len(parser.events))
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        self.assertEquals(nr_semantic_events, len(checker.events))
        self.assertEquals(event_name, checker.events[0]['id'])

    def test_too_many_nodes(self):
        file_name = 'data/too_many_nodes.pbs'
        nr_syntax_events = 0
        event_names = ['insufficient_nodes', 'insufficient_ppn_nodes']
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(nr_syntax_events, len(parser.events))
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        self.assertEquals(len(event_names), len(checker.events))
        for event in checker.events:
            self.assertTrue(event['id'] in event_names)
