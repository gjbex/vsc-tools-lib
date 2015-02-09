#!/usr/bin/env python
'''module to test the vsc.pbs.check.JobChecker logic'''

import json, unittest
from vsc.pbs.script_parser import PbsScriptParser
from vsc.pbs.check import JobChecker

class JObCheckerTest(unittest.TestCase):
    '''Tests for the PBS job checker'''

    def setUp(self):
        conf_file_name = '../config.json'
        with open(conf_file_name, 'r') as conf_file:
            self._config = json.load(conf_file)
        self._config['cluster_db'] = 'data/cluster.db'
        self._config['mock_balance'] = 'data/gbalance_new.txt'

    def test_job_correct(self):
        file_name = 'data/correct.pbs'
        nr_syntax_events = 0
        nr_semantic_events = 0
        parser = PbsScriptParser(self._config)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(nr_syntax_events, len(parser.events))
        checker = JobChecker(self._config)
        checker.check(parser.job)
        self.assertEquals(nr_semantic_events, len(checker.events))
