#!/usr/bin/env python
'''module to test the vsc.pbs.script_parser.PbsScriptParser parser'''

import json
import unittest
from vsc.pbs.script_parser import PbsScriptParser
from vsc.pbs.check import JobChecker, ScriptChecker


class PbsScriptCheckTest(unittest.TestCase):
    '''Tests for the Bash script checker'''

    def setUp(self):
        conf_file_name = '../../conf/config.json'
        event_file_name = '../../lib/events.json'
        with open(conf_file_name, 'r') as conf_file:
            self._config = json.load(conf_file)
        self._config['cluster_db'] = 'data/cluster.db'
        self._config['mock_balance'] = 'data/gbalance_new.txt'
        with open(event_file_name, 'r') as event_file:
            self._event_defs = json.load(event_file)

    def test_module_laod(self):
        file_name = 'data/module_laod.pbs'
        event_names = ['missspelled']
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEqual(0, len(parser.events))
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        script_checker = ScriptChecker(self._config, self._event_defs)
        script_checker.check(parser.job, parser.script_first_line_nr)
        self.assertEqual(len(event_names), len(script_checker.events))
        for event in script_checker.events:
            self.assertTrue(event['id'] in event_names)
        self.assertEqual(len(event_names), script_checker.nr_warnings)

    def test_working_dir(self):
        file_name = 'data/working_dir.pbs'
        event_names = ['missspelled']
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEqual(0, len(parser.events))
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        script_checker = ScriptChecker(self._config, self._event_defs)
        script_checker.check(parser.job, parser.script_first_line_nr)
        self.assertEqual(len(event_names), len(script_checker.events))
        for event in script_checker.events:
            self.assertTrue(event['id'] in event_names)
        self.assertEqual(len(event_names), script_checker.nr_warnings)

    def test_working_dir_no_var(self):
        file_name = 'data/working_dir_no_var.pbs'
        event_names = ['missspelled']
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEqual(0, len(parser.events))
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        script_checker = ScriptChecker(self._config, self._event_defs)
        script_checker.check(parser.job, parser.script_first_line_nr)
        self.assertEqual(len(event_names), len(script_checker.events))
        for event in script_checker.events:
            self.assertTrue(event['id'] in event_names)
        self.assertEqual(len(event_names), script_checker.nr_warnings)
