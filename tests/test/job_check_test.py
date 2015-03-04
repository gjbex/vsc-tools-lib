#!/usr/bin/env python
'''module to test the vsc.pbs.check.JobChecker logic'''

import json, unittest
from vsc.pbs.script_parser import PbsScriptParser
from vsc.pbs.check import JobChecker

class JObCheckerTest(unittest.TestCase):
    '''Tests for the PBS job checker'''

    def setUp(self):
        conf_file_name = '../../conf/config.json'
        event_file_name = '../../lib/events.json'
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
        event_names = ['insufficient_nodes', 'insufficient_ppn_nodes',
                       'insufficient_nodes_mem']
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(nr_syntax_events, len(parser.events))
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        self.assertEquals(len(event_names), len(checker.events))
        for event in checker.events:
            self.assertTrue(event['id'] in event_names)

    def test_mem_pmem(self):
        file_name = 'data/mem_pmem.pbs'
        event_names = ['both_mem_pmem_specs']
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        self.assertEquals(len(event_names), len(checker.events))
        for event in checker.events:
            self.assertTrue(event['id'] in event_names)
        self.assertEquals(len(event_names), checker.nr_warnings)

    def test_mem_violation(self):
        file_name = 'data/mem_violation.pbs'
        event_names = ['insufficient_mem']
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        self.assertEquals(len(event_names), len(checker.events))
        for event in checker.events:
            self.assertTrue(event['id'] in event_names)
        self.assertEquals(len(event_names), checker.nr_errors)

    def test_pmem_violation(self):
        file_name = 'data/pmem_violation.pbs'
        event_names = ['insufficient_nodes_mem']
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        self.assertEquals(len(event_names), len(checker.events))
        for event in checker.events:
            self.assertTrue(event['id'] in event_names)
        self.assertEquals(len(event_names), checker.nr_errors)

    def test_unknown_property(self):
        file_name = 'data/unknown_property.pbs'
        nr_syntax_events = 0
        nr_semantic_events = 1
        event_name = 'unknown_feature'
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(nr_syntax_events, len(parser.events))
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        self.assertEquals(nr_semantic_events, len(checker.events))
        self.assertEquals(event_name, checker.events[0]['id'])

    def test_unknown_feature(self):
        file_name = 'data/unknown_feature.pbs'
        nr_syntax_events = 0
        nr_semantic_events = 1
        event_name = 'unknown_feature'
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(nr_syntax_events, len(parser.events))
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        self.assertEquals(nr_semantic_events, len(checker.events))
        self.assertEquals(event_name, checker.events[0]['id'])

    def test_queue_no_walltime(self):
        file_name = 'data/queue_no_walltime.pbs'
        nr_syntax_events = 0
        nr_semantic_events = 0
        queue_name = 'q72h'
        expected_walltime = 72*3600
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(nr_syntax_events, len(parser.events))
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        self.assertEquals(nr_semantic_events, len(checker.events))
        job = parser.job
        self.assertEquals(queue_name, job.queue)
        self.assertEquals(expected_walltime, job.resource_spec('walltime'))

    def test_walltime_and_queue(self):
        file_name = 'data/walltime_and_queue.pbs'
        nr_syntax_events = 0
        nr_semantic_events = 0
        queue_name = 'q72h'
        expected_walltime = 45*3600 + 15*60
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(nr_syntax_events, len(parser.events))
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        self.assertEquals(nr_semantic_events, len(checker.events))
        job = parser.job
        self.assertEquals(queue_name, job.queue)
        self.assertEquals(expected_walltime, job.resource_spec('walltime'))

    def test_queue_and_walltime(self):
        file_name = 'data/queue_and_walltime.pbs'
        nr_syntax_events = 0
        nr_semantic_events = 0
        queue_name = 'q72h'
        expected_walltime = 45*3600 + 15*60
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(nr_syntax_events, len(parser.events))
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        self.assertEquals(nr_semantic_events, len(checker.events))
        job = parser.job
        self.assertEquals(queue_name, job.queue)
        self.assertEquals(expected_walltime, job.resource_spec('walltime'))

    def test_walltime_no_queue(self):
        file_name = 'data/walltime_no_queue.pbs'
        nr_syntax_events = 0
        nr_semantic_events = 0
        queue_name = 'qdef'
        expected_walltime = 45*3600 + 15*60
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        self.assertEquals(nr_syntax_events, len(parser.events))
        checker = JobChecker(self._config, self._event_defs)
        checker.check(parser.job)
        self.assertEquals(nr_semantic_events, len(checker.events))
        job = parser.job
        self.assertEquals(queue_name, job.queue)
        self.assertEquals(expected_walltime, job.resource_spec('walltime'))
