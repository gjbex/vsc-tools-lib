#!/usr/bin/env python
'''module to test the vsc.mam.quote.QuoteCalculator'''

import json, unittest
from vsc.pbs.script_parser import PbsScriptParser
from vsc.mam.quote import QuoteCalculator

class QuoteCalculatorTest(unittest.TestCase):
    '''Tests the computations of job costs'''

    def setUp(self):
        conf_file_name = 'conf/config.json'
        event_file_name = 'lib/events.json'
        with open(conf_file_name, 'r') as conf_file:
            self._config = json.load(conf_file)
        self._config['cluster_db'] = 'tests/test/data/cluster.db'
        self._config['mock_balance'] = 'tests/test/data/gbalance_new.txt'
        with open(event_file_name, 'r') as event_file:
            self._event_defs = json.load(event_file)

    def test_compute(self):
        file_name = 'tests/test/data/correct.pbs'
        expected_cost = 685.99
        parser = PbsScriptParser(self._config, self._event_defs)
        with open(file_name, 'r') as pbs_file:
            parser.parse_file(pbs_file)
        quote_calc = QuoteCalculator(self._config)
        credits = quote_calc.compute(parser.job)
        self.assertAlmostEqual(expected_cost, credits, delta=0.1)
