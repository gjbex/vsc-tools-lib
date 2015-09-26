#!/usr/bin/env python
'''module to test the PBS option parser'''

import json
import unittest
from vsc.pbs.option_parser import PbsOptionParser


class PbsOptionParserTest(unittest.TestCase):
    '''Tests the PBS option parser'''

    def setUp(self):
        conf_file_name = '../../conf/config.json'
        event_file_name = '../../lib/events.json'
        with open(conf_file_name, 'r') as conf_file:
            self._config = json.load(conf_file)
        self._config['cluster_db'] = 'data/cluster.db'
        self._config['mock_balance'] = 'data/gbalance_new.txt'
        with open(event_file_name, 'r') as event_file:
            self._event_defs = json.load(event_file)

    def test_date_time(self):
# date 201609190454.17, i.e., 19 September 2016, 04:54:17
        valid_dts = ['0454', '0454.17', '190454', '190454.17',
                     '09190454', '09190454.17', '1609190454',
                     '1609190454.17', '201609190454', '201609190454.17']
        invalid_dts = ['454', '9190454']
        parser = PbsOptionParser(self._config, self._event_defs, None)
        for dt_str in valid_dts:
            self.assertTrue(parser.is_valid_datetime(dt_str),
                            msg="date time string '{0}'".format(dt_str))
        for dt_str in invalid_dts:
            self.assertFalse(parser.is_valid_datetime(dt_str),
                             msg="date time string '{0}'".format(dt_str))
