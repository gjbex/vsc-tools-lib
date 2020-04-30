#!/usr/bin/env python
'''module to test the vsc.pbs.qstat.QstatParser parser for progress information'''

from datetime import datetime
import json, unittest
from vsc.pbs.qstat import QstatParser

class QstatProgressTest(unittest.TestCase):
    '''Tests for the qstat -f output parser'''

    def setUp(self):
        conf_file_name = '../../conf/config.json'
        with open(conf_file_name, 'r') as conf_file:
            self._config = json.load(conf_file)
        self._config['cluster_db'] = 'data/cluster.db'
        self._config['mock_balance'] = 'data/gbalance_new.txt'

    def test_parsing(self):
        file_name = 'data/qstat_f_out.txt'
        test_jobs = [0, 82, 89]
        job_id = ['50011943.tier2-p-moab-2.icts.hpc.kuleuven.be',
                  '50317784.tier2-p-moab-2.tier2.hpc.kuleuven.be',
                  '50318782.tier2-p-moab-2.tier2.hpc.kuleuven.be']
        job_state = ['Q', 'R', 'C']
        job_queue_times = ['Tue Jul 31 11:59:59 2018',
                           'Fri Apr 17 16:06:19 2020',
                           'Sun Apr 19 11:25:17 2020',]
        job_start_times = [None,
                           'Sat Apr 18 15:42:00 2020',
                           'Mon Apr 20 14:32:53 2020']
        parser = QstatParser(self._config)
        with open(file_name, 'r') as qstat_file:
            jobs = parser.parse_file(qstat_file)
        for i in range(len(test_jobs)):
            self.assertEqual(job_id[i], jobs[test_jobs[i]].job_id)
            if job_start_times[i] is not None:
                delta = (datetime.strptime(job_start_times[i], '%c') -
                         datetime.strptime(job_queue_times[i], '%c'))
            else:
                delta = (datetime.now() - datetime.strptime(job_queue_times[i], '%c'))
            self.assertAlmostEqual(delta.total_seconds(), jobs[test_jobs[i]].time_in_queue, places=0)
