#!/usr/bin/env python
'''module to test the vsc.pbs.qstat.QstatParser parser'''

import json, unittest
from vsc.pbs.qstat import QstatParser

class QstatTest(unittest.TestCase):
    '''Tests for the gbalance output parser'''

    def setUp(self):
        conf_file_name = '../config.json'
        with open(conf_file_name, 'r') as conf_file:
            self._config = json.load(conf_file)
        self._config['cluster_db'] = 'data/cluster.db'
        self._config['mock_balance'] = 'data/gbalance_new.txt'

    def test_parsing(self):
        file_name = 'data/qstat_full.txt'
        nr_jobs = 121
        test_jobs = [1, 50, 121]
        job_id = ['20025307.icts-p-svcs-1', '20034175.icts-p-svcs-1',
                  '20034531.icts-p-svcs-1']
        job_state = ['Q', 'R', 'Q']
        job_walltime = [168*3600, 72*3600, 2*3600]
        job_nodect = [20, 1, 1]
        parser = QstatParser(self._config)
        with open(file_name, 'r') as qstat_file:
            jobs = parser.parse_file(qstat_file)
        self.assertEquals(nr_jobs, len(jobs))
        for i, job_nr in enumerate(test_jobs):
            self.assertEquals(job_id[i], jobs[test_jobs[i] - 1].job_id)
            self.assertEquals(job_state[i], jobs[test_jobs[i] - 1].state)
            walltime = jobs[test_jobs[i] - 1].resource_specs['walltime']
            self.assertEquals(job_walltime[i], walltime)
            nodect = jobs[test_jobs[i] - 1].resource_specs['nodect']
            self.assertEquals(job_nodect[i], nodect)

                              

