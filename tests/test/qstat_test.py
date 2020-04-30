#!/usr/bin/env python
'''module to test the vsc.pbs.qstat.QstatParser parser'''

import json, unittest
from vsc.pbs.qstat import QstatParser

class QstatTest(unittest.TestCase):
    '''Tests for the qstat -f output parser'''

    def setUp(self):
        conf_file_name = 'conf/config.json'
        with open(conf_file_name, 'r') as conf_file:
            self._config = json.load(conf_file)
        self._config['cluster_db'] = 'tests/test/data/cluster.db'
        self._config['mock_balance'] = 'tests/test/data/gbalance_new.txt'

    def test_parsing(self):
        file_name = 'tests/test/data/qstat_full.txt'
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
        self.assertEqual(nr_jobs, len(jobs))
        for i, job_nr in enumerate(test_jobs):
            self.assertEqual(job_id[i], jobs[test_jobs[i] - 1].job_id)
            self.assertEqual(job_state[i], jobs[test_jobs[i] - 1].state)
            walltime = jobs[test_jobs[i] - 1].resource_specs['walltime']
            self.assertEqual(job_walltime[i], walltime)
            nodect = jobs[test_jobs[i] - 1].resource_specs['nodect']
            self.assertEqual(job_nodect[i], nodect)

    def test_parsing_large(self):
        file_name = 'tests/test/data/qstat_f_out.txt'
        nr_jobs = 532
        test_jobs = [1, 50, 122]
        job_id = ['50011943.tier2-p-moab-2.icts.hpc.kuleuven.be',
                  '50218028.tier2-p-moab-2.tier2.hpc.kuleuven.be',
                  '50321299.tier2-p-moab-2.tier2.hpc.kuleuven.be']
        job_state = ['Q', 'Q', 'R']
        job_walltime = [1, 120*3600, 72*3600]
        job_nodect = [1, 1, 4]
        parser = QstatParser(self._config)
        with open(file_name, 'r') as qstat_file:
            jobs = parser.parse_file(qstat_file)
        self.assertEqual(nr_jobs, len(jobs))
        for i, job_nr in enumerate(test_jobs):
            self.assertEqual(job_id[i], jobs[test_jobs[i] - 1].job_id)
            self.assertEqual(job_state[i], jobs[test_jobs[i] - 1].state)
            walltime = jobs[test_jobs[i] - 1].resource_specs['walltime']
            self.assertEqual(job_walltime[i], walltime)
            nodect = jobs[test_jobs[i] - 1].resource_specs['nodect']
            self.assertEqual(job_nodect[i], nodect)
                              

