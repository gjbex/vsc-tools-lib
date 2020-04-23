#!/usr/bin/env python

import re
import sqlite3
from subprocess import check_output, CalledProcessError, STDOUT
from vsc.utils import bytes2size
from vsc.event_logger import EventLogger
from vsc.mam.gbalance import GbalanceParser
from vsc.mam.quote import QuoteCalculator


class JobChecker(EventLogger):
    '''Semantic checker for jobs'''

    def __init__(self, config, event_defs):
        '''Constructor for job checker'''
        super(JobChecker, self).__init__(event_defs, context='global')
        self._conn = sqlite3.connect(config['cluster_db'])
        self._cursor = self._conn.cursor()
        self._config = config

    def check(self, job):
        '''Check semantics of given job'''
        self.check_partition(job)
        self.check_properties(job)
        self.check_features(job)
        self.check_ppn(job)
        self.check_qos(job)
        self.check_total_pmem(job)
        self.check_mem(job)
        self.check_mem_vs_pmem(job)
        if self._config['check_accounting']:
            self.check_credit_account(job)

    def check_qos(self, job):
        '''check QOS specified exists'''
        job_qos = job.resource_spec('qos')
        if job_qos is not None:
            qos = self._qos()
            if job_qos not in qos:
                self.reg_event('unknown_qos',
                               {'qos': job_qos})

    def check_properties(self, job):
        '''check node properties specified exists'''
        node_specs = job.resource_spec('nodes')
        properties = self._properties()
        for node_spec in node_specs:
            if 'properties' in node_spec:
                for property in node_spec['properties']:
                    if property not in properties:
                        self.reg_event('unknown_property',
                                       {'property': property})

    def check_features(self, job):
        '''check features specified exists'''
        features = self._features()
        if job.resource_spec('feature'):
            job_features = job.resource_spec('feature')
            for feature in job_features:
                if feature not in features:
                    self.reg_event('unknown_feature',
                                   {'feature': feature})

    def check_partition(self, job):
        '''check whether the specified partition exists, and whether
           it has the total number of requested nodes'''
        partition = job.resource_spec('partition')
        partitions = self._partitions()
        if partition not in partitions:
            self.reg_event('unknown_partition',
                           {'partition': partition})
        else:
            nodes = 0
            for nodes_spec in job.resource_spec('nodes'):
                nodes += nodes_spec['nodes']
            if nodes > partitions[partition]:
                self.reg_event('insufficient_nodes',
                               {'nodes': nodes,
                                'max_nodes': partitions[partition]})

    def check_ppn(self, job):
        '''check whether there are enough nodes with the requested ppn'''
        partition = job.resource_spec('partition')
        all_ppn = self._ppn(partition)
        for node_spec in job.resource_spec('nodes'):
            if 'ppn' in node_spec:
                job_ppn = node_spec['ppn']
                job_nodes = node_spec['nodes']
                for ppn in sorted(all_ppn):
                    if ppn >= job_ppn:
                        if job_nodes <= all_ppn[ppn]:
                            all_ppn[ppn] -= job_nodes
                            job_nodes = 0
                            break
                        else:
                            job_nodes -= all_ppn[ppn]
                            all_ppn[ppn] = 0
                if job_nodes > 0:
                    self.reg_event('insufficient_ppn_nodes',
                                   {'ppn': job_ppn})

    def check_pmem(self, job):
        '''Check whether the requested memory per node is available'''
        partition = job.resource_spec('partition')
        mem_sizes = list(self._mem_sizes(partition).keys())
        for nodes_spec in job.resource_spec('nodes'):
            satisfied = False
            ppn = nodes_spec['ppn']
            pmem = job.resource_spec('pmem')
            if ppn and pmem:
                node_mem = ppn*pmem
            else:
                continue
            for mem in mem_sizes:
                if node_mem < mem:
                    satisfied = True
                    break
            if not satisfied:
                self.reg_event('insufficient_node_pmem',
                               {'mem': bytes2size(node_mem, 'gb')})

    def check_total_pmem(self, job):
        '''check total memory requirements of job as aggregated of pmem'''
        partition = job.resource_spec('partition')
        mem_sizes = self._mem_sizes(partition)
        for nodes_spec in job.resource_spec('nodes'):
            orig_nodes = nodes_spec['nodes']
            nodes = orig_nodes
            if 'ppn' in nodes_spec:
                ppn = nodes_spec['ppn']
            else:
                ppn = None
            pmem = job.resource_spec('pmem')
            if ppn and pmem:
                node_mem = ppn*pmem
            else:
                continue
            for mem in sorted(mem_sizes.keys()):
                if node_mem < mem:
                    if nodes <= mem_sizes[mem]:
                        mem_sizes[mem] -= nodes
                        nodes = 0
                        break
                    else:
                        nodes -= mem_sizes[mem]
            if nodes > 0:
                self.reg_event('insufficient_nodes_mem',
                               {'mem': bytes2size(node_mem, 'gb'),
                                'nodes': orig_nodes})

    def check_mem(self, job):
        '''check total memory requirements of job as mem'''
        partition = job.resource_spec('partition')
        mem_sizes = self._mem_sizes(partition)
        mem_spec = job.resource_spec('mem')
        if mem_spec:
            for nodes_spec in job.resource_spec('nodes'):
                nodes = nodes_spec['nodes']
                for mem in sorted(mem_sizes.keys()):
                    mem_spec -= mem*nodes
                    if mem_spec <= 0:
                        break
            if mem_spec > 0:
                self.reg_event('insufficient_mem',
                               {'mem': bytes2size(mem_spec, 'gb')})

    def check_mem_vs_pmem(self, job):
        '''Check whether both mem and pmem are specified'''
        if job.resource_spec('mem') and not job.has_default_pmem:
            self.reg_event('both_mem_pmem_specs')

    def check_credit_account(self, job):
        '''Check whether a project is available to debit credits from,
           and whether the balance is sufficient'''
        if 'mock_balance' in self._config:
            with open(self._config['mock_balance'], 'r') as balance_file:
                balance_sheet = ''.join(balance_file.readlines())
        else:
            balance_cmd = self._config['balance_cmd']
            try:
                balance_sheet = check_output([balance_cmd], stderr=STDOUT)
            except OSError:
                return
            except CalledProcessError:
                # TODO: decide on user feedback
                return
        accounts = GbalanceParser().parse(balance_sheet)
        if len(accounts) == 0:
            self.reg_event('no_credit_account')
            return
        elif job.project is None:
            credit_account = None
            for account_id in accounts:
                account_name = accounts[account_id].name
                if (account_name == '' or
                        account_name == self._config['default_project']):
                    credit_account = accounts[account_id]
                    break
            if not credit_account:
                self.reg_event('no_default_credit_account')
                return
        else:
            credit_account = None
            for account_id in accounts:
                if accounts[account_id].name == job.project:
                    credit_account = accounts[account_id]
                    break
            if not credit_account:
                self.reg_event('unknown_credit_account',
                               {'account': job.project})
                return
        quoteCalculator = QuoteCalculator(self._config)
        credits = quoteCalculator.compute(job)
        if credits > credit_account.available_credits:
            self.reg_event('insufficient_credits',
                           {'account': credit_account.name})

    def _mem_sizes(self, partition):
        '''retrieve the memory sizes of the nodes from the databse'''
        sizes = {}
        stmt = '''SELECT n.mem, count(n.node_id)
                      FROM nodes as n NATURAL JOIN partitions as p
                      WHERE p.partition_name = ?
                      GROUP BY mem'''
        self._cursor.execute(stmt, (partition, ))
        for row in self._cursor:
            sizes[row[0]] = row[1]
        return sizes

    def _partitions(self):
        '''retrieve the list of partitions and their nodes from databasse'''
        partitions = {}
        stmt = '''SELECT p.partition_name, count(n.node_id)
                      FROM partitions as p NATURAL JOIN nodes as n
                      GROUP BY p.partition_name'''
        self._cursor.execute(stmt)
        for row in self._cursor:
            partitions[row[0]] = row[1]
        return partitions

    def _qos(self):
        '''retrieve list of QOS levels'''
        qos = []
        stmt = '''SELECT qos FROM qos_levels'''
        self._cursor.execute(stmt)
        for row in self._cursor:
            qos.append(row[0])
        return qos

    def _features(self):
        '''retrieve list of features'''
        features = []
        stmt = '''SELECT DISTINCT feature FROM features'''
        self._cursor.execute(stmt)
        for row in self._cursor:
            features.append(row[0])
        return features

    def _properties(self):
        '''retrieve list of propertie and propertiess'''
        properties = []
        stmt = '''SELECT DISTINCT property FROM properties'''
        self._cursor.execute(stmt)
        for row in self._cursor:
            properties.append(row[0])
        return properties

    def _ppn(self, partition):
        '''retrieve the number of nodes per ppn'''
        ppn = {}
        stmt = '''SELECT n.np, count(*)
                      FROM nodes as n NATURAL JOIN partitions as p
                      WHERE p.partition_name = ?
                      GROUP BY n.np'''
        self._cursor.execute(stmt, (partition, ))
        for row in self._cursor:
            ppn[row[0]] = row[1]
        return ppn


class ScriptChecker(EventLogger):
    '''Implementation of a bash script checker that uses some heuristics
       to check for common errors'''

    def __init__(self, config, event_defs):
        '''Constructor for script checker'''
        super(ScriptChecker, self).__init__(event_defs)
        try:
            from fuzzywuzzy import fuzz
            self._fuzz = fuzz
        except ImportError:
            self._fuzz = None

    def check(self, job, start_line_nr):
        '''check a script for potential errors'''
        self._line_nr = start_line_nr - 1
        for line in job.script.split('\n'):
            self._line_nr += 1
            self._check_module_load(line)
            self._check_workdir(line)

    def _check_module_load(self, line):
        '''check whether this line contains a module load, if so do
           some basic validity checks'''
        if not self._fuzz:
            return
        line = line.strip()
        cmd = 'module load'
        cmd_re = r'module\s+load\s+.*'
        ratio = self._fuzz.partial_token_sort_ratio(cmd, line)
        if ratio > 75:
            if not re.match(cmd_re, line):
                self.reg_event('missspelled', {'correct': cmd})

    def _check_workdir(self, line):
        '''check whether this line contains PBS_O_WORKDIR and performs
           some basic validity checks'''
        if not self._fuzz:
            return
        line = line.strip()
        var = 'PBS_O_WORKDIR'
        ratio = self._fuzz.partial_token_sort_ratio(var.lower(),
                                                    line.lower())
        if ratio > 75:
            if var not in line:
                self.reg_event('missspelled',
                               {'correct': '${{{0}}}'.format(var)})
            else:
                var_value = '${{{0}}}'.format(var)
                if not('${0}'.format(var) in line or
                       var_value in line):
                    self.reg_event('missspelled', {'correct': var_value})
