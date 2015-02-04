#!/usr/bin/env python

import sqlite3
from subprocess import check_output, CalledProcessError, STDOUT
from vsc.utils import bytes2size
from vsc.event_logger import EventLogger
from vsc.mam.gbalance import GbalanceParser

class JobChecker(EventLogger):
    '''Semantic checker for jobs'''

    def __init__(self, config):
        '''Constructor for job checker'''
        super(JobChecker, self).__init__('global')
        self._conn = sqlite3.connect(config['cluster_db'])
        self._cursor = self._conn.cursor()
        self._config = config

    def check(self, job):
        '''Check semantics of given job'''
        self.check_partition(job)
        self.check_ppn(job)
        self.check_qos(job)
        self.check_total_pmem(job)
        self.check_mem(job)
        self.check_mem_vs_pmem(job)
        self.check_credit_account(job)

    def check_qos(self, job):
        '''check QOS specified exists'''
        job_qos = job.resource_spec('qos')
        if job_qos is not None:
            qos = self._qos()
            if job_qos not in qos:
                self.reg_event('unknown_qos',
                               {'qos': job_qos})

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
            job_ppn = node_spec['ppn']
            job_nodes = node_spec['nodes']
            for ppn in sorted(all_ppn):
                if job_nodes <= all_ppn[ppn]:
                    all_ppn[ppn] -= job_nodes
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
        mem_sizes = self._mem_sizes(partition).keys()
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
            satisfied = False
            orig_nodes = nodes_spec['nodes']
            nodes = orig_nodes
            ppn = nodes_spec['ppn']
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
        if job.resource_spec('mem') and job.resource_spec('pmem'):
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
            except CalledProcessError as e:
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
                    credit_account = account_id
                    break
            if not account_id:
                self.reg_event('no_default_credit_account')
                return
        else:
            credit_account = None
            for account_id in accounts:
                if accounts[account_id].name == job.project:
                    credit_account = account_id
                    break
            if not account_id:
                self.reg_event('unknow_credit_account',
                               {'acccount': job.project})
                return

            

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

