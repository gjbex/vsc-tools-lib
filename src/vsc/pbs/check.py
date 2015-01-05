#!/usr/bin/env python

import sqlite3
from vsc.utils import bytes2size
from vsc.event_logger import EventLogger

class JobChecker(EventLogger):
    '''Semantic checker for jobs'''

    def __init__(self, db_name):
        '''Constructor for job checker'''
        super(JobChecker, self).__init__('global')
        self._conn = sqlite3.connect(db_name)
        self._cursor = self._conn.cursor()

    def check(self, job):
        '''Check semantics of given job'''
        self.check_total_pmem(job)
        self.check_mem(job)
        self.check_mem_vs_pmem(job)
        self.check_partition(job)

    def check_partition(self, job):
        '''check whether the specified partition exists'''
        partition = job.resource_spec('partition')
        partitions = self._partitions()
        if partition not in partitions:
            self.reg_event('unknown_partition',
                           {'partition': partition})

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
                    if mem_spec <=0:
                        break
            if mem_spec > 0:
                self.reg_event('insufficient_mem',
                               {'mem': bytes2size(mem_spec, 'gb')})

    def check_mem_vs_pmem(self, job):
        '''Check whether both mem and pmem are specified'''
        if job.resource_spec('mem') and job.resource_spec('pmem'):
            self.reg_event('both_mem_pmem_specs')

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

