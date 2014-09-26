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
        self.check_pmem(job)
        self.check_total_pmem(job)
        self.check_mem(job)
        self.check_mem_vs_pmem(job)

    def check_pmem(self, job):
        '''Check whether the requested memory per node is available'''
        partition = job.resource_spec('partition')
        mem_sizes = self._mem_sizes(partition).keys()
        for nodes_spec in job.resource_spec('nodes'):
            ppn = nodes_spec['ppn']
            pmem = job.resource_spec('pmem')
            if ppn and pmem:
                node_mem = ppn*pmem
            else:
                continue
            for mem in mem_sizes:
                if node_mem < mem:
                    continue
            self.reg_event('insufficient_node_pmem',
                           {'mem': bytes2size(node_mem, 'gb')})

    def check_total_pmem(self, job):
        '''check total memory requirements of job as aggregated of pmem'''
        partition = job.resource_spec('partition')
        mem_sizes = self._mem_sizes(partition)
        for nodes_spec in job.resource_spec('nodes'):
            nodes = nodes_spec['nodes']
            ppn = nodes_spec['ppn']
            pmem = job.resource_spec('pmem')
            if ppn and pmem:
                node_mem = ppn*pmem
            else:
                continue
            for mem in sort(sortmem_sizes.keys()):
                if node_mem < mem:
                    mem_sizes[mem] -= nodes
                    if mem_sizes[mem] < 0:
                        self.reg_event('insufficient_nodes_mem',
                                       {'mem': bytes2size(node_mem, 'gb',
                                        'nodes': nodes)})
                    continue
            self.reg_event('insufficient_nodes_mem',
                           {'mem': bytes2size(node_mem, 'gb',
                            'nodes': nodes)})

    def check_mem(self, job):
        '''check total memory requirements of job as mem'''
        partition = job.resource_spec('partition')
        mem_sizes = self._mem_sizes(partition)
        mem_spec = job.resource_spec('mem')
        if mem_spec:
            for nodes_spec in job.resource_spec('nodes'):
                nodes = nodes_spec['nodes']
                for mem in sort(sortmem_sizes.keys()):
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

