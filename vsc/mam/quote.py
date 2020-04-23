'''module containing logic to compute the credit cost of a job'''

import sqlite3


class QuoteCalculator(object):
    '''class implementing a calculator of job costs'''

    def __init__(self, config):
        '''constructor that takes a configuration'''
        self._config = config

    def determine_node_types(self, node_spec, partition):
        '''determine the relevant type of a node to base to cost
           calculation on'''
        if 'properties' in node_spec:
            node_types = node_spec['properties']
        else:
            node_types = []
            with sqlite3.connect(self._config['cluster_db']) as conn:
                cursor = conn.cursor()
                result = cursor.execute(
                    '''SELECT DISTINCT p.property
                           FROM properties as p, nodes as n,
                                partitions as p
                           WHERE p.node_id = n.node_id AND
                                 n.partition_id = p.partition_id AND
                                 p.partition_name = ?''',
                    (partition, ))
                for row in result:
                    node_types.append(row[0])
        return node_types

    def compute(self, job):
        '''compute the cost in credits of the given job'''
        type_costs = self._config['node_type_credits']
        walltime = float(job.resource_specs['walltime'])
        node_rate = self._config['node_rate']
        partition = job.resource_specs['partition']
        resource_specs = job.resource_specs['nodes']
        cost = 0.0
        for resource_spec in resource_specs:
            nr_nodes = resource_spec['nodes']
            node_types = self.determine_node_types(resource_spec, partition)
            node_cost = -1.0
            for node_type in node_types:
                if node_type in type_costs:
                    if type_costs[node_type] > node_cost:
                        node_cost = type_costs[node_type]
            if node_cost < 0.0:
                node_cost = type_costs[job.resource_spec('partition')]
            cost += nr_nodes*node_cost
        return cost*walltime*node_rate
