'''Miscellaneous functions for dealing the PBS data'''

def compute_features(node):
    '''Compute features for a node'''
    features = []
    if 'thinking' in node.properties:
        if node.memory < 64*1024**3:
            features.append('mem64')
        else:
            features.append('mem128')
    return features

def create_partition_computer(cursor):
    '''Returns a function that will compute the partition a given node
       belongs to, or NOne'''
    cursor.execute('''SELECT partition_id, partition_name
                          FROM partitions;''')
    partitions = {}
    for row in cursor:
        partitions[row[1]] = row[0]
    def compute_partition(node):
        partition_id = None
        for partition, id in partitions.items():
            if node.has_property(partition):
                partition_id = id
        return partition_id
    return compute_partition

