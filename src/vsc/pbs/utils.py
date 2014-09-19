'''Miscellaneous functions for dealing the PBS data'''

def compute_features(node):
    '''Compute features for a node'''
    features = []
    if 'thinking' in node.properties:
        if node.memory <= 64*1024**3:
            features.append('mem64')
        else:
            features.append('mem128')
    return features

def compute_partition(node, partitions):
    '''Compute partition for a node based on its properties'''
    partition_id = None
    if type(partitions) == dict:
        for partition, id in partitions.items():
            if node.has_property(partition):
                partition_id = id
                break
    elif type(partitions) == list:
        for partition in partitions:
            if node.has_property(partition):
                partition_id = partition
                break
    return partition_id
