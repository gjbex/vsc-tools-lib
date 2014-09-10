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

