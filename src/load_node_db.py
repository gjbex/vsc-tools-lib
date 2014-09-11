#!/usr/bin/env python

if __name__ == '__main__':
    from argparse import ArgumentParser
    import re, sqlite3, sys

    from vsc.pbs.node import PbsnodesParser
    from vsc.pbs.utils import compute_features, compute_partition
    from vsc.utils import hostname2rackinfo

    arg_parser = ArgumentParser(description=('loads a database with node '
                                             'information'))
    arg_parser.add_argument('--file', help='node file')
    arg_parser.add_argument('--db', default='nodes.db',
                             help='file to store the database in')
    arg_parser.add_argument('--partitions', default='thinking,gpu,phi',
                             help='partitions defined for the cluster')
    arg_parser.add_argument('--qos_levels', default='debugging',
                             help='QOS defined for the cluster')
    options = arg_parser.parse_args()
    partition_list = options.partitions.split(',')
    qos_levels = options.qos_levels.split(',')
    conn = sqlite3.connect(options.db)
    cursor = conn.cursor()
    pbsnodes_parser = PbsnodesParser()
    with open(options.file, 'r') as node_file:
        nodes = pbsnodes_parser.parse_file(node_file)
    partition_insert_cmd = '''INSERT INTO partitions
                                  (partition_name) VALUES (?)'''
    partitions = {}
    for partition_name in partition_list:
        cursor.execute(partition_insert_cmd, (partition_name, ))
        partitions[partition_name] = cursor.lastrowid
    conn.commit()
    qos_insert_cmd = '''INSERT INTO qos_levels
                                  (qos) VALUES (?)'''
    for qos in qos_levels:
        cursor.execute(qos_insert_cmd, (qos, ))
    conn.commit()
    node_insert_cmd = '''INSERT INTO nodes
                             (hostname, partition_id, rack, iru, np, mem)
                         VALUES
                             (?, ?, ?, ?, ?, ?)'''
    prop_insert_cmd = '''INSERT INTO properties
                             (node_id, property) VALUES
                             (?, ?)'''
    feature_insert_cmd = '''INSERT INTO features
                                (node_id, feature) VALUES
                                (?, ?)'''
    for node in nodes:
        partition_id = compute_partition(node, partitions)
        rack, iru, _ = hostname2rackinfo(node.hostname)
        if partition_id:
            if node.status:
                cursor.execute(node_insert_cmd, (node.hostname,
                                                 partition_id,
                                                 rack,
                                                 iru,
                                                 node.np,
                                                 node.memory))
                node_id = cursor.lastrowid
                for property in node.properties:
                    cursor.execute(prop_insert_cmd, (node_id, property))
                for feature in compute_features(node):
                    cursor.execute(feature_insert_cmd, (node_id, feature))
            else:
                msg = 'E: node {0} has no status\n'.format(node.hostname)
                sys.stderr.write(msg)
    conn.commit()    
    cursor.close()

