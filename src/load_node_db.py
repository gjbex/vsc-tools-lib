#!/usr/bin/env python

if __name__ == '__main__':
    from argparse import ArgumentParser
    import re, sqlite3, sys

    from vsc.pbs.node import PbsnodesParser
    from vsc.pbs.utils import compute_features, create_partition_computer
    from vsc.utils import hostname2rackinfo

    arg_parser = ArgumentParser(description=('loads a database with node '
                                             'information'))
    arg_parser.add_argument('--file', help='node file')
    arg_parser.add_argument('--db', default='nodes.db',
                            help='file to store the database in')
    options = arg_parser.parse_args()
    conn = sqlite3.connect(options.db)
    cursor = conn.cursor()
    compute_partition = create_partition_computer(cursor)
    pbsnodes_parser = PbsnodesParser()
    with open(options.file, 'r') as node_file:
        nodes = pbsnodes_parser.parse_file(node_file)
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
        partition_id = compute_partition(node)
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

