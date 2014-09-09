#!/usr/bin/env python

import sqlite3, sys

from vsc.pbs.node import PbsnodesParser

if __name__ == '__main__':
    from argparse import ArgumentParser

    arg_parser = ArgumentParser(description=('loads a database with node '
                                             'information'))
    arg_parser.add_argument('--file', help='node file')
    arg_parser.add_argument('--db', default='nodes.db',
                            help='file to store the database in')
    options = arg_parser.parse_args()
    conn = sqlite3.connect(options.db)
    cursor = conn.cursor()
    cursor.execute('''SELECT id, name FROM partitions;''')
    partitions = {}
    for row in cursor:
        partitions[row[1]] = row[0]
    pbsnodes_parser = PbsnodesParser()
    with open(options.file, 'r') as node_file:
        nodes = pbsnodes_parser.parse_file(node_file)
    insert_cmd = '''INSERT INTO nodes
                        (name, partition_id, np, mem) VALUES (?, ?, ?, ?)'''
    for node in nodes:
        partition_id = None
        for partition, id in partitions.items():
            if node.has_property(partition):
                partition_id = id
        if partition_id:
            if node.status:
                cursor.execute(insert_cmd, (node.hostname, partition_id,
                                            node.np,
                                            node.status['physmem']))
            else:
                msg = 'E: node {0} has no status\n'.format(node.hostname)
                sys.stderr.write(msg)
    conn.commit()    
    cursor.close()

