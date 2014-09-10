#!/usr/bin/env python

import sqlite3

if __name__ == '__main__':
    from argparse import ArgumentParser

    arg_parser = ArgumentParser(description=('create a database to store '
                                             'store node information'))
    arg_parser.add_argument('--partitions', default='thinking,gpu,phi',
                            help='partitions defined for the cluster')
    arg_parser.add_argument('--db', default='nodes.db',
                            help='file to store the database in')
    options = arg_parser.parse_args()
    partitions = options.partitions.split(',')
    conn = sqlite3.connect(options.db)
    cursor = conn.cursor()
    partitions_cmd = '''CREATE TABLE partitions
                            (partition_id INTEGER PRIMARY KEY AUTOINCREMENT,
                             partition_name TEXT NOT NULL,
                             UNIQUE(partition_name))'''
    partitions_idx = '''CREATE INDEX partition_idx
                            ON partitions(partition_name)'''
    cursor.execute(partitions_cmd)
    cursor.execute(partitions_idx)
    partitions_insert = '''INSERT INTO partitions
                               (partition_name) VALUES (?)'''
    for partition in partitions:
        cursor.execute(partitions_insert, (partition, ))
    conn.commit()
    nodes_cmd = '''CREATE TABLE nodes
                       (node_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        hostname TEXT NOT NULL,
                        partition_id INTEGER,
                        rack INTEGER,
                        iru INTEGER,
                        np INTEGER NOT NULL,
                        ngpus INTEGER,
                        mem INTEGER NOT NULL,
                        FOREIGN KEY(partition_id)
                            REFERENCES partitions(partition_id),
                        UNIQUE(hostname, partition_id))'''
    nodes_idx = '''CREATE INDEX node_idx
                       ON nodes(hostname, partition_id)'''
    features_cmd = '''CREATE TABLE features
                          (feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
                           node_id INTEGER NOT NULL,
                           feature TEXT NOT NULL,
                           FOREIGN KEY(node_id)
                               REFERENCES nodes(node_id),
                           UNIQUE(node_id, feature))'''
    features_idx = '''CREATE INDEX feature_idx
                          ON features(node_id, feature)'''
    properties_cmd = '''CREATE TABLE properties
                          (property_id INTEGER PRIMARY KEY AUTOINCREMENT,
                           node_id INTEGER NOT NULL,
                           property TEXT NOT NULL,
                           FOREIGN KEY (node_id)
                               REFERENCES nodes (node_id),
                           UNIQUE (node_id, property))'''
    properties_idx = '''CREATE INDEX property_idx
                          ON properties (node_id, property)'''
    cursor.execute(nodes_cmd)
    cursor.execute(features_cmd)
    cursor.execute(properties_cmd)
    conn.commit()
    cursor.close()

