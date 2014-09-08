#!/usr/bin/env python

import sqlite3

if __name__ == '__main__':
    from argparse import ArgumentParser

    arg_parser = ArgumentParser(description=('create a database to store '
                                             'store node information'))
    arg_parser.add_argument('--db', default='nodes.db',
                            help='file to store the database in')
    options = arg_parser.parse_args()
    conn = sqlite3.connect(options.db)
    nodes_cmd = '''CREATE TABLE nodes
                       (id integer PRIMARY KEY AUTOINCREMENT,
                        name text NOT NULL,
                        partition text NOT NULL,
                        rack integer,
                        iru integer,
                        np integer NOT NULL,
                        ngpus integer,
                        mem text NOT NULL,
                        UNIQUE (name, partition))'''
    nodes_idx = '''CREATE INDEX node_idx
                       ON nodes (name, partition)'''
    features_cmd = '''CREATE TABLE features
                          (id integer PRIMARY KEY AUTOINCREMENT,
                           node_id integer,
                           feature text NOT NULL,
                           FOREIGN KEY (node_id) REFERENCES nodes (id),
                           UNIQUE (node_id, feature))'''
    features_idx = '''CREATE INDEX feature_idx
                          ON features (node_id, feature)'''
    properties_cmd = '''CREATE TABLE properties
                          (id integer PRIMARY KEY AUTOINCREMENT,
                           node_id integer,
                           property text NOT NULL,
                           FOREIGN KEY (node_id) REFERENCES nodes (id),
                           UNIQUE (node_id, property))'''
    properties_idx = '''CREATE INDEX property_idx
                          ON properties (node_id, property)'''
    conn.execute(nodes_cmd)
    conn.execute(features_cmd)
    conn.execute(properties_cmd)
    conn.commit()
    conn.close()

