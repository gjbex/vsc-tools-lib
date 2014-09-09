#!/usr/bin/env python

import sqlite3

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
    pbsnodes_parser = PbsnodesParser()
    with open(options.file, 'r') as node_file:
        nodes = pbsnodes_parser.parse_file(node_file)
    for node in nodes:
        print '# ----'
        print node
    print '# ----'
    conn.close()

