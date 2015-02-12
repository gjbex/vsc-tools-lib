#!/usr/bin/env python

import sqlite3, sys

DB_DESC = {
    'partitions': {
        'create':
            '''CREATE TABLE partitions
                   (partition_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    partition_name TEXT NOT NULL,
                    UNIQUE(partition_name))''',
        'index': [
            '''CREATE INDEX partition_idx
                   ON partitions(partition_name)'''
        ],
    },
    'nodes': {
        'create':
            '''CREATE TABLE nodes
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
                    UNIQUE(hostname, partition_id))''',
        'index': [
            '''CREATE INDEX node_idx
                   ON nodes(hostname, partition_id)'''
        ],
    },
    'properties': {
        'create':
            '''CREATE TABLE properties
                   (property_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id INTEGER NOT NULL,
                    property TEXT NOT NULL,
                    FOREIGN KEY (node_id) REFERENCES nodes (node_id),
                    UNIQUE (node_id, property))''',
        'index': [
            '''CREATE INDEX property_idx
                   ON properties (node_id, property)'''
        ],
    },
    'features': {
        'create':
            '''CREATE TABLE features
                   (feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id INTEGER NOT NULL,
                    feature TEXT NOT NULL,
                    FOREIGN KEY(node_id) REFERENCES nodes(node_id),
                    UNIQUE(node_id, feature))''',
        'index': [
            '''CREATE INDEX feature_idx
                   ON features(node_id, feature)'''
        ],
    },
    'qos_levels': {
        'create':
            '''CREATE TABLE qos_levels
                   (qos_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    qos TEXT NOT NULL,
                    UNIQUE(qos))''',
        'index': [
            '''CREATE INDEX qos_idx
                   ON qos_levels(qos)'''
        ],
    },
    'jobs': {
        'create':
            '''CREATE TABLE jobs
                   (job_id INTEGER PRIMARY KEY,
                    user TEXT NOT NULL,
                    state TEXT NOT NULL,
                    procs INTEGER NOT NULL,
                    remaining INTEGER,
                    starttime TEXT,
                    wclimit INTEGER,
                    queuetime TEXT)''',
        'index': [
            '''CREATE INDEX job_idx
                   ON jobs(job_id, user)''',
            '''CREATE INDEX job_state_idx
                   ON jobs(state)''',
        ],
    },
    'running_jobs': {
        'create':
            '''CREATE TABLE running_jobs
                   (job_id INTEGER,
                    node_id INTEGER,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id),
                    FOREIGN KEY(node_id) REFERENCES nodes(node_id))''',
        'index': [
            '''CREATE INDEX running_job_idx
                   ON running_jobs(job_id, node_id)''',
        ],
    },
}

def init_table(conn, table_name, table_desc, force=False):
    '''Create table and index according to description, drop table
       if force is True, use connection'''
    cursor = conn.cursor()
    try:
        if force:
            cursor.execute('''DROP TABLE {0}'''.format(table_name))
        cursor.execute(table_desc['create'])
        for index_stmt in table_desc['index']:
            cursor.execute(index_stmt)
    except sqlite3.OperationalError as error:
        msg = 'W: problem initializing table {0} ({1}), skipping\n'
        sys.stderr.write(msg.format(table_name, error.message))
    cursor.close()

def init_db(conn, db_desc, force=False, create_jobs_tables=False):
    '''Create tables and indices in the connection's database, drop tables
       first when using force'''
    for table_name, table_desc in db_desc.items():
        if not table_name.endswith('jobs') or create_jobs_tables:
            init_table(conn, table_name, table_desc, force)

if __name__ == '__main__':
    from argparse import ArgumentParser

    arg_parser = ArgumentParser(description=('create a database to store '
                                             'node information'))
    arg_parser.add_argument('--db', default='nodes.db',
                            help='file to store the database in')
    arg_parser.add_argument('--jobs', action='store_true',
                            help='create job-related tables')
    arg_parser.add_argument('--force', action='store_true',
                            help='when database exists, first drop tables')
    options = arg_parser.parse_args()
    conn = sqlite3.connect(options.db)
    init_db(conn, DB_DESC, force=options.force, create_jobs_tables=options.jobs)

