#!/usr/bin/env python

if __name__ == '__main__':
    from argparse import ArgumentParser
    import subprocess, sys

    from vsc.pbs.pbsnodes import PbsnodesParser
    from vsc.pbs.utils import compute_partition
    from vsc.utils import hostname2rackinfo

    arg_parser = ArgumentParser(description=('loads a database with node '
                                             'information'))
    arg_parser.add_argument('--pbsnodes_file', help='pbsnodes file')
    arg_parser.add_argument('--partitions', default='thinking,gpu,phi',
                             help='partitions defined for the cluster')
    arg_parser.add_argument('--pbsnodes', default='/usr/local/bin/pbsnodes',
                            help='pbsnodes command to use')
    arg_parser.add_argument('--verbose', action='storetrue',
                            help='show run time information')
    options = arg_parser.parse_args()
    partitions = options.partitions.split(',')
    pbsnodes_parser = PbsnodesParser()
    if options.pbsnodes_file:
        with open(options.pbsnodes_file, 'r') as node_file:
            nodes = pbsnodes_parser.parse_file(node_file)
    else:
        try:
            node_output = subprocess.check_output([options.pbsnodes])
            nodes = pbsnodes_parser.parse(node_output)
            if options.verbose:
                print '{0:d} nodes found'.format(len(nodes))
        except subprocess.CalledProcessError:
            sys.stderr.write('### error: could not execute pbsnodes\n')
            sys.exit(1)
    fields = ['hostname', 'partition', 'rack', 'iru', 'np', 'mem',
                 'cpuload', 'memload', 'jobs']
    fmt_str = ';'.join(['{{{0}}}'.format(x) for x in fields])
    print ';'.join(fields)
    for node in nodes:
        partition_id = compute_partition(node, partitions)
        rack, iru, _ = hostname2rackinfo(node.hostname)
        if partition_id == 'thinking':
            if node.status:
                if node.jobs and len(node.jobs):
                    job_ids = ','.join(node.job_ids)
                else:
                    job_ids = ''
                print fmt_str.format(
                    hostname=node.hostname,
                    partition=partition_id,
                    rack=rack, iru=iru,
                    np=node.np, mem=node.memory,
                    cpuload=node.cpuload, memload=node.memload,
                    jobs=job_ids
                )
            else:
                msg = 'E: node {0} has no status\n'.format(node.hostname)
                sys.stderr.write(msg)
