#!/usr/bin/env python

import datetime
import numpy as np
import plotly.plotly as py
import plotly.tools as tls
from plotly.graph_objs import Heatmap, Data, Layout, Font, Stream, Figure
from vsc.utils import hostname2rackinfo
from vsc.pbs.node import PbsnodesParser

def create_annotations(options):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    anno_text = 'time: {0}'.format(timestamp)
    annotations = [
        dict(
            text=anno_text,  # set annotation text
            showarrow=False, # remove arrow 
            xref='paper',  # use paper coords
            yref='paper',  #  for both coordinates
            x=0.95,  # position's x-coord
            y=1.15,  #   and y-coord
            font=Font(size=16),    # increase font size (default is 12)
            bgcolor='#FFFFFF',     # white background
            borderpad=4            # space bt. border and text (in px)
        )
    ]
    return annotations

def init_plot(z, x, y, options):
    stream = Stream(token=options.stream_id, maxpoints=10)
    heatmap = Heatmap(z=z, x=x, y=y, stream=stream)
    data = Data([heatmap])
    layout = Layout(
        title='{0} load'.format(options.partition),
        annotations=create_annotations(options)
    )
    figure = Figure(data=data, layout=layout)
    filename = '{0}_cpu_load'.format(options.partition)
    url = py.plot(figure, filename=filename, auto_open=False)
    return url

def update_plot(z, x, y, options):
    stream = py.Stream(options.stream_id)
    stream.open()
    heatmap = Heatmap(z=z, x=x, y=y, stream=stream)
    layout = Layout(
        annotations=create_annotations(options)
    )
    stream.write(heatmap, layout=layout)
    stream.close()

def compute_maps(nodes, options):
    cpu = -np.ones((options.nr_racks*options.nr_irus, options.nr_nodes))
    mem = -np.ones((options.nr_racks*options.nr_irus, options.nr_nodes))
    for node in (n for n in nodes if n.has_property('thinking')):
        rack_nr, iru_nr, node_nr = hostname2rackinfo(node.hostname)
        x = ((rack_nr - options.rack_offset)*options.nr_irus +
             (iru_nr - options.iru_offset))
        y = node_nr - options.node_offset
        cpu[x, y] = node.cpuload if node.cpuload is not None else -1.0
        mem[x, y] = node.memload if node.memload is not None else -1.0
        if options.verbose:
            print '{0}: {1:.2f}, {2:.2f}'.format(node.hostname,
                                                 cpu[x, y], mem[x, y])
    return cpu, mem

def compute_xy_labels(options):
    n_min = options.node_offset
    n_max = n_min + options.nr_nodes
    x_labels = ['n{0:02d}'.format(i) for i in range(n_min, n_max)]
    r_min = options.rack_offset
    r_max = r_min + options.nr_racks
    i_min = options.iru_offset
    i_max = i_min + options.nr_irus
    y_labels = ['r{0}i{1}'.format(r, i) for r in range(r_min, r_max)
                for i in range(i_min, i_max)]
    return x_labels, y_labels

if __name__ == '__main__':
    from argparse import ArgumentParser
    import subprocess, sys

    arg_parser = ArgumentParser(description='Create a heatmap of CPU load')
    arg_parser.add_argument('--stream_id', required=True,
                            help='Plotly stream ID for graph')
    arg_parser.add_argument('--init', action='store_true',
                            help='initialize plot on Plotly')
    arg_parser.add_argument('--partition', default='thinking',
                            help='cluster partition to visualize')
    arg_parser.add_argument('--nr_racks', type=int, default=3,
                            help='number of racks')
    arg_parser.add_argument('--rack_offset', type=int, default=1,
                            help='rack offset')
    arg_parser.add_argument('--nr_irus', type=int, default=3,
                            help='number of IRUs per rack')
    arg_parser.add_argument('--iru_offset', type=int, default=0,
                            help='IRU offset')
    arg_parser.add_argument('--nr_nodes', type=int, default=16,
                            help='number of nodes per IRU')
    arg_parser.add_argument('--node_offset', type=int, default=1,
                            help='node offset')
    arg_parser.add_argument('--pbsnodes', default='/usr/local/bin/pbsnodes',
                            help='pbsnodes command to use')
    arg_parser.add_argument('--verbose', action='store_true',
                            help='verbose output')
    arg_parser.add_argument('--file', help='file with pbsnodes output')
    options = arg_parser.parse_args()
    parser = PbsnodesParser()
    if options.file:
        with open(options.file, 'r') as pbs_file:
            nodes = parser.parse_file(pbs_file)
    else:
        try:
            node_output = subprocess.check_output([options.pbsnodes])
            nodes = parser.parse(node_output)
        except CalledProcessError:
            sys.stderr.write('### error: could not execute pbsnodes\n')
            sys.exit(1)
    x_labels, y_labels = compute_xy_labels(options)
    cpu, mem = compute_maps(nodes, options)
    if options.init:
        url = init_plot(cpu, x_labels, y_labels, options)
        print 'URL: {0}'.format(url)
    else:
        update_plot(cpu, x_labels, y_labels, options)

