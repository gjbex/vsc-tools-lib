#!/usr/bin/env python

import datetime
import plotly.plotly as py
import plotly.tools as tls
from plotly.graph_objs import (Heatmap, Data, Layout, Font, Stream, Figure,
                               Scatter, Marker)
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
            y=1.10,  #   and y-coord
            font=Font(size=16),    # increase font size (default is 12)
            bgcolor='#FFFFFF',     # white background
            borderpad=4            # space bt. border and text (in px)
        )
    ]
    return annotations

def compute_coordinates(x, y, options):
    x_coords = []
    y_coords = []
    for j in xrange(1, 1 + len(y)):
        for i in xrange(1, 1 + len(x)):
            x_coords.append(i)
            y_coords.append(j)
    return x_coords, y_coords

def compute_cpu_colors(cpu, options):
    nr_blues = 7
    color_map = [
        'rgb(37,0,250)',
        'rgb(57,28,250)',
        'rgb(79,52,250)',
        'rgb(107,85,250)',
        'rgb(138,119,250)',
        'rgb(164,150,250)',
        'rgb(200,200,200)', # grey
        'rgb(250,177,177)',
        'rgb(250,93,93)',
        'rgb(250,0,0)',
    ]
    down_color = 'rgb(0,0,0)'
    colors = []
    for cpu_value in cpu:
        if cpu_value < -0.1:
            colors.append(down_color)
        else:
            if cpu_value <= 1.01:
                idx = int(round((nr_blues)*cpu_value))
            elif cpu_value <= 1.06:
                idx = nr_blues
            elif cpu_value <= 2.0:
                idx = nr_blues + 1
            else:
                idx = nr_blues + 2
            colors.append(color_map[idx])
    return colors

def compute_mem_sizes(mem, options):
    sizes = []
    down_size = 10
    for mem_value in mem:
        if mem_value < -0.1:
            sizes.append(down_size)
        else:
            size = 15 + 20*mem_value
            sizes.append(size)
    return sizes

def compute_status_symbols(status, options):
    symbol_map = {
        'free': 'circle',
        'down': 'cross',
        'singlejob': 'square',
        'multijob': 'diamond',
    }
    symbols = [symbol_map[state] for state in status]
    return symbols

def compute_texts(names, cpu, mem, status, jobs):
    texts = []
    for idx in xrange(len(names)):
        text_str = '<b>{0}</b>'.format(names[idx])
        if status[idx] != 'down':
            text_str += '<br>CPU: {0:.2f}'.format(cpu[idx])
            text_str += '<br>MEM: {0:.2f}'.format(mem[idx])
            if status[idx] != 'free':
                text_str += '<br>JOB: {0}'.format(','.join(jobs[idx]))
        else:
            text_str += ' DOWN'
        texts.append(text_str)
    return texts

def create_plot(names, cpu, mem, status, jobs, x, y, options):
    x_coords, y_coords = compute_coordinates(x, y, options)
    cpu_colors = compute_cpu_colors(cpu, options)
    mem_sizes = compute_mem_sizes(mem, options)
    status_symbols = compute_status_symbols(status, options)
    texts = compute_texts(names, cpu, mem, status, jobs)
    trace = Scatter(
        x=x_coords, y=y_coords, mode='markers',
        marker=Marker(
            color=cpu_colors,
            size=mem_sizes,
            symbol=status_symbols,
        ),
        text=texts,
    )
    data = Data([trace])
    layout = Layout(
        title='{0} load'.format(options.partition),
        showlegend=False,
        annotations=create_annotations(options)
    )
    figure = Figure(data=data, layout=layout)
    filename = '{0}_cpu_load'.format(options.partition)
    url = py.plot(figure, filename=filename, auto_open=False)
    return url

def compute_maps(nodes, options):
    cpu = []
    mem = []
    for node in (n for n in nodes if n.has_property('thinking')):
        cpu.append(node.cpuload if node.cpuload is not None else -1.0)
        mem.append(node.memload if node.memload is not None else -1.0)
    return cpu, mem

def compute_job_status(nodes, options):
    jobs = []
    status = []
    for node in (n for n in nodes if n.has_property('thinking')):
        if node.status:
            if node.job_ids:
                jobs.append(node.job_ids)
                if len(node.job_ids) > 1:
                    status.append('multijob')
                else:
                    status.append('singlejob')
            else:
                jobs.append([])
                status.append('free')
        else:
            jobs.append(None)
            status.append('down')
    return jobs, status

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
            if options.verbose:
                print '{0:d} nodes found'.format(len(nodes))
        except subprocess.CalledProcessError:
            sys.stderr.write('### error: could not execute pbsnodes\n')
            sys.exit(1)
    x_labels, y_labels = compute_xy_labels(options)
    names = [node.hostname for node in nodes if node.has_property('thinking')]
    cpu, mem = compute_maps(nodes, options)
    jobs, status = compute_job_status(nodes, options)
    url = create_plot(names, cpu, mem, status, jobs,
                      x_labels, y_labels, options)
    print 'URL: {0}'.format(url)

