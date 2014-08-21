#!/usr/bin/env python

from argparse import ArgumentParser
from vsc.qlint.pbs_parser import PbsParser

if __name__ == '__main__':
    arg_parser = ArgumentParser(description='PBS script syntax checker')
    arg_parser.add_argument('pbs_file', help='PBS file to check')
    options, rest = arg_parser.parse_known_args()
    pbs_parser = PbsParser()
    pbs_parser.parse(options.pbs_file)
    for event in pbs_parser.events:
        print 'event line {0}: {1}'.format(event['line'],
                                           event['event'])
