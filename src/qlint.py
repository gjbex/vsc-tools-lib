#!/usr/bin/env python

from argparse import ArgumentParser
from vsc.qlint.pbs_parser import PbsParser

if __name__ == '__main__':
    arg_parser = ArgumentParser(description='PBS script syntax checker')
    arg_parser.add_argument('pbs_file', help='PBS file to check')
    options, rest = arg_parser.parse_known_args()
    pbs_parser = PbsParser()
    pbs_parser.parse(options.pbs_file)
    for error in pbs_parser.errors:
        print 'E: {0}'.format(error)
    for warning in pbs_parser.warnings:
        print 'W: {0}'.format(warning)
