#!/usr/bin/env python3

from logging import debug
import sys, os
from vsc.moab.checknode import ChecknodeBlock, ChecknodeParser

def main():
    filename = 'data/checknode_ALL.txt'
    parser = ChecknodeParser(debug=False)
    parser.parse_file(filename)
    print(parser.dic_nodes.keys())

    return 0

if __name__ == '__main__':
    sys.exit(main())