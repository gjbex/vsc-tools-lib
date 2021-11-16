#!/usr/bin/env python3

import sys, os
import re
from vsc.moab.checknode import ChecknodeBlock, ChecknodeParser

testfile = 'data/checknode_r27i13n24.txt'
# testfile = 'data/checknode_r23i13n23.txt'

with open(testfile, 'r') as fh:
    lines = fh.readlines()
    text = ''.join(lines)

parser = ChecknodeParser(debug=False)
block = parser.parse_one(text)
