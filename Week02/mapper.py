#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW2.1

import sys

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    # In this case, we do not need to map the input to anything
    key, value = line.strip().split(',')
    print '%s\t%s' % (int(key), value)