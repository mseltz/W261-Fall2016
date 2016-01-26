#!/usr/bin/python
## reducer.py
## Author: Miki Seltzer
## Description: reducer code for HW2.1

from operator import itemgetter
import sys

for line in sys.stdin:
    # In this case, we do not need to reduce anything
    key, value = line.strip().split('\t')
    print "%d\t%s" % (int(key), value)