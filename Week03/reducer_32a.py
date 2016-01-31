#!/usr/bin/python
## reducer.py
## Author: Miki Seltzer
## Description: reducer code for HW3.2

import sys
from operator import itemgetter

# Increment mapper counter
sys.stderr.write("reporter:counter:Custom_Counter,Reducer,1\n")

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    print line
    