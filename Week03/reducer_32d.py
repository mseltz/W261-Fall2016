#!/usr/bin/python
## reducer.py
## Author: Miki Seltzer
## Description: reducer code for HW3.2d

import sys
from operator import itemgetter

# Increment mapper counter
sys.stderr.write("reporter:counter:Custom_Counter,Reducer,1\n")

# Initialize variables
total = 0

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    fields = line.replace('\n','').split('\t')
    count = fields[0]
    word = fields[1]
    
    try:
        count = int(count)
    except ValueError:
        continue
        
    # The first word should be *total, save this as total
    if word == '*total': total = float(count)
    else: print '%s\t%s\t%s' % (word, count, count/total)