#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW3.5b

import sys
import itertools

# Increment mapper counter
sys.stderr.write("reporter:counter:Custom_Counter,Mapper,1\n")

# Initialize variables
total = 0

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    # Define our key and value
    fields = line.replace('\n','').split('\t')
    word = fields[0]
    stripe = eval(fields[1])
    # Now we need to "unpack" the stripe and emit each pair for sorting
    for item in stripe:
        if stripe[item] >= 100: 
            print '%s\t%s\t%s' % (stripe[item], word, item)