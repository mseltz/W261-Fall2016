#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW3.4b

import sys

# Increment mapper counter
sys.stderr.write("reporter:counter:Custom_Counter,Mapper,1\n")

# Initialize variables
total = 0

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    fields = line.replace('\n','').split('\t')
    if int(fields[2]) >= 100:
        print '%s\t%s\t%s' % (fields[2], fields[0], fields[1])