#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW3.2

import sys
from csv import reader

# Increment mapper counter
sys.stderr.write("reporter:counter:Custom_Counter,Mapper,1\n")

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.split()
    for word in line:
        print '%s\t%s' % (word, 1)
    