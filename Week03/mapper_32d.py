#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW3.2d

import sys
from csv import reader
import string

# Increment mapper counter
sys.stderr.write("reporter:counter:Custom_Counter,Mapper,1\n")

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    word, count = line.replace('\n','').split('\t')
    print '%s\t%s' % (count, word)