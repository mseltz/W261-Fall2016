#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW2.3

import sys
import string
import re

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    
    # Replace delimiter
    line = line.replace('\n', '')
    fields = line.split('\t')
    print '%s^%s^%s^%s^%s' % (fields[0], fields[1], fields[2], fields[3], fields[4])