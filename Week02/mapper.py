#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW2.2.1

import sys
import string
import re

# Our input comes from STDIN (standard input)
for line in sys.stdin:
    fields = line.replace('\n', '').split('\t')
    print '%s\t%s' % (fields[1], fields[0])