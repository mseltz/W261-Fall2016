#!/usr/bin/python
## mapper.py
## Author: Miki Seltzer
## Description: mapper code for HW3.2b

import sys
from csv import reader
import string

# Increment mapper counter
sys.stderr.write("reporter:counter:Custom_Counter,Mapper,1\n")

# Initialize variables
total = 0

# Our input comes from STDIN (standard input)
for line in reader(sys.stdin):
    # Format our line
    issue = line[3].lower()
    issue = issue.replace(',',' ').replace('/',' ')
    
    for word in issue.split():
        if len(word) > 0:
            print '%s\t%s' % (word, 1)
            total += 1

# Print total words
print '%s\t%s' % ('*total', total)